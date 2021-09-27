import time
import threading
import math
from huobi.model.generic.symbol import Symbol

from wampy.constants import DEFAULT_REALM, DEFAULT_ROLES, DEFAULT_TIMEOUT
from wampy.peers.clients import Client
from wampy.roles.callee import callee
from wampy.roles.subscriber import subscribe

from utils.logging import quite_logger
from market import MarketClient
from dataset.redis import Redis
from target import Target
from user import User
from utils import config, get_target_time, logger, user_config

STOP_PROFIT_RATE_HIGH = config.getfloat('sell', 'STOP_PROFIT_RATE_HIGH')
STOP_PROFIT_RATE_LOW = config.getfloat('sell', 'STOP_PROFIT_RATE_LOW')
STOP_PROFIT_SLEEP = config.getfloat('time', 'STOP_PROFIT_SLEEP')
IOC_BATCH_NUM = config.getint('sell', 'IOC_BATCH_NUM')
IOC_INTERVAL = config.getfloat('time', 'IOC_INTERVAL')
REPORT_PRICE = user_config.getboolean('setting', 'REPORT_PRICE')
WATCHER_NUM = config.getint('watcher', 'WATCHER_NUM')

WS_HOST = config.get('data', 'WsHost')
WS_PORT = config.getint('data', 'WsPort')
WS_URL = f'ws://{WS_HOST}:{WS_PORT}'


class Topic:
    CLIENT_INFO = 'CLIENT_INFO'
    STATE = 'STATE'
    TIME = 'TIME'
    BUY_SIGNAL = 'BUY_SIGNAL'
    SELL_SIGNAL = 'SELL_SIGNAL'
    AFTER_BUY = 'AFTER_BUY'
    HIGH = 'HIGH'
    STOP_PROFIT = 'STOP_PROFIT'
    STOP_LOSS = 'STOP_LOSS'
    CLEAR = 'CLEAR'

class State:
    STOPPED = 0
    STARTED = 1
    RUNNING = 2


quite_logger(all_logger=True)


class ControlledClient(Client):
    def __init__(
        self, url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            url=url, cert_path=cert_path, ipv=ipv, name=name,
            realm=realm, roles=roles, call_timeout=call_timeout,
            message_handler_cls=message_handler_cls
        )
        self.state = State.STOPPED
        self.target_time = None
        self.client_type = 'unknown'

    @subscribe(topic=Topic.STATE)
    def state_handler(self, state, *args, **kwargs):
        if self.state != state:
            self.state = state
            logger.info(f"Change state to {['stopped', 'started', 'running'][self.state]}")

    @subscribe(topic=Topic.TIME)
    def time_handler(self, target_time,  *args, **kwargs):
        if self.target_time != target_time:
            self.target_time = target_time
            logger.info(f'Change target time {self.target_time}')

    def wait_state(self, state=State.STARTED):
        while self.state != state:
            time.sleep(0.1)

    def start(self):
        super().start()
        self.after_start()

    def after_start(self):
        self.publish(topic=Topic.CLIENT_INFO, client_type=self.client_type, remove=False)

    def stop(self):
        self.before_stop()
        try:
            super().stop()
        except Exception as e:
            logger.error(e)

    def before_stop(self):
        self.publish(topic=Topic.CLIENT_INFO, client_type=self.client_type, remove=True)


class WatcherClient(ControlledClient):
    def __init__(
        self, market_client: MarketClient, url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            url=url, cert_path=cert_path, ipv=ipv, name=name,
            realm=realm, roles=roles, call_timeout=call_timeout,
            message_handler_cls=message_handler_cls
        )
        self.market_client : MarketClient = market_client
        self.client_type = 'watcher'
        self.high_stop_profit = True
        self.task : list[str] = []
        self.targets : dict[str, Target] = {}
        self.redis_conn: Redis = Redis()

    def get_task(self, num) -> 'list[str]':
        self.task = self.rpc.req_task(num)

    def send_buy_signal(self, symbol, price, init_price, trade_time, now):
        if now > trade_time + 0.2:
            logger.info(f'Buy. {symbol} with price {price}USDT at {trade_time}. Recieved at {now}. Too late')
            return

        self.publish(topic=Topic.BUY_SIGNAL, symbol=symbol, price=price, init_price=init_price, now=trade_time)
        self.market_client.symbols_info[symbol].init_price = init_price
        target = Target(symbol, price, now, self.high_stop_profit)
        target.set_info(self.market_client.symbols_info[symbol])
        self.targets[symbol] = target
        logger.info(f'Buy. {symbol} with price {price}USDT at {trade_time}. recieved at {now}')
        self.redis_conn.write_target(symbol)

    def send_stop_loss_signal(self, target: Target, price, trade_time, now):
        self.publish(topic=Topic.STOP_LOSS, symbol=target.symbol, price=target.stop_loss_price)
        target.own = False
        logger.info(f'Stop loss. {target.symbol}: {price}USDT at {trade_time}. recieved at {now}')

    def send_stop_profit_signal(self, target: Target, price, trade_time, now):
        if not self.high_stop_profit:
            return

        self.publish(topic=Topic.STOP_PROFIT, status=False, symbol=target.symbol, price=target.stop_profit_price)
        target.own = False
        self.high_stop_profit = False
        logger.info(f'Stop profit. {target.symbol}: {price}USDT at {trade_time}. recieved at {now}')

    @subscribe(topic=Topic.STOP_PROFIT)
    def stop_profit_handler(self, status, *arg, **kwargs):
        self.high_stop_profit = status

    @subscribe(topic=Topic.AFTER_BUY)
    def after_buy_handler(self, symbol, price, *args, **kwargs):
        if symbol not in self.targets:
            return

        if price == 0:
            # del self.targets[symbol]
            pass
        else:
            self.targets[symbol].set_buy_price(price)


class WatcherMasterClient(WatcherClient):
    def __init__(
        self, market_client: MarketClient, url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            market_client=market_client, url=url, cert_path=cert_path, ipv=ipv,
            name=name, realm=realm, roles=roles, call_timeout=call_timeout,
            message_handler_cls=message_handler_cls
        )
        self.client_info = {
            'watcher': 0,
            'dealer': 0
        }
        vols = self.market_client.get_vol()
        self.symbols : list[str] = sorted(
            self.market_client.symbols_info.keys(),
            key=lambda symbol: vols[symbol],
            reverse=True
        )

        self.tasks = [[] for _ in range(WATCHER_NUM)]
        tasks_vol = [0] * WATCHER_NUM
        for symbol in self.symbols:
            i = tasks_vol.index(min(tasks_vol))
            self.tasks[i].append(symbol)
            tasks_vol[i] += vols[symbol]

    def set_state(self, state):
        self.state = state
        self.publish(topic=Topic.STATE, state=state)

    def set_time(self, target_time):
        self.target_time = target_time
        self.publish(topic=Topic.TIME, target_time=target_time)

    def after_start(self):
        self.info_handler(self.client_type)

    def before_stop(self):
        self.info_handler(self.client_type, True)

    def get_task(self, num=0):
        self.task = self.req_task(num)

    @callee
    def req_task(self, num=0) -> 'list[str]':
        task = self.tasks[0]
        self.tasks = self.tasks[1:]
        return task

    @subscribe(topic=Topic.CLIENT_INFO)
    def info_handler(self, client_type, remove=False, *args, **kwargs):
        if remove:
            self.client_info[client_type] -= 1
            if client_type == 'dealer' and self.client_info['dealer'] == 0:
                self.stop_running()
        else:
            self.client_info[client_type] += 1
        
            self.publish(topic=Topic.STATE, state=self.state)
            self.publish(topic=Topic.TIME, target_time=self.target_time)

    def starting(self):
        if self.state == State.STOPPED:
            self.set_state(State.STARTED)
            logger.info(f"Change state to started")

    def running(self):
        if self.state != State.RUNNING:
            self.set_state(State.RUNNING)
            self.set_time(get_target_time())
            logger.info(f"Change state to running")

    def stopping(self):
        if self.state != State.STOPPED:
            self.set_state(State.STOPPED)
            logger.info(f"Change state to stopped")

    def stop_running(self):
        if self.state == State.RUNNING:
            self.set_state(State.STARTED)
            logger.info(f"Change state to not running")

    def clear(self, num=IOC_BATCH_NUM):
        [_, targets] = self.redis_conn.get_target()
        logger.info(f'Start clear, targets are {targets}')
        if targets:
            targets = targets.split(',')
            for i in range(num):
                threading.Timer(
                    IOC_INTERVAL * i,
                    self.clear_handler,
                    [targets, i]
                ).start()

    def clear_handler(self, targets, count):
        data = [(symbol, self.redis_conn.get_new_price(symbol)) for symbol in targets]
        self.publish(topic=Topic.CLEAR, data=data, count=count)
        logger.info(f'Start ioc clear for round {count+1}, data={data}')


class DealerClient(ControlledClient):
    def __init__(
        self, market_client: MarketClient,
        user: User,
        url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            url=url, cert_path=cert_path, ipv=ipv, name=name,
            realm=realm, roles=roles, call_timeout=call_timeout,
            message_handler_cls=message_handler_cls
        )
        self.market_client : MarketClient = market_client
        self.targets : dict[str, Target] = {}
        self.user : User = user
        self.client_type = 'dealer'
        self.high_stop_profit = True
        self.not_buy = False


    @subscribe(topic=Topic.BUY_SIGNAL)
    def buy_signal_handler(self, symbol, price, init_price, now, *args, **kwargs):
        if self.state != State.RUNNING or symbol in self.targets:
            return

        if self.not_buy:
            logger.info(f'Fail to buy {symbol}, already stop buy')
            return

        receive_time = time.time()
        self.market_client.symbols_info[symbol].init_price = init_price
        target = Target(symbol, price, now, self.high_stop_profit)
        target.set_info(self.market_client.symbols_info[symbol])
        self.targets[symbol] = target

        self.user.buy_limit_and_sell(target, self)
        logger.info(f'Buy. {symbol}, recieved at {receive_time}, sent at {now}, price {price}')

    def after_buy(self, symbol, price):
        if self.targets[symbol].buy_price:
            return

        if REPORT_PRICE:
            self.publish(topic=Topic.AFTER_BUY, symbol=symbol, price=price)

        if price == 0:
            del self.targets[symbol]
        else:
            self.targets[symbol].set_buy_price(price)

    @subscribe(topic=Topic.STOP_LOSS)
    def stop_loss_handler(self, symbol, price, *args, **kwargs):
        if self.state != State.RUNNING or symbol not in self.targets:
            return

        target = self.targets[symbol]
        self.user.cancel_and_sell(target)
        logger.info(f'Stop loss. {symbol}: {price}USDT')

    @subscribe(topic=Topic.STOP_PROFIT)
    def stop_profit_handler(self, symbol, price, *args, **kwargs):
        if self.state != State.RUNNING or not self.high_stop_profit:
            return

        def high_cancel_and_sell():
            self.user.high_cancel_and_sell(list(self.targets.values()), symbol, price)

        self.high_stop_profit = False
        if symbol:
            self.not_buy = True

        threading.Timer(STOP_PROFIT_SLEEP, high_cancel_and_sell).start()
        logger.info(f'Stop profit. {symbol}: {price}USDT')

    def sell_in_buy_price(self):
        self.user.cancel_and_sell_in_buy_price(self.targets.values())

    def check_and_sell(self, limit=True):
        self.user.check_and_sell(self.targets.values(), limit)

    def check_all_stop_profit(self):
        while self.state == State.RUNNING:
            time.sleep(0.1)
            try:
                asset = self.user.get_asset()
                if asset > self.user.all_stop_profit_asset:
                    self.state_handler(State.STARTED)
                    logger.info(f'Now asset {asset}U, start asset {self.user.start_asset}U, stop profit')
                    break
            except:
                pass
    
    @subscribe(topic=Topic.CLEAR)
    def clear_handler(self, data, count, *arg, **kwargs):
        if self.state != State.RUNNING:
            return

        logger.info(f'Start ioc clear for round {count+1}')
        for symbol, price in data:
            if symbol not in self.targets:
                continue
            
            target = self.targets[symbol]
            self.user.cancel_and_sell_ioc(target, price, count)
