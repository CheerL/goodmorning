import time
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
from utils import config, get_target_time, logger

DEALER_NUM = config.getint('setting', 'DealerNum')
WATCHER_NUM = config.getint('setting', 'WatcherNum')
SELL_LEAST_AFTER = config.getfloat('setting', 'SellLeastAfter')
MAX_BUY = config.getint('setting', 'MaxBuy')
MAX_BUY_WAIT = config.getfloat('setting', 'MaxBuyWait')
SELL_RATE = config.getfloat('setting', 'SellRate')
SECOND_SELL_RATE = config.getfloat('setting', 'SecondSellRate')

WS_HOST = config.get('setting', 'WsHost')
WS_PORT = config.getint('setting', 'WsPort')
WS_URL = f'ws://{WS_HOST}:{WS_PORT}'

HIGH_SELL_SLEEP = 1

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
    SELL_DELAY = 'SELL_DELAY'
    NOT_SELL_DELAY = 'NOT_SELL_DELAY'

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
            time.sleep(1)

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
        self.client_type: str = 'watcher'
        self.task : list[str] = []
        self.stop_profit: bool = False
        self.targets : dict[str, Target] = {}
        self.redis_conn: Redis = Redis()

    def get_task(self, num) -> 'list[str]':
        self.task = self.rpc.req_task(num)

    def send_buy_signal(self, symbol, price, init_price, now, vol, start_time):
        if start_time - now > 0.2:
            logger.info(f'Buy signal. {symbol} with price {price}USDT, vol {vol} at {now}. Recieved at {start_time}. Too late')
            return

        self.publish(topic=Topic.BUY_SIGNAL, symbol=symbol, price=price, init_price=init_price, vol=vol, now=now)
        target = Target(symbol, price, init_price, now)
        self.targets[symbol] = target
        logger.info(f'Buy signal. {symbol} with price {price}USDT, vol {vol} at {now}. recieved at {start_time}')
        self.redis_conn.write_target(symbol)

    @subscribe(topic=Topic.AFTER_BUY)
    def after_buy_handler(self, symbol, price, *args, **kwargs):
        if symbol not in self.targets:
            return

        rate = SELL_RATE if not self.stop_profit else SECOND_SELL_RATE
        self.targets[symbol].set_buy_price(price, rate)

    def send_sell_signal(self, target: Target, price, now, vol, start_time):
        self.publish(topic=Topic.SELL_SIGNAL, symbol=target.symbol)
        target.own = False
        logger.info(f'Sell signal. {target.symbol} with price {price}USDT at {now}. recieved at {start_time}')

    def send_high_sell_signal(self, target: Target, start_time):
        if self.stop_profit:
            return

        self.publish(topic=Topic.STOP_PROFIT, status=True, symbol=target.symbol, price=target.stop_profit_price)
        target.own = False
        
        self.stop_profit = True
        logger.info(f'Stop profit. {target.symbol} comes to stop profit point {target.stop_profit}, sell all. recieved at {start_time}')

    @subscribe(topic=Topic.STOP_PROFIT)
    def stop_profit_handler(self, status, *arg, **kwargs):
        self.stop_profit = status

    def send_delay_sell(self, target: Target, price, now):
        symbol = target.symbol
        self.publish(topic=Topic.SELL_DELAY, symbol=symbol, price=price, time=now)
        target.new_high_price = price
        target.new_high_time = now
        logger.info(f'Delay sell {symbol} with price {price} at {now}')

    def send_not_delay_sell(self, target: Target, close, now):
        pass

    def send_delay_sell(self, symbol, close, now):
        next_sell_at = now + SELL_LEAST_AFTER
        self.publish(topic=Topic.SELL_DELAY, symbol=symbol, price=close, time=next_sell_at)
        self.targets[symbol].sell_least_time = next_sell_at
        logger.info(f'Delay sell {symbol} with price {close} at {now}, next sell at {next_sell_at}')


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
        self.symbols : list[str] = sorted(
            self.market_client.symbols_info.keys(),
            key=lambda symbol: self.market_client.mark_price[symbol],
            reverse=True
        )

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

    def get_task(self, num):
        self.task = self.req_task(num)

    @callee
    def req_task(self, num) -> 'list[str]':
        task = self.symbols[:num]
        self.symbols = self.symbols[num:]

        if not self.symbols:
            self.symbols = sorted(
                self.market_client.symbols_info.keys(),
                # key=lambda symbol: symbol,
                reverse=True
            )
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

    @subscribe(topic=Topic.BUY_SIGNAL)
    def buy_signal_handler(self, symbol, price, init_price, vol, now, *args, **kwargs):
        if self.state != State.RUNNING or symbol in self.targets or len(self.targets) >= MAX_BUY:
            return

        start_time = time.time()
        target = Target(symbol, price, init_price, now)
        target.set_info(self.market_client.symbols_info[symbol])
        self.targets[symbol] = target
        self.user.buy_and_sell([target])

        if target.buy_price > 0:
            self.publish(topic=Topic.AFTER_BUY, symbol=symbol, price=target.buy_price)
        else:
            del self.targets[symbol]
        logger.info(f'Buy {symbol}, signal time {now} recieved at {start_time}')

    @subscribe(topic=Topic.SELL_SIGNAL)
    def sell_signal_handler(self, symbol, *args, **kwargs):
        if self.state != State.RUNNING or symbol not in self.targets:
            return

        target = self.targets[symbol]
        if target.own:
            self.user.cancel_and_sell([target])
            target.own = False
            logger.info(f'Stop loss {symbol}')

    @subscribe(topic=Topic.STOP_PROFIT)
    def high_sell_handler(self, symbol, price, *args, **kwargs):
        if self.state != State.RUNNING:
            return

        time.sleep(HIGH_SELL_SLEEP)
        self.user.high_cancel_and_sell(self.targets.values(), symbol, price)
        logger.info(f'Stop profit {symbol} at {price} USDT')

    @subscribe(topic=Topic.SELL_DELAY)
    def delay_sell_handler(self, symbol, price, time, *args, **kwargs):
        if self.state != State.RUNNING or symbol not in self.targets:
            return

        target = self.targets[symbol]
        if target.own:
            target.sell_least_time = time
        logger.info(f'Delay sell {symbol} with {price}, next sell at {time}')

    def check_sell(self):
        # TODO
        while True:
            time.sleep(0.01)
            if self.state != State.RUNNING:
                continue

            now = time.time()
            own_targets = [target for target in self.targets.values() if target.own]

            if now > self.target_time + MAX_BUY_WAIT and not own_targets:
                self.state = State.STOPPED
                break
            elif not own_targets:
                continue

            sell_targets = [target for target in own_targets if now > target.sell_least_time]
            if sell_targets:
                amounts = [self.user.balance[target.base_currency] for target in sell_targets]
                self.user.sell(sell_targets, amounts)
                for target in sell_targets:
                    target.own = False


