import time

from wampy.constants import DEFAULT_REALM, DEFAULT_ROLES, DEFAULT_TIMEOUT
from wampy.peers.clients import Client
from wampy.roles.callee import callee
from wampy.roles.subscriber import subscribe

from logger import quite_logger
from market import MarketClient
from record import write_target
from target import Target
from user import User
from utils import config, get_target_time, logger

DEALER_NUM = config.getint('setting', 'DealerNum')
WATCHER_NUM = config.getint('setting', 'WatcherNum')
SELL_AFTER = config.getfloat('setting', 'SellAfter')
MAX_BUY = config.getint('setting', 'MaxBuy')
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

class State:
    STOPPED = 0
    STARTED = 1
    RUNNING = 2


# quite_logger(all_logger=True)


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
        self.client_type = 'watcher'
        self.stop_profit = False
        self.task : list[str] = []
        self.targets : list[Target] = {}

    def get_task(self, num) -> 'list[str]':
        self.task = self.rpc.req_task(num)

    def send_buy_signal(self, symbol, price, init_price, now, vol):
        self.publish(topic=Topic.BUY_SIGNAL, symbol=symbol, price=price, init_price=init_price, vol=vol, now=now)
        target = Target(symbol, price, init_price, now)
        self.targets[symbol] = target
        increase = round((price - init_price) / init_price * 100, 4)
        logger.info(f'Buy signal. {symbol} with price {price}USDT, vol {vol}, increament {increase}% at {now}')

        time.sleep(1)
        if target.buy_price == 0:
            del self.targets[symbol]
        write_target(symbol)

    def send_sell_signal(self, symbol, price, init_price, now, vol):
        self.publish(topic=Topic.SELL_SIGNAL, symbol=symbol, price=price, init_price=init_price, vol=vol, now=now)
        self.targets[symbol].own = False
        increase = round((price - init_price) / init_price * 100, 4)
        logger.info(f'Sell signal. {symbol} with price {price}USDT, vol {vol} increament {increase}% at {now}')

    def send_high_sell_signal(self, symbol):
        if self.stop_profit:
            return

        self.publish(topic=Topic.HIGH, symbol=symbol, price=self.targets[symbol].high_price)
        for target in self.targets.values():
            target.own = False

        self.stop_profit = True
        self.publish(topic=Topic.STOP_PROFIT, status=True)
        logger.info(f'Stop profit. {symbol} comes to stop profit point {target.high_price}, sell all')

    @subscribe(topic=Topic.STOP_PROFIT)
    def stop_profit_handler(self, status, *arg, **kwargs):
        self.stop_profit = status

    @subscribe(topic=Topic.AFTER_BUY)
    def after_buy_handler(self, symbol, price, *args, **kwargs):
        if symbol not in self.targets:
            return

        rate = SECOND_SELL_RATE if self.stop_profit else SELL_RATE
        self.targets[symbol].set_buy_price(price, rate)


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
            market_client.symbols_info.keys(),
            key=lambda symbol: self.market_client.mark_price[symbol],
            reverse=True)

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
        return task

    @subscribe(topic=Topic.CLIENT_INFO)
    def info_handler(self, client_type, remove=False, *args, **kwargs):
        if remove:
            self.client_info[client_type] -= 1

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
        self.targets : list[Target] = {}
        self.user : User = user
        self.client_type = 'dealer'


    @subscribe(topic=Topic.BUY_SIGNAL)
    def buy_signal_handler(self, symbol, price, init_price, vol, now, *args, **kwargs):
        if not self.state == State.RUNNING or symbol in self.targets or len(self.targets) >= MAX_BUY:
            return

        target = Target(symbol, price, init_price, now)
        target.set_info(self.market_client.symbols_info[symbol])

        self.user.buy_and_sell([target])

        if target.buy_price > 0:
            self.publish(topic=Topic.AFTER_BUY, symbol=symbol, price=target.buy_price)
            self.targets[symbol] = target

    @subscribe(topic=Topic.SELL_SIGNAL)
    def sell_signal_handler(self, symbol, price, init_price, vol, now, *args, **kwargs):
        if self.state != State.RUNNING or symbol not in self.targets:
            return

        target = self.targets[symbol]
        self.user.cancel_and_sell([target])
        logger.info(f'Stop loss{symbol} at {price} USDT')

    @subscribe(topic=Topic.HIGH)
    def high_sell_handler(self, symbol, price, *args, **kwargs):
        if self.state != State.RUNNING:
            return

        time.sleep(HIGH_SELL_SLEEP)
        self.user.high_cancel_and_sell(self.targets.values(), symbol, price)
        logger.info(f'Stop profit {symbol} at {price} USDT')
