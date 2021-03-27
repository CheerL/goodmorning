from wampy.peers.clients import Client
from wampy.roles.subscriber import subscribe
from wampy.roles.callee import callee
from market import MarketClient
from user import User
import time

from logger import quite_logger
from utils import config, logger, get_target_time
from target import Target
# from parallel import run_thread


from wampy.constants import (
    DEFAULT_TIMEOUT, DEFAULT_ROLES, DEFAULT_REALM,
)

DEALER_NUM = config.getint('setting', 'DealerNum')
WATCHER_NUM = config.getint('setting', 'WatcherNum')
SELL_AFTER = config.getfloat('setting', 'SellAfter')
MAX_BUY = config.getint('setting', 'MaxBuy')

WS_HOST = config.get('setting', 'WsHost')
WS_PORT = config.getint('setting', 'WsPort')
WS_URL = f'ws://{WS_HOST}:{WS_PORT}'

RUN_TOPIC = 'run'
CLIENT_INFO_TOPIC = 'info'
BUY_SIGNAL_TOPIC = 'buy'
SELL_SIGNAL_TOPIC = 'sell'

quite_logger(all_logger=True)

class ControlledClient(Client):
    def __init__(
        self, url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            url=url, cert_path=cert_path, ipv=ipv, name=name, realm=realm, roles=roles, call_timeout=call_timeout, message_handler_cls=message_handler_cls
        )
        self.run = False
        self.target_time = None
        self.client_type = 'unknown'

    @subscribe(topic=RUN_TOPIC)
    def run_handler(self, target_time,  *args, **kwargs):
        if not self.run:
            self.run = True
            self.target_time = target_time
            logger.info(f'Run {self.client_type}')

    def wait_to_run(self):
        while not self.run:
            time.sleep(1)

    def start(self):
        super().start()
        self.after_start()

    def after_start(self):
        self.publish(topic=CLIENT_INFO_TOPIC, client_type=self.client_type, remove=False)

    def stop(self):
        self.before_stop()
        try:
            super().stop()
        except Exception as e:
            logger.error(e)
        

    def before_stop(self):
        self.publish(topic=CLIENT_INFO_TOPIC, client_type=self.client_type, remove=True)

class WatcherClient(ControlledClient):
    def __init__(
        self, market_client: MarketClient, url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            url=url, cert_path=cert_path, ipv=ipv, name=name, realm=realm, roles=roles, call_timeout=call_timeout, message_handler_cls=message_handler_cls
        )
        self.market_client = market_client
        self.client_type = 'watcher'

    def get_task(self, num) -> 'list[str]':
        return self.rpc.get_task(num)

    def send_buy_signal(self, symbol, price, init_price, now):
        self.publish(topic=BUY_SIGNAL_TOPIC, symbol=symbol, price=price, init_price=init_price)
        self.market_client.targets[symbol] = Target(symbol, price, init_price, now)
        increase = round((price - init_price) / init_price * 100, 4)
        logger.info(f'Buy {symbol} with price {price}USDT, increament {increase}% at {now}')
        
    def send_sell_signal(self, symbol, price, init_price, now):
        self.publish(topic=SELL_SIGNAL_TOPIC, symbol=symbol, price=price, init_price=init_price)
        self.market_client.targets[symbol].own = False
        increase = round((price - init_price) / init_price * 100, 4)
        logger.info(f'Sell {symbol} with price {price}USDT, increament {increase}% at {now}')

class WatcherMasterClient(WatcherClient):
    def __init__(
        self, market_client: MarketClient, url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            market_client=market_client, url=url, cert_path=cert_path, ipv=ipv, name=name, realm=realm, roles=roles, call_timeout=call_timeout, message_handler_cls=message_handler_cls
        )
        price = market_client.get_price()
        market_client.exclude_expensive(price)
        self.symbols = sorted(
            market_client.symbols_info.keys(),
            key=lambda s: price[s][1],
            reverse=True)
        self.client_info = {
            'watcher': 0,
            'dealer': 0
        }

    def after_start(self):
        self.info_handler(self.client_type)

    def before_stop(self):
        self.info_handler(self.client_type, True)

    @callee
    def get_task(self, num) -> 'list[str]':
        task = self.symbols[:num]
        self.symbols = self.symbols[num:]
        return task

    @subscribe(topic=CLIENT_INFO_TOPIC)
    def info_handler(self, client_type, remove=False, *args, **kwargs):
        self.client_info[client_type] += 1 if not remove else -1

        
        if self.client_info['watcher'] >= WATCHER_NUM and self.client_info['dealer'] >= DEALER_NUM:
            if not self.run:
                self.target_time = get_target_time()
                self.run = True
                logger.info('Run all')
            else:
                logger.info('Run new')
            self.publish(topic=RUN_TOPIC, target_time=self.target_time)

class DealerClient(ControlledClient):
    def __init__(
        self, market_client: MarketClient,
        user: User,
        url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            url=url, cert_path=cert_path, ipv=ipv, name=name, realm=realm, roles=roles, call_timeout=call_timeout, message_handler_cls=message_handler_cls
        )
        self.market_client = market_client
        self.targets = []
        self.user = user
        self.client_type = 'dealer'

    @subscribe(topic=BUY_SIGNAL_TOPIC)
    def buy_signal_handler(self, symbol, price, init_price, *args, **kwargs):
        if not self.run:
            return

        if len(self.targets) >= MAX_BUY:
            return

        target = self.market_client.symbols_info[symbol]
        target.init_price = init_price
        target.buy_price = price

        self.buy_and_sell([target])

        # run_thread([
        #     (buy_and_sell, (user, [target], ))
        #     for user in self.users
        # ], is_lock=False)
        self.targets.append(target)
        increase = round((price - init_price) / init_price * 100, 4)
        logger.info(f'Buy {symbol} with price {price}USDT, increament {increase}% at {time.time()}')

    @subscribe(topic=SELL_SIGNAL_TOPIC)
    def sell_signal_handler(self, symbol, price, init_price, *args, **kwargs):
        if not self.run:
            return

        if symbol not in [target.symbol for target in self.targets]:
            return

        target = self.market_client.symbols_info[symbol]

        self.user.cancel_and_sell([target])

        # run_thread([(user.cancel_and_sell, ([target], )) for user in self.users], is_lock=False)
        increase = round((price - init_price) / init_price * 100, 4)
        logger.info(f'Sell {symbol} with price {price}USDT, increament {increase}% at {time.time()}')