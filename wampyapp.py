from wampy.peers.clients import Client, logger
from wampy.roles.subscriber import subscribe
from wampy.roles.callee import callee
import market
import user
import logging

from utils import config
SELL_AFTER = config.getfloat('setting', 'SellAfter')
MAX_BUY = config.getint('setting', 'MaxBuy')

from wampy.constants import (
    DEFAULT_TIMEOUT, DEFAULT_ROLES, DEFAULT_REALM,
)

BUY_SIGNAL_TOPIC = config.get('setting', 'BuySignalTopic')
SELL_SIGNAL_TOPIC = config.get('setting', 'SellSignalTopic')
WS_HOST = config.get('setting', 'WsHost')
WS_PORT = config.getint('setting', 'WsPort')
WS_URL = f'ws://{WS_HOST}:{WS_PORT}'

class WatcherClient(Client):
    def __init__(
        self, market_client: 'market.MarketClient', target_time: float, url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            url=url, cert_path=cert_path, ipv=ipv, name=name, realm=realm, roles=roles, call_timeout=call_timeout, message_handler_cls=message_handler_cls
        )
        price = market_client.get_price()
        market_client.exclude_expensive(price)
        self.symbols = sorted(
            market_client.symbols_info.keys(),
            key=lambda s: price[s][1],
            reverse=True)
        self.target_time = target_time
        self.market_client = market_client

    def get_task(self, num) -> 'list[str]':
        return self.rpc.get_task(num)

    def send_buy_signal(self, symbol, price, init_price):
        self.publish(topic=BUY_SIGNAL_TOPIC, symbol=symbol, price=price, init_price=init_price)

    def send_sell_signal(self, symbol, price, init_price):
        self.publish(topic=SELL_SIGNAL_TOPIC, symbol=symbol, price=price, init_price=init_price)


class WatcherMasterClient(WatcherClient):
    @callee
    def get_task(self, num) -> 'list[str]':
        task = self.symbols[:num]
        self.symbols = self.symbols[num:]
        return task

class DealerClient(Client):
    def __init__(
        self, market_client: 'market.MarketClient',
        users: 'list[user.User]',
        url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            url=url, cert_path=cert_path, ipv=ipv, name=name, realm=realm, roles=roles, call_timeout=call_timeout, message_handler_cls=message_handler_cls
        )
        self.market_client = market_client
        self.targets = []
        self.users = users

    # def start(self):
    #     super().start()


    @subscribe(topic=BUY_SIGNAL_TOPIC)
    def buy_signal_handler(self, symbol, price, init_price, *args, **kwargs):
        print(symbol, price, 'buy')
        if len(self.targets) >= MAX_BUY:
            return

        target = self.market_client.symbols_info[symbol]
        target.init_price = init_price
        target.buy_price = price
        # run_thread([
        #     (buy_and_sell, (user, [target], ))
        #     for user in self.users
        # ], is_lock=False)
        self.targets.append(target)

    @subscribe(topic=SELL_SIGNAL_TOPIC)
    def sell_signal_handler(self, symbol, price, initail_price, *args, **kwargs):
        print(symbol, price, 'sell')
        target = self.market_client.symbols_info[symbol]
        # run_thread([(user.cancel_and_sell, ([target], )) for user in self.users], is_lock=False)
