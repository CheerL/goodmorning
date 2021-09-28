import time

from threading import Timer
from retry import retry
from market import MarketClient
from target import Target
from utils import config, logger, user_config
from wampy.roles.subscriber import subscribe
from client import ControlledClient, Topic, State, WS_URL
from user import BaseUser as User

STOP_PROFIT_SLEEP = config.getfloat('time', 'STOP_PROFIT_SLEEP')
REPORT_PRICE = user_config.getboolean('setting', 'REPORT_PRICE')


class BaseDealerClient(ControlledClient):
    def __init__(self, market_client: MarketClient, user: User, url=WS_URL):
        super().__init__(url=url)
        self.market_client : MarketClient = market_client
        self.targets = {}
        self.user = user
        self.client_type = 'base_dealer'

    @classmethod
    @retry(tries=5, delay=1, logger=logger)
    def init_dealer(cls, user):
        market_client = MarketClient()
        client = cls(market_client, user)
        client.start()
        return client

class SingleDealerClient:
    def __init__(self, market_client: MarketClient, user: User, *args, **kwargs):
        self.market_client : MarketClient = market_client
        self.targets = {}
        self.user = user
        self.client_type = 'single_dealer'
        self.state = 0

    @classmethod
    @retry(tries=5, delay=1, logger=logger)
    def init_dealer(cls, user):
        market_client = MarketClient()
        client = cls(market_client, user)
        # client.start()
        return client

    def wait_state(self, state=State.STARTED):
        while self.state != state:
            time.sleep(0.1)


class DealerClient(BaseDealerClient):
    def __init__(self, market_client: MarketClient, user: User, url=WS_URL):
        super().__init__(market_client=market_client, user=user, url=url)
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

        Timer(STOP_PROFIT_SLEEP, high_cancel_and_sell).start()
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
