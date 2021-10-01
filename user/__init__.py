import time
import math

from gevent import monkey
monkey.patch_all()

from order import OrderSummary
from report import wx_name
from utils import logger, user_config, timeout_handle
from retry import retry
from target import BaseTarget as Target


class BaseMarketClient:
    exclude_list = []
    exclude_price = 10

    def __init__(self, **kwargs):
        self.all_symbol_info = {}
        self.symbols_info = {}
        self.mark_price: 'dict[str, float]' = {}

    def exclude(self, infos, base_price):
        return {
            symbol: info
            for symbol, info in infos.items()
            if symbol not in self.exclude_list
            and symbol in base_price
            and base_price[symbol] < self.exclude_price
        }

    def get_all_symbols_info(self):
        return {}

    def update_symbols_info(self) -> 'tuple[list[str], list[str]]':
        new_symbols_info = self.get_all_symbols_info()
        if len(self.all_symbol_info) != len(new_symbols_info):
            self.all_symbol_info = new_symbols_info

        price = self.get_price()
        symbols_info = self.exclude(new_symbols_info, price)
        new_symbols = [symbol for symbol in symbols_info.keys() if symbol not in self.symbols_info]
        removed_symbols = [symbol for symbol in self.symbols_info.keys() if symbol not in symbols_info]
        self.symbols_info = symbols_info
        self.mark_price = price
        return new_symbols, removed_symbols

    def get_market_tickers(self, **kwargs):
        raise NotImplementedError

    @timeout_handle({})
    def get_price(self) -> 'dict[str, float]':
        return {
            pair.symbol: pair.close
            for pair in self.get_market_tickers()
        }

    @timeout_handle({})
    def get_vol(self) -> 'dict[str, float]':
        return {
            pair.symbol: pair.vol
            for pair in self.get_market_tickers(all_info=True)
        }

class BaseUser:
    user_type = 'Base'
    MarketClient = BaseMarketClient
    min_usdt_amount = 0

    def __init__(self, access_key, secret_key, buy_amount, wxuid):
        self.access_key = access_key
        self.sercet_key = secret_key
        self.account_id = self.get_account_id()

        self.balance: dict[str, float] = {}
        self.available_balance: dict[str, float] = {}
        self.balance_update_time: dict[str, float] = {}
        self.orders: dict[int, OrderSummary] = {}
        self.wxuid = wxuid.split(';')

        self.buy_id = []
        self.sell_id = []
        self.username = wx_name(self.wxuid[0])
        self.buy_amount = buy_amount
        self.market_client = self.MarketClient()
        self.scheduler = None

    @classmethod
    @retry(tries=5, delay=1, logger=logger)
    def init_users(cls, num=-1):
        ACCESSKEY = user_config.get('setting', f'{cls.user_type}AccessKey')
        SECRETKEY = user_config.get('setting', f'{cls.user_type}SecretKey')
        WXUIDS = user_config.get('setting', 'WxUid')
        BUY_AMOUNT = user_config.get('setting', 'BuyAmount')
        TEST = user_config.getboolean('setting', 'Test')

        access_keys = [key.strip() for key in ACCESSKEY.split(',')]
        secret_keys = [key.strip() for key in SECRETKEY.split(',')]
        wxuids = [uid.strip() for uid in WXUIDS.split(',')]
        buy_amounts = [amount.strip() for amount in BUY_AMOUNT.split(',')]

        if num == -1:
            users = [cls(*user_data) for user_data in zip(access_keys, secret_keys, buy_amounts, wxuids)]
        else:
            users = [cls(access_keys[num], secret_keys[num], buy_amounts[num], wxuids[num])]
                                                    
        if TEST:
            users = users[:1]
        return users

    def get_account_id(self) -> int:
        raise NotImplementedError

    def get_order(self, order_id):
        raise NotImplementedError

    def cancel_order(self, order_id):
        raise NotImplementedError

    @retry(tries=5, delay=0.05, logger=logger)
    def get_amount(self, currency: str, available=False, check=True):
        if currency in self.balance:
            pass
        elif currency.upper() in self.balance:
            currency = currency.upper()
        else:
            return 0

        if available:
            return self.available_balance[currency]
        if check:
            assert self.balance[currency] - self.available_balance[currency] < 1e-8, 'unavailable'
        return self.balance[currency]

    def start(self, **kwargs):
        raise NotImplementedError

    def buy(self, target: Target, vol):
        raise NotImplementedError

    def buy_limit(self, target: Target, vol, price=None):
        raise NotImplementedError

    def sell(self, target: Target, amount):
        raise NotImplementedError
        

    def sell_limit(self, target: Target, amount, price, ioc=False):
        raise NotImplementedError
