from order import OrderSummary
from report import wx_name
from utils import logger, user_config
from retry import retry
from target import BaseTarget as Target

class BaseUser:
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
        self.type = 'base'

    @classmethod
    @retry(tries=5, delay=1, logger=logger)
    def init_users(cls, num=-1) -> 'list[BaseUser]':
        ACCESSKEY = user_config.get('setting', 'AccessKey')
        SECRETKEY = user_config.get('setting', 'SecretKey')
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

    def get_asset(self) -> float:
        raise NotImplementedError

    def get_account_id(self) -> int:
        raise NotImplementedError

    def get_order(self, order_id):
        raise NotImplementedError

    def cancel_order(self, order_id):
        raise NotImplementedError

    @retry(tries=5, delay=0.05, logger=logger)
    def get_amount(self, currency, available=False, check=True):
        if currency not in self.balance:
            return 0
        if available:
            return self.available_balance[currency]
        if check:
            assert self.balance[currency] - self.available_balance[currency] < 1e-8, 'unavailable'
        return self.balance[currency]

    def start(self, balance_update_kwargs={}, order_update_kwargs={}):
        self.sub_balance_update(**balance_update_kwargs)
        # self.sub_order_update(**order_update_kwargs)

        # while 'usdt' not in self.balance:
        #     time.sleep(0.1)

        # usdt = self.balance['usdt']
        # if isinstance(self.buy_amount, str) and self.buy_amount.startswith('/'):
        #     self.buy_amount = max(math.floor(usdt / float(self.buy_amount[1:])), 5)
        # else:
        #     self.buy_amount = float(self.buy_amount)

    def sub_balance_update(self, **kwargs):
        raise NotImplementedError

    def sub_order_update(self, **kwargs):
        raise NotImplementedError

    def buy(self, target: Target, vol):
        raise NotImplementedError

    def buy_limit(self, target: Target, vol, price=None):
        raise NotImplementedError

    def sell(self, target: Target, amount):
        raise NotImplementedError
        

    def sell_limit(self, target: Target, amount, price, ioc=False):
        raise NotImplementedError
