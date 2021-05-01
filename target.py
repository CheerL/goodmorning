from utils import config
import math

SELL_LEAST_AFTER = config.getfloat('setting', 'SellLeastAfter')
SELL_BACK_RATE = config.getfloat('setting', 'SellBackRate')

DELAY = config.getfloat('setting', 'Delay')
SECOND_DELAY = config.getfloat('setting', 'SecondDelay')
SELL_RATE = config.getfloat('setting', 'SellRate')
SECOND_SELL_RATE = config.getfloat('setting', 'SecondSellRate')

class Target:
    def __init__(self, symbol, price, init_price, time=None):
        self.symbol: str = symbol
        self.price: float = price
        self.init_price: float = init_price
        self.buy_price: float = 0
        self.stop_profit_price: float = 0
        self.stop_loss_price = init_price * (1 - SELL_BACK_RATE / 100)
        self.new_high_time: float = time
        self.new_high_price: float = price
        self.own: bool = True
        self.high: bool = True

    def set_buy_price(self, price, rate):
        if not self.buy_price:
            self.buy_price = price
        else:
            self.buy_price = min(self.buy_price, price)
        
        rate = SELL_RATE if self.high else SECOND_SELL_RATE
        self.stop_profit_price = self.buy_price * (1 + rate / 100)

    def set_info(self, info):
        self.base_currency = info.base_currency
        self.amount_precision = info.amount_precision
        self.price_precision = info.price_precision
        self.min_order_value = info.min_order_value
        self.sell_market_min_order_amt = info.sell_market_min_order_amt
        self.limit_order_min_order_amt = info.limit_order_min_order_amt

    def check_amount(self, amount):
        precision_num = 10 ** self.amount_precision
        return math.floor(amount * precision_num) / precision_num

    def check_price(self, price):
        precision_num = 10 ** self.price_precision
        return math.floor(price * precision_num) / precision_num