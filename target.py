from utils import config
import math

MIN_STOP_LOSS_HOLD_TIME = config.getfloat('time', 'MIN_STOP_LOSS_HOLD_TIME')
STOP_LOSS_RATE = config.getfloat('sell', 'STOP_LOSS_RATE')
STOP_PROFIT_RATE_HIGH = config.getfloat('sell', 'STOP_PROFIT_RATE_HIGH')
STOP_PROFIT_RATE_LOW = config.getfloat('sell', 'STOP_PROFIT_RATE_LOW')
BUY_RATE = config.getfloat('buy', 'BUY_RATE')

class Target:
    def __init__(self, symbol, price, time, high_stop_profit=True):
        self.symbol = symbol
        self.price = price
        self.init_price = 0
        self.buy_price = 0
        self.high_price = 0
        self.time = time
        self.stop_loss_price = 0
        self.min_stop_loss_hold_time = time + MIN_STOP_LOSS_HOLD_TIME
        self.stop_profit_price = 0
        self.own = False
        self.high_stop_profit = high_stop_profit

    def set_buy_price(self, price):
        if price <= 0:
            return

        if not self.buy_price:
            self.buy_price = price
        else:
            self.buy_price = min(self.buy_price, price)
        self.set_high_stop_profit(self.high_stop_profit)
        self.own = True

    def set_high_stop_profit(self, status):
        self.high_stop_profit = status
        rate = STOP_PROFIT_RATE_HIGH if self.high_stop_profit else STOP_PROFIT_RATE_LOW
        self.stop_profit_price = self.buy_price * (1 + rate / 100)

    def get_buy_price(self):
        buy_price = (1 + BUY_RATE / 100) * self.price
        # return self.check_price(buy_price)
        return buy_price

    def set_info(self, info):
        self.init_price = info.init_price
        self.base_currency = info.base_currency
        self.amount_precision = info.amount_precision
        self.price_precision = info.price_precision
        self.min_order_value = info.min_order_value
        self.sell_market_min_order_amt = info.sell_market_min_order_amt
        self.limit_order_min_order_amt = info.limit_order_min_order_amt
        self.stop_loss_price = self.init_price * (1 - STOP_LOSS_RATE / 100)

    def check_amount(self, amount):
        precision_num = 10 ** self.amount_precision
        return math.floor(amount * precision_num) / precision_num

    def check_price(self, price):
        precision_num = 10 ** self.price_precision
        return math.floor(price * precision_num) / precision_num