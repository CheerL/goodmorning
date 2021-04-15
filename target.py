from utils import config

SELL_LEAST_AFTER = config.getfloat('setting', 'SellLeastAfter')
SELL_BACK_RATE = config.getfloat('setting', 'SellBackRate')

class Target:
    def __init__(self, symbol, price, init_price=None, time=None):
        self.symbol = symbol
        self.price = price
        self.init_price = init_price
        self.buy_price = 0
        self.high_price = 0

        self.time = time

        self.sell_least_time = time + SELL_LEAST_AFTER
        self.sell_least_price = init_price * (1 - SELL_BACK_RATE / 100)
        self.own = True

    def set_buy_price(self, price, rate):
        if not self.buy_price:
            self.buy_price = price
        else:
            self.buy_price = min(self.buy_price, price)
        
        self.high_price = self.buy_price * (1 + rate / 100)

    def set_info(self, info):
        self.base_currency = info.base_currency
        self.amount_precision = info.amount_precision
        self.min_order_value = info.min_order_value
        self.sell_market_min_order_amt = info.sell_market_min_order_amt
        self.limit_order_min_order_amt = info.limit_order_min_order_amt
