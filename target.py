from utils import config

SELL_LEAST_AFTER = config.getfloat('setting', 'SellLeastAfter')
SELL_AT_BACK = config.getfloat('setting', 'SellAtBack')

class Target:
    def __init__(self, symbol, price, init_price=None, time=None):
        self.symbol = symbol
        self.price = price
        self.init_price = init_price
        self.time = time

        self.sell_least_time = time + SELL_LEAST_AFTER
        self.sell_least_price = price * (100 - SELL_AT_BACK) / 100
        self.own = True
