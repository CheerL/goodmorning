from utils import config
from utils.datetime import ts2date
import math

MIN_STOP_LOSS_HOLD_TIME = config.getfloat('time', 'MIN_STOP_LOSS_HOLD_TIME')
STOP_LOSS_RATE = config.getfloat('sell', 'STOP_LOSS_RATE')
STOP_PROFIT_RATE_HIGH = config.getfloat('sell', 'STOP_PROFIT_RATE_HIGH')
STOP_PROFIT_RATE_LOW = config.getfloat('sell', 'STOP_PROFIT_RATE_LOW')
BUY_RATE = config.getfloat('buy', 'BUY_RATE')
HIGH_RATE = config.getfloat('loss', 'HIGH_RATE')
LOW_RATE = config.getfloat('loss', 'LOW_RATE')
SELL_RATE = config.getfloat('loss', 'SELL_RATE')
AVER_INTERVAL_LENGTH = config.getfloat('loss', 'AVER_INTERVAL_LENGTH')
PRICE_INTERVAL = config.getfloat('loss', 'PRICE_INTERVAL')
AVER_NUM = int(AVER_INTERVAL_LENGTH // PRICE_INTERVAL)

class BaseTarget:
    def __init__(self, symbol, price, time):
        self.symbol = symbol
        self.price = price
        self.init_price = 0
        self.buy_price = 0
        self.time = time

        self.own = False

    def set_buy_price(self, price):
        if price <= 0:
            return

        if not self.buy_price:
            self.buy_price = price
        else:
            self.buy_price = min(self.buy_price, price)
        self.own = True

    def get_target_buy_price(self, rate=BUY_RATE):
        buy_price = (1 + rate / 100) * self.now_price
        # return self.check_price(buy_price)
        return buy_price

    def set_info(self, info):
        # self.init_price = info.init_price
        self.base_currency = info.base_currency
        self.amount_precision = info.amount_precision
        self.price_precision = info.price_precision
        self.min_order_value = info.min_order_value
        self.sell_market_min_order_amt = info.sell_market_min_order_amt
        self.limit_order_min_order_amt = info.limit_order_min_order_amt

    def check_amount(self, amount):
        checked_amount = round(amount, self.amount_precision)
        if checked_amount > amount:
            return round(amount - 0.1 ** self.amount_precision, self.amount_precision)
        else:
            return checked_amount

    def check_price(self, price):
        return round(price, self.price_precision)

class MorningTarget(BaseTarget):
    def __init__(self, symbol, price, time, high_stop_profit=True):
        super().__init__(symbol, price, time) 
        self.stop_loss_price = 0
        self.stop_profit_price = 0
        self.min_stop_loss_hold_time = time + MIN_STOP_LOSS_HOLD_TIME
        self.high_stop_profit = high_stop_profit

    def set_buy_price(self, price):
        super().set_buy_price(price)
        self.set_high_stop_profit(self.high_stop_profit)

    def set_high_stop_profit(self, status):
        self.high_stop_profit = status
        rate = STOP_PROFIT_RATE_HIGH if self.high_stop_profit else STOP_PROFIT_RATE_LOW
        self.stop_profit_price = self.buy_price * (1 + rate / 100)


class LossTarget(BaseTarget):
    def __init__(self, symbol, date, open_, close, vol):
        super().__init__(symbol, close, 0)
        self.own_amount = 0
        self.open = open_
        self.close = close
        self.vol = vol
        self.date = date
        self.buy_vol = 0
        self.selling = 0
        self.ticker_id = 0
        self.recent_price = []
        self.now_price = 0
        self.fee_rate = 0

        self.high_mark_price = 0
        self.high_mark_back_price = 0
        self.high_mark = False

        self.low_mark_price = 0
        self.low_mark = False
        self.sell_price = 0

        self.set_init_price(close)

    def set_info(self, info, fee_rate):
        super().set_info(info)
        self.fee_rate = fee_rate

    def set_init_price(self, price, high_rate=HIGH_RATE, low_rate=LOW_RATE):
        self.init_price = price
        self.high_mark_price = price * (1+high_rate)
        self.high_mark_back_price = price * (1+high_rate/2)
        self.low_mark_price = max(price * (1+low_rate), (self.open+price)/2)

    def set_buy(self, vol, amount, sell_rate=SELL_RATE):
        if vol <= 0 or amount <= 0:
            return

        own_vol = self.own_amount * self.buy_price
        self.own = True
        self.own_amount += amount * (1 - self.fee_rate)
        self.buy_vol += vol
        if self.own_amount:
            self.buy_price = (own_vol+vol) / self.own_amount * (1 - self.fee_rate)
            self.sell_price = self.buy_price * (1+sell_rate)

    def set_sell(self, amount, vol=0):
        if amount <= 0:
            return

        self.own_amount -= amount
        self.buy_vol -= vol
        if self.own_amount <= self.sell_market_min_order_amt:
            self.own = False

    def update_price(self, tickers, num=AVER_NUM):
        if tickers[self.ticker_id].symbol != self.symbol:
            for i, ticker in enumerate(tickers):
                if ticker.symbol == self.symbol:
                    self.ticker_id = i
                    break
        else:
            ticker = tickers[self.ticker_id]

        self.now_price = ticker.close
        self.recent_price.append(self.now_price)
        self.recent_price = self.recent_price[-num:]

        if self.recent_price:
            self.price = sum(self.recent_price) / len(self.recent_price)