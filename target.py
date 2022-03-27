import numpy as np
from utils import config, logger, get_boll, get_level

MIN_STOP_LOSS_HOLD_TIME = config.getfloat('time', 'MIN_STOP_LOSS_HOLD_TIME')
STOP_LOSS_RATE = config.getfloat('sell', 'STOP_LOSS_RATE')
STOP_PROFIT_RATE_HIGH = config.getfloat('sell', 'STOP_PROFIT_RATE_HIGH')
STOP_PROFIT_RATE_LOW = config.getfloat('sell', 'STOP_PROFIT_RATE_LOW')
BUY_RATE = config.getfloat('buy', 'BUY_RATE')
HIGH_RATE = config.getfloat('loss', 'HIGH_RATE')
LOW_RATE = config.getfloat('loss', 'LOW_RATE')
FINAL_RATE = config.getfloat('loss', 'FINAL_RATE')
FINAL_MODIFY_RATE = config.getfloat('loss', 'FINAL_MODIFY_RATE')
CLEAR_RATE = config.getfloat('loss', 'CLEAR_RATE')
HIGH_BACK_RATE = config.getfloat('loss', 'HIGH_BACK_RATE')
LOW_BACK_RATE = config.getfloat('loss', 'LOW_BACK_RATE')
BACK_BEFORE_RATE = config.getfloat('loss', 'BACK_BEFORE_RATE')
AVER_INTERVAL_LENGTH = config.getfloat('loss', 'AVER_INTERVAL_LENGTH')
PRICE_INTERVAL = config.getfloat('loss', 'PRICE_INTERVAL')
UP_NEAR_RATE = config.getfloat('loss', 'UP_NEAR_RATE')
UP_NEAR_RATE_FAKE = config.getfloat('loss', 'UP_NEAR_RATE_FAKE')
BUY_UP_RATE = config.getfloat('loss', 'BUY_UP_RATE')

AVER_NUM = int(AVER_INTERVAL_LENGTH // PRICE_INTERVAL)

LEVEL = config.get('loss', 'LEVEL')
_, LEVEL_TS = get_level(LEVEL)

class BaseTarget:
    def __init__(self, symbol, price, time):
        self.symbol = symbol
        self.price = price
        self.init_price = 0
        self.buy_price = 0
        self.time = time

        self.own = False

        self.base_currency = ''
        self.amount_precision = 0
        self.price_precision = 0
        self.min_order_value = 0
        self.sell_market_min_order_amt = 0
        self.limit_order_min_order_amt = 0

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

    def check_amount(self, amount, to_str=False):
        if amount <= 0:
            checked_amount = 0
        else:
            checked_amount = round(amount, self.amount_precision)
            if checked_amount < 0:
                checked_amount = 0
            elif checked_amount > amount:
                checked_amount = round(checked_amount - 0.1 ** self.amount_precision, self.amount_precision)

        if to_str:
            return f'%.{self.amount_precision}f' % checked_amount
        return checked_amount

    def check_price(self, price, to_str=False):
        checked_price = round(price, self.price_precision)
        if to_str:
            return f'%.{self.price_precision}f' % checked_price
        return checked_price

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
        self.now_price = close
        self.init_buy_amount = 0
        self.buy_vol = 0
        self.selling = 0
        self.ticker_id = 0
        self.recent_price = []
        self.fee_rate = 0
        self.buy_price = 0
        self.real_buy_price = 0
        self.clear_price = 0
        self.long_sell_price = 0

        self.high_mark_price = 0
        self.high_mark_back_price = 0
        self.high_mark = False
        self.high_selling = False

        self.low_mark_price = 0
        self.low_mark = False
        self.low_selling = False

        self.his_close = {}
        self.boll = 0
        self.boll_target_buy_price = 0
        self.boll_target_sell_price = 0

        self.set_mark_price(close)

    def __repr__(self) -> str:
        return f'<LossTarget symbol={self.symbol} date={self.date} close={self.close} high_mark={self.high_mark} low_mark={self.low_mark} own_amount={self.own_amount} buy_price={self.buy_price} buy_vol={self.buy_vol} init_buy_vol={self.init_buy_amount}>'

    def update_his_close(self, klines, fake=False):
        for kline in klines:
            self.his_close[kline.id]=kline.close
        
        if fake:
            max_ts = max(self.his_close.keys())
            self.his_close[max_ts+LEVEL_TS] = self.his_close[max_ts]

    def get_boll(self, ts, std_range=[0]):
        time_list = sorted([time for time in self.his_close.keys() if time <= ts])[-20:]
        close_list = [self.his_close[time] for time in time_list]
        bolls = get_boll(close_list, std_range)
        return bolls, time_list, close_list

    def update_buy_price(self, ts, std_range=[2,1.5,1,0.5,0,-0.5,-1,-1.5,-2]):
        bolls, time_list, close_list = self.get_boll(ts, std_range)
        last_ts = time_list[-2]
        last_open, last_close, close = close_list[-3:]
        old_bolls, _, _ = self.get_boll(last_ts, [0])
        if bolls.size and old_bolls.size:
            self.boll = boll = bolls[int((len(std_range)-1)/2)]
            last_boll_mid = old_bolls[0]

            close_pos = bolls[bolls>close].size
            if close_pos <= 0:
                buy_price = bolls[0] * (1 + BUY_UP_RATE)
            elif close_pos == 1:
                buy_price = bolls[1] * (1 + BUY_UP_RATE)
            elif close_pos >= bolls.size:
                buy_price = self.close
            elif last_boll_mid > last_open > last_close and boll > close:
                buy_price = bolls[-1] * (1 + BUY_UP_RATE)
            else:
                up_price = bolls[close_pos-1]
                down_price = bolls[close_pos]
                
                if (self.close - down_price) / (up_price - down_price) > UP_NEAR_RATE_FAKE and not close_pos % 2:
                    buy_price = close
                elif (self.close - down_price) / (up_price - down_price) > UP_NEAR_RATE and close_pos % 2:
                    buy_price = close
                else:
                    buy_price = down_price * (1 + BUY_UP_RATE)
            
            self.boll_target_buy_price = buy_price
            logger.info(f'Boll: {bolls}, close: {close}, sell_price: {buy_price}')
            # self.boll_target_buy_price = buy_price * (1+buy_up_rate)

    def update_sell_price(self, ts, std_range=[2,1,0,-1,-2]):
        bolls, _, close_list = self.get_boll(ts, std_range)
        last_close = close_list[-1]
        if bolls.size:
            pos = bolls[bolls>last_close].size
            if pos == 0:
                sell_price = last_close
            else:
                sell_price = bolls[pos-1]
                diff = bolls[0]-bolls[1]
                if (sell_price-last_close) / diff < FINAL_RATE:
                    sell_price += diff * FINAL_MODIFY_RATE

            self.long_sell_price = self.boll_target_sell_price = sell_price
            logger.info(f'Boll: {bolls}, close: {last_close}, sell_price: {sell_price}')
            # self.long_sell_price = self.boll_target_sell_price = sell_price * (1+sell_down_rate)

    def set_info(self, info, fee_rate):
        super().set_info(info)
        self.ice_part = info.ice_part
        self.fee_rate = fee_rate

    def set_mark_price(self, price,
        high_rate=HIGH_RATE, low_rate=LOW_RATE, high_back_rate=HIGH_BACK_RATE,
        low_back_rate=LOW_BACK_RATE, FINAL_RATE=FINAL_RATE, clear_rate=CLEAR_RATE
    ):
        self.init_price = price
        self.high_mark_price = max(price * (1+high_rate), (self.open+price)/2)
        self.high_mark_back_price = (self.high_mark_price - price) * high_back_rate + price
        self.low_mark_price = price * (1+low_rate)
        self.low_mark_back_price = price * (1+low_back_rate)
        logger.info(f'{self.symbol} of {self.date} price init {self.init_price}, high mark {self.high_mark_price}, high back {self.high_mark_back_price}, low mark {self.low_mark_price}, low back {self.low_mark_back_price}, clear {self.clear_price}, long sell {self.long_sell_price}')

    def set_buy(self, vol, amount, clear_rate=CLEAR_RATE, fee_rate=None):
        if vol <= 0 or amount <= 0:
            return
        fee_rate = self.fee_rate if fee_rate == None else fee_rate

        own_vol = self.own_amount * self.buy_price
        self.own = True
        self.own_amount += amount * (1 - fee_rate)
        self.buy_vol += vol
        if self.own_amount:
            self.real_buy_price = (own_vol+vol) / self.own_amount 
            self.buy_price = self.real_buy_price * (1 - fee_rate)
            self.clear_price = self.buy_price * (1+clear_rate)

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

        self.price = self.now_price = ticker.close
        # self.recent_price.append(self.now_price)
        # self.recent_price = self.recent_price[-num:]

        # if self.recent_price:
        #     self.price = sum(self.recent_price) / len(self.recent_price)

    def high_check(self, callback=None):
        if (
            self.high_mark and self.own and not self.high_selling and
            self.price <= self.high_mark_back_price*(1+5*BACK_BEFORE_RATE) 
        ):
            logger.info(f'{self.symbol} reach high back {self.high_mark_back_price}, now price {self.price}')
            self.high_selling = True
            return True

        elif not self.high_mark and self.price >= self.high_mark_price:
            self.high_mark = True
            logger.info(f'{self.symbol} reach high mark {self.high_mark_price}, now price {self.price}, back price {self.high_mark_back_price}')
            if callback:
                try:
                    callback(self)
                except Exception as e:
                    logger.error(e)

        return False

    def low_check(self, callback=None):
        if (
            self.low_mark and self.own and not self.low_selling and
            self.price <= self.low_mark_back_price*(1+2*BACK_BEFORE_RATE) 
        ):
            self.low_selling = True
            logger.info(f'{self.symbol} reach low back {self.low_mark_back_price}, now price {self.price}')
            return True

        elif not self.low_mark and self.price >= self.low_mark_price:
            self.low_mark = True
            logger.info(f'{self.symbol} reach low mark {self.low_mark_price}, now price {self.price}, back price {self.low_mark_back_price}')
            if callback:
                try:
                    callback(self)
                except Exception as e:
                    logger.error(e)

        return False
