import os
import pickle
import numpy as np
import multiprocessing

# from user.huobi import HuobiUser
from utils import get_rate, datetime

class Global:
    user = None
    num =  multiprocessing.Value('i', 0)
    # num = 0
    # manager = multiprocessing.Manager()
    sell_dict = dict()
    # cont_loss_list_dict = manager.dict()
    # base_klines_list_dict = manager.dict()
    # klines_dict = manager.dict()
    # sell_dict = {}

    @classmethod
    def add_num(cls):
        cls.num.value += 1
        # cls.num += 1

    @classmethod
    def show(cls, num):
        print(f'{cls.num.value}/{num}, {cls.num.value/num:.3%}')
        # print(f'{cls.num}/{num}, {cls.num/num:.3%}')

class Record:
    def __init__(self, symbol, buy_price, buy_time, buy_vol, fee_rate = 0.001):
        self.fee_rate = fee_rate
        self.symbol = symbol
        self.buy_price = buy_price
        self.buy_time = buy_time
        self.buy_vol = buy_vol
        self.amount = self.buy_vol / buy_price * (1 - self.fee_rate)
        self.sell_price = 0
        self.sell_time = 0
        self.sell_vol = 0
        self.profit = 0
        self.rate = 0
        self.fee = 0

    def sell(self, sell_price, sell_time):
        if self.sell_time == 0:
            self.sell_price = sell_price
            self.sell_time = sell_time
            self.sell_vol = self.amount * sell_price * (1 - self.fee_rate)
            self.fee = (self.buy_vol + self.amount * sell_price) * self.fee_rate
            self.profit = self.sell_vol - self.buy_vol
            self.rate = self.profit / self.buy_vol

    def to_csv(self):
        items = [
            self.symbol,
            datetime.ts2time(self.buy_time),
            self.buy_price,
            self.buy_vol,
            datetime.ts2time(self.sell_time),
            self.sell_price,
            self.sell_vol,
            self.profit,
            self.rate,
            self.fee,
            (self.sell_time - self.buy_time) // 86400
        ]
        csv_line = ','.join([str(e) for e in items])
        return csv_line + '\n'

    @staticmethod
    def write_csv(record_list, path):
        title = '币种,买入时间,买入价格,买入额,卖出时间,卖出价格,卖出额,收益,收益率,手续费,持有天数\n'
        with open(path, 'w+') as f:
            f.write(title)
            for record in record_list:
                f.write(record.to_csv())

class NumpyData:
    dtype = np.dtype([
        ('null', 'b')
    ])

    def __init__(self):
        self.data = np.array([], dtype=self.dtype)

    def load_from_raw(self, *args, **kwargs):
        pass

    @classmethod
    def load_from_pkl(self, filename):
        pass

    @classmethod
    def load(cls, filename):
        if os.path.exists(filename):
            self = cls()
            self.data = np.load(filename, allow_pickle=False)

            return self

    def save(self, filename):
        if self.data.size:
            np.save(filename, arr=self.data)

    @classmethod
    def trans_all(cls, from_dir, to_dir, filter_func=None):
        if not filter_func:
            filter_func = lambda _: True
        
        for filename in os.listdir(from_dir):
            if filter_func(filename):
                data = cls.load_from_pkl(os.path.join(from_dir, filename))
                data.save(os.path.join(to_dir, filename.replace('pkl', 'npy')))

class Klines(NumpyData):
    dtype = np.dtype([
        ('symbol', 'S30'),
        ('id', 'i8'),
        ('open', 'f4'),
        ('close', 'f4'),
        ('high', 'f4'),
        ('low', 'f4'),
        ('vol', 'f4')
    ])

    def load_from_raw(self, symbol, klines):
        temp_list = [(
            symbol, kline.id, kline.open, kline.close,
            kline.high, kline.low, kline.vol
        ) for kline in klines]
        self.data = np.concatenate([self.data, np.array(temp_list, dtype=self.dtype)])
        self.data.sort(order='id')

    @classmethod
    def load_from_pkl(cls, filename):
        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                klines = pickle.load(f)

            symbol = filename.split('/')[-1].split('_')[0]
            self = cls()
            self.load_from_raw(symbol, klines)
            return self

class BaseKlineDict(NumpyData):
    dtype = np.dtype([
        ('symbol', 'S30'),
        ('id', 'i8'),
        ('open', 'f4'),
        ('close', 'f4'),
        ('high', 'f4'),
        ('low', 'f4'),
        ('vol', 'f4')
    ])

    def __init__(self):
        super().__init__()
        self.dict = {}

    def load_from_raw(self, symbol, klines):
        temp_list = [(
            symbol, kline.id, kline.open, kline.close,
            kline.high, kline.low, kline.vol
        ) for kline in klines]
        data = np.array(temp_list, dtype=self.dtype)
        self.data = np.concatenate([self.data, data])
        self.dict[symbol] = self.data[self.data['symbol']==symbol.encode()]

    @classmethod
    def load_from_pkl(cls, filename):
        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                base_klines_dict = pickle.load(f)

        self = cls()
        for symbol, klines in base_klines_dict.items():
            self.load_from_raw(symbol, klines)
        return self

    @classmethod
    def load(cls, filename):
        self = super().load(filename)
        
        for symbol in np.unique(self.data['symbol']):
            self.dict[symbol.decode()] = self.data[self.data['symbol']==symbol]

        return self

class ContLossList(NumpyData):
    dtype = np.dtype([
        ('symbol', 'S30'),
        ('id', 'i8'),
        ('open', 'f4'),
        ('close', 'f4'),
        ('high', 'f4'),
        ('low', 'f4'),
        ('vol', 'f4'),
        ('id2', 'i8'),
        ('open2', 'f4'),
        ('close2', 'f4'),
        ('high2', 'f4'),
        ('low2', 'f4'),
        ('vol2', 'f4'),
        ('date', 'S20'),
        ('rate', 'f4'),
        ('cont_loss_days', 'i2'),
        ('cont_loss_rate', 'f4'),
        ('is_max_loss', 'b'),
        ('boll', 'f4'),
        ('bollup', 'f4'),
        ('bolldown', 'f4'),
    ])

    def load_from_raw(self, cont_loss_list: 'list[ContLoss]'):
        temp_list = []
        for cont_loss in cont_loss_list:
            temp_item = [
                cont_loss.symbol,  cont_loss.date, cont_loss.kline.id,
                cont_loss.kline.open, cont_loss.kline.close, 
                cont_loss.kline.high, cont_loss.kline.low, cont_loss.kline.vol, 
                cont_loss.rate, cont_loss.cont_loss_days, cont_loss.cont_loss_rate,
                cont_loss.is_big_loss, cont_loss.is_max_loss, 
                cont_loss.boll, cont_loss.bollup, cont_loss.bolldown,
                0,0,0,0,0,0,
            ]
            try:
                kline2 = cont_loss.more_klines[0]
                temp_item[-6:] = [
                    kline2.id,kline2.open,kline2.close,
                    kline2.high,kline2.low,kline2.vol
                ]
            except IndexError:
                pass
            temp_list.append((*temp_item,))

        self.data = np.concatenate([self.data, np.array(temp_list, dtype=self.dtype)])

    @classmethod
    def load_from_pkl(cls, filename):
        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                cont_loss_list = pickle.load(f)

        self = cls()
        self.load_from_raw(cont_loss_list)
        return self

class ContLoss:
    def __init__(self, symbol, kline, rate, cont_loss_days, cont_loss_rate, is_big_loss, is_max_loss):
        self.symbol = symbol
        self.kline = kline
        self.date = datetime.ts2date(kline.id)
        self.rate = rate
        self.cont_loss_days = cont_loss_days
        self.cont_loss_rate = cont_loss_rate
        self.is_big_loss = is_big_loss
        self.is_max_loss = is_max_loss
        self.close_back_kline = None
        self.close_back_profit = 0
        self.close_back_days = 0
        self.high_back_kline = None
        self.high_back_profit = 0
        self.high_back_days = 0
        self.more_klines = []
        self.boll = 0
        self.bollup = 0
        self.bolldown = 0

    def add_close_back(self, kline):
        if not self.close_back_kline:
            self.close_back_kline = kline
            self.close_back_profit = get_rate(kline.close, self.kline.close)
            self.close_back_days = (kline.id - self.kline.id) // 86400

    def add_high_back(self, kline):
        if not self.high_back_kline:
            self.high_back_kline = kline
            self.high_back_profit = get_rate(kline.high, self.kline.close)
            self.high_back_days = (kline.id - self.kline.id) // 86400

    def add_more(self, klines):
        if isinstance(klines, list):
            self.more_klines.extend(klines)
        else:
            self.more_klines.append(klines)

    def get_more_kline(self, num, item):
        if len(self.more_klines) > num:
            return self.more_klines[num].__dict__[item]
        else:
            return 0

    def to_csv(self):
        def get_more_kline_rate(num, item):
            price = self.get_more_kline(num, item)
            return get_rate(price, self.kline.close) if price else 0

        items = [
            self.symbol,
            self.date,
            round(self.kline.vol),
            self.kline.close,
            self.rate,
            self.cont_loss_rate,
            self.cont_loss_days,
            self.is_big_loss,
            self.is_max_loss,
            datetime.ts2date(self.close_back_kline.id) if self.close_back_kline else '',
            self.close_back_days,
            self.close_back_kline.close if self.close_back_kline else 0,
            self.close_back_profit,
            datetime.ts2date(self.high_back_kline.id) if self.high_back_kline else '',
            self.high_back_days,
            self.high_back_kline.high if self.high_back_kline else 0,
            self.high_back_profit,
            get_more_kline_rate(0, 'close'),
            get_more_kline_rate(0, 'high'),
            get_more_kline_rate(0, 'low'),
            get_more_kline_rate(1, 'close'),
            get_more_kline_rate(1, 'high'),
            get_more_kline_rate(1, 'low')
        ]
        csv_line = ','.join([str(e) for e in items])
        return csv_line + '\n'

    @staticmethod
    def write_csv(cont_loss_list, path):
        title = '币种,日期,交易额,收盘价,跌幅,累计跌幅,连跌天数,是否超过累计跌幅,是否当前最大跌幅,收盘价回本日期,收盘价回本天数,收盘价回本价,收盘价回本收益率,最高价回本日期,最高价回本天数,最高价回本价,最高价回本收益率,1日收盘涨幅,1日最高涨幅,1日最低涨幅,2日收盘涨幅,2日最高涨幅,2日最低涨幅\n'
        with open(path, 'w+') as f:
            f.write(title)
            for cont_list in cont_loss_list:
                f.write(cont_list.to_csv())