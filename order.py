from huobi.model.trade.order_update import OrderUpdate
from threading import Timer
from utils import logger

class OrderSummaryStatus:
    FAILED = -1
    EMPTY = 0
    CREATED = 1
    PARTIAL_FILLED = 2
    FILLED = 3
    CANCELED = 4

    str_dict = {
        -1: 'FAILED',
        0: 'EMPTY',
        1: 'CREATED',
        2: 'PARTIAL_FILLED',
        3: 'FILLED',
        4: 'CANCELED'
    }

    @staticmethod
    def str(num):
        return OrderSummaryStatus.str_dict[num]

class OrderSummary:
    def __init__(self, order_id, symbol, direction, label=''):
        self.order_id = order_id
        self.symbol = symbol
        self.direction = direction
        self.label = label
        self.limit = True
        self.created_price = 0
        self.created_amount = 0
        self.created_vol = 0
        self.aver_price = 0
        self.amount = 0
        self.vol = 0
        self.remain = 0
        self.fee = 0
        self.error_msg = ''
        self.status = 0
        self.created_ts = 0
        self.ts = 0
        self.filled_callback = None
        self.filled_callback_args = []
        self.cancel_callback = None
        self.cancel_callback_args = []

    def report(self):
        logger.info(f'{self.order_id}: {self.symbol} : {self.direction}-{"limit" if self.limit else "market"} {OrderSummaryStatus.str(self.status)} | amount: {self.amount} vol: {self.vol} price: {self.aver_price} remain: {self.remain} | created amount: {self.created_amount} vol: {self.created_vol} price: {self.created_price}| {self.error_msg}')

    def create(self, data):
        if isinstance(data, OrderUpdate) and 'market' in data.type:
            self.limit = False
            self.created_ts = self.ts = data.tradeTime / 1000
        elif isinstance(data, dict) and data['from'] == 'binance' and data['o'] == 'MARKET':
            self.limit = False
            self.created_ts = self.ts = data['O'] / 1000
            
        self.status = OrderSummaryStatus.CREATED
        self.report()

    def update(self, data, fee_rate):
        if isinstance(data, OrderUpdate):
            new_price = float(data.tradePrice)
            new_amount = float(data.tradeVolume)
            new_vol = new_price * new_amount
            self.ts = data.tradeTime / 1000
            self.amount += new_amount
            self.vol += new_vol
            self.fee = self.vol * fee_rate
            self.aver_price = self.vol / self.amount if self.amount else 0
            if 'partial-filled' == data.orderStatus:
                self.status = OrderSummaryStatus.PARTIAL_FILLED
                self.remain = float(data.remainAmt)
            elif 'filled' == data.orderStatus:
                self.status = OrderSummaryStatus.FILLED
                self.remain = 0
                if self.filled_callback:
                    self.filled_callback(*self.filled_callback_args)
        elif isinstance(data, dict) and data['from'] == 'binance':
            self.amount = float(data['z'])
            self.vol = float(data['Z'])
            self.fee = self.vol * 0.001
            self.aver_price = self.vol / self.amount if self.amount else 0
            self.ts = data['T'] / 1000

            if 'PARTIALLY_FILLED' == data['X']:
                self.status = OrderSummaryStatus.PARTIAL_FILLED
                self.remain = self.created_amount - self.amount
            elif 'FILLED' == data['X']:
                self.status = OrderSummaryStatus.FILLED
                self.remain = 0
                if self.filled_callback:
                    self.filled_callback(*self.filled_callback_args)

        self.report()

    def cancel_update(self, data):
        self.status = OrderSummaryStatus.CANCELED
        if isinstance(data, OrderUpdate):
            self.remain = float(data.remainAmt)
        elif isinstance(data, dict) and data['from'] == 'binance':
            self.remain = float(data['q']) - float(data['z'])

        if self.cancel_callback:
            self.cancel_callback(*self.cancel_callback_args)
        self.report()

    def finish(self):
        pass

    def error(self, e):
        self.status = OrderSummaryStatus.FAILED
        self.error_msg = e
        self.order_id = -1
        self.report()
        
    def check_after_buy(self, client, wait=1.5):
        def wrapper():
            if self.status in [OrderSummaryStatus.FAILED, OrderSummaryStatus.EMPTY]:
                client.after_buy(self.symbol, 0)
            elif self.status in [OrderSummaryStatus.CREATED, OrderSummaryStatus.PARTIAL_FILLED]:
                client.user.trade_client.cancel_order(self.symbol, self.order_id)
        
        Timer(wait, wrapper).start()

    def add_filled_callback(self, callback, args=[]):
        self.filled_callback = callback
        self.filled_callback_args = args
        if self.status == OrderSummaryStatus.FILLED:
            self.filled_callback(*args)

    def add_cancel_callback(self, callback, args=[]):
        self.cancel_callback = callback
        self.cancel_callback_args = args
        if self.status == OrderSummaryStatus.CANCELED:
            self.cancel_callback(*args)
