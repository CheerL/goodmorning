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

    @classmethod
    def str(num):
        return OrderSummaryStatus.str_dict[num]

class OrderSummary:
    def __init__(self, symbol, direction):
        self.order_id = None
        self.symbol = symbol
        self.direction = direction
        self.limit = True
        self.created_price = 0
        self.created_amount = 0
        self.created_vol = 0
        self.aver_price = 0
        self.amount = 0
        self.vol = 0
        self.remain_amount = 0
        self.fee = 0
        self.orders: list[OrderUpdate] = []
        self.status = 0
        self.error_msg = ''
        self.filled_callback = None
        self.filled_callback_args = []
        self.cancel_callback = None
        self.cancel_callback_args = []

    def report(self):
        logger.info(f'{self.order_id}: {self.direction}-{"limit" if self.limit else "market"} {OrderSummaryStatus.str(self.status)} | amount: {self.amount} vol: {self.vol} price: {self.aver_price} remain: {self.remain_amount} | created amount: {self.created_amount} vol: {self.created_vol} price: {self.created_price}| {self.error_msg}')

    def create(self, data: OrderUpdate):
        self.orders.append(data)
        self.order_id = data.orderId
        if 'market' in data.type:
            self.limit = False
        self.status = OrderSummaryStatus.CREATED
        self.report()

    def update(self, data: OrderUpdate):
        new_price = float(data.tradePrice)
        new_amount = float(data.tradeVolume)
        new_vol = new_price * new_amount
        self.amount += new_amount
        self.vol += new_vol
        self.aver_price = self.vol / self.amount
        self.fee = self.vol * 0.002
        if 'partial-filled' == data.orderStatus:
            self.status = OrderSummaryStatus.PARTIAL_FILLED
            self.remain_amount = float(data.remainAmt)
        elif 'filled' == data.orderStatus:
            self.status = OrderSummaryStatus.FILLED
            self.remain_amount = 0
        self.report()

    def cancel_update(self, data: OrderUpdate):
        self.status = OrderSummaryStatus.CANCELED
        self.remain_amount = float(data.remainAmt)
        self.report()

    def finish(self):
        pass

    def error(self, e):
        self.status = OrderSummaryStatus.FAILED
        self.error_msg = e
        self.report()
        
    def check_after_buy(self, client):
        def wrapper():
            if self.status in [OrderSummaryStatus.FAILED, OrderSummaryStatus.EMPTY]:
                client.after_buy(self.symbol, 0)
            elif self.status in [OrderSummaryStatus.CREATED, OrderSummaryStatus.PARTIAL_FILLED]:
                client.user.trade_client.cancel_order(self.symbol, self.order_id)
        
        Timer(1, wrapper).start()

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
