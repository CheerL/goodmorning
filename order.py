from huobi.model.trade.order_update import OrderUpdate
from threading import Timer

class OrderSummaryStatus:
    FAILED = -1
    EMPTY = 0
    CREATED = 1
    PARTIAL_FILLED = 2
    FILLED = 3
    CANCELED = 4

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

    def create(self, data: OrderUpdate):
        self.orders.append(data)
        self.order_id = data.orderId
        if 'market' in data.type:
            self.limit = False
        self.status = OrderSummaryStatus.CREATED

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

    def cancel_update(self, data: OrderUpdate):
        self.status = OrderSummaryStatus.CANCELED
        self.remain_amount = float(data.remainAmt)

    def finish(self):
        pass

    def error(self, e):
        self.status = OrderSummaryStatus.FAILED
        self.error_msg = e
        
    def check_after_buy(self, client):
        def wrapper():
            if self.status in [OrderSummaryStatus.FAILED, OrderSummaryStatus.EMPTY]:
                client.after_buy(self.symbol, 0)
            elif self.status in [OrderSummaryStatus.CREATED, OrderSummaryStatus.PARTIAL_FILLED]:
                client.user.trade_client.cancel_order(self.symbol, self.order_id)
        
        Timer(1, wrapper)

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
