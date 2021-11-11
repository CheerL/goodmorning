from huobi.model.trade.order_update import OrderUpdate
from threading import Timer
from utils import logger, config

CANCEL_BUY_TIME = config.getfloat('time', 'CANCEL_BUY_TIME')

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

    map_dict = {
        'submitted': 1,
        'partial-filled': 2,
        'partial-canceled': 4,
        'filled': 3,
        'canceled': 4
    }

    @staticmethod
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
        self.ts = 0
        self.orders: list[OrderUpdate] = []
        self.status = 0
        self.error_msg = ''
        self.filled_callback = None
        self.filled_callback_args = []
        self.cancel_callback = None
        self.cancel_callback_args = []

    def __repr__(self) -> str:
        return f'<OrderSummary id={self.order_id}, symbol={self.symbol}, status={OrderSummaryStatus.str_dict[self.status]}, direction={self.direction}, limit={self.limit}, amount={self.amount}, price={self.aver_price}, ts={self.ts}>'

    def report(self):
        logger.info(f'{self.order_id}: {self.symbol} : {self.direction}-{"limit" if self.limit else "market"} {OrderSummaryStatus.str(self.status)} | amount: {self.amount} vol: {self.vol} price: {self.aver_price} remain: {self.remain_amount} | created amount: {self.created_amount} vol: {self.created_vol} price: {self.created_price}| {self.error_msg}')

    def create(self, data: OrderUpdate):
        self.orders.append(data)
        self.order_id = data.orderId
        if 'market' in data.type:
            self.limit = False
        self.status = OrderSummaryStatus.CREATED
        self.ts = max(data.tradeTime / 1000, self.ts)
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
        self.ts = max(data.tradeTime / 1000, self.ts)
        self.report()

    def cancel_update(self, data: OrderUpdate):
        self.status = OrderSummaryStatus.CANCELED
        self.remain_amount = float(data.remainAmt)
        self.ts = max(data.tradeTime / 1000, self.ts)
        self.report()

    def finish(self):
        pass

    def error(self, e):
        self.status = OrderSummaryStatus.FAILED
        self.error_msg = e
        self.order_id = -1
        self.report()
        
    def check_cancel(self, client):
        def wrapper():
            if self.status not in [OrderSummaryStatus.FILLED, OrderSummaryStatus.CANCELED]:
                if self.order_id:
                    try:
                        client.user.trade_client.cancel_order(self.symbol, self.order_id)
                    except Exception as e:
                        logger.error(e)
                        client.after_buy(self.symbol, 0)
                else:
                    client.after_buy(self.symbol, 0)
        
        Timer(CANCEL_BUY_TIME, wrapper).start()

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
