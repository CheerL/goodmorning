import time
import math

class ListenKey:
    def __init__(self, api):
        self.api = api
        self.key = ''
        self.create_time = self.update_time = 0

    def update(self):
        self.api.renew_listen_key(self.key)
        self.update_time = time.time()

    def recreate(self):
        if self.key:
            self.api.close_listen_key(self.key)
        self.key = self.api.new_listen_key()['listenKey']
        self.update_time = time.time()
        self.create_time = self.update_time

    def check(self, user=None):
        now = time.time()
        if now > self.create_time + 12 * 60 * 60:
            self.recreate()
        elif now > self.update_time + 30 * 60:
            self.update()

        if not user:
            return

        for conn_name in user.websocket._conns.copy():
            if len(conn_name) > 50:
                if conn_name != self.key:
                    user.websocket.stop_socket(conn_name)
                else:
                    factory = user.websocket.factories[conn_name]
                    factory.protocol_instance.sendPong()

        if self.key not in user.websocket._conns:
            user.websocket.user_data(self.key, 1, user.user_data_callback)

class Symbol:
    def __init__(self, info):
        self.base_currency = info['baseAsset']
        self.quote_currency = info['quoteAsset']
        self.symbol = info['symbol']
        self.state = info['status']
        self.sell_market_min_order_amt = 0.0
        self.ice_part = 1

        for each in info['filters']:
            if each['filterType'] == 'PRICE_FILTER':
                self.value_precision = self.price_precision = round(-math.log10(float(each['tickSize'])))
            elif each['filterType'] == 'LOT_SIZE':
                self.limit_order_min_order_amt = self.min_order_amt = float(each['minQty'])
                self.limit_order_max_order_amt = self.max_order_amt = float(each['maxQty'])
                self.amount_precision = round(-math.log10(float(each['stepSize'])))
            elif each['filterType'] == 'MARKET_LOT_SIZE':
                self.sell_market_min_order_amt = float(each['minQty'])
                self.sell_market_max_order_amt = float(each['maxQty'])
            elif each['filterType'] == 'MIN_NOTIONAL':
                self.min_order_value = float(each['minNotional'])
            elif each['filterType'] == 'ICEBERG_PARTS':
                self.ice_part = int(each['limit'])

        min_amount_step = round(0.1**self.amount_precision, self.amount_precision)
        self.sell_market_min_order_amt = max(self.sell_market_min_order_amt, min_amount_step)

class Candlestick:
    def __init__(self, kline):
        self.id = int(kline[0] / 1000)
        #self.timestamp = 0
        self.open = float(kline[1])
        self.high = float(kline[2])
        self.low = float(kline[3])
        self.close = float(kline[4])
        self.amount = float(kline[5])
        self.count = kline[8]
        self.vol = float(kline[7])


class OrderDetail:
    def __init__(self, order):
        self.id = order['orderId']
        self.symbol = order['symbol']
        self.account_id = 0
        self.amount = float(order['origQty'])
        self.price = float(order['price'])
        self.created_at = order['time']
        self.canceled_at = order['updateTime'] if order['status'] == 'CANCELED' else 0
        self.finished_at = order['updateTime'] 
        self.type = f"{order['side'].lower()}-{order['type'].lower()}"
        self.filled_amount = float(order['executedQty'])
        self.filled_cash_amount = float(order['cummulativeQuoteQty'])
        self.filled_fees = self.filled_cash_amount * 0.001
        self.source = ''
        self.state = order['status']
        self.client_order_id = order['clientOrderId']
        self.stop_price = float(order['stopPrice'])
        self.next_time = 0
        self.operator=""


class Ticker:
    def __init__(self, ticker):
        if 'price' in ticker:
            self.amount = 0
            self.count = 0
            self.open = 0
            self.close = float(ticker['price'])
            self.low = 0
            self.high = 0
            self.vol = 0
            self.symbol = ticker['symbol']
            self.bid = 0
            self.bidSize = 0
            self.ask = 0
            self.askSize = 0
        else:
            self.amount = float(ticker['volume'])
            self.count = ticker['count']
            self.open = float(ticker['openPrice'])
            self.close = float(ticker['lastPrice'])
            self.low = float(ticker['lowPrice'])
            self.high = float(ticker['highPrice'])
            self.vol = float(ticker['quoteVolume'])
            self.symbol = ticker['symbol']
            self.bid = float(ticker['bidPrice'])
            self.bidSize = 0
            self.ask = float(ticker['askPrice'])
            self.askSize = 0


class OrderUpdate:
    def __init__(self, update):
        self.orderId = update['i']
        self.tradePrice = float(update['L'])
        self.tradeVolume = float(update['Y'])
        self.tradeId = update['t']
        self.tradeTime = update['T']
        self.aggressor = False
        self.remainAmt = float(update['q']) - float(update['z'])
        self.orderStatus = update['X']
        self.clientOrderId = update['c']
        self.eventType = update['x']
        self.symbol = update['s']
        self.type = f"{update['S'].lower()}-{update['o'].lower()}"
        self.accountId = 0
