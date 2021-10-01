# from huobi.client.account import AccountClient
# from huobi.client.trade import TradeClient
# from huobi.constant import OrderSource, OrderType, AccountBalanceMode
# from huobi.model.account.account_update_event import AccountUpdateEvent, AccountUpdate
# from huobi.model.trade.order_update_event import OrderUpdateEvent, OrderUpdate
from utils import logger, timeout_handle
from binance.spot import Spot
from binance.websocket.spot.websocket_client import SpotWebsocketClient
from retry import retry
from target import BaseTarget as Target
from order import OrderSummary
from user import BaseUser, BaseMarketClient

import re
import time
import math
# AccountBalanceMode.TOTAL = '2'

class ListenKey:
    def __init__(self, api: Spot):
        self.api = api
        self.key = self.api.new_listen_key()['listenKey']
        self.update_time = time.time()
        self.create_time = self.update_time

    def update(self):
        self.key = self.api.renew_listen_key(self.key)
        self.update_time = time.time()

    def recreate(self):
        self.api.close_listen_key(self.key)
        self.key = self.api.new_listen_key()['listenKey']
        self.update_time = time.time()
        self.create_time = self.update_time

    def check(self):
        now = time.time()
        if now > self.create_time + 12 * 60 * 60:
            self.recreate()
        elif now > self.update_time + 30 * 60:
            self.update()

class Symbol:
    def __init__(self, info):
        self.base_currency = info['baseAsset']
        self.quote_currency = info['quoteAsset']
        self.symbol = info['symbol']
        self.state = info['status']

        for each in info['filters']:
            if each['filterType'] == 'PRICE_FILTER':
                # print(each)
                self.value_precision = self.price_precision = len(str(int(1/float(each['tickSize'])))) - 1
            elif each['filterType'] == 'LOT_SIZE':
                self.limit_order_min_order_amt = self.min_order_amt = float(each['minQty'])
                self.limit_order_max_order_amt = self.max_order_amt = float(each['maxQty'])
                self.amount_precision = len(str(int(1/float(each['stepSize'])))) - 1
            elif each['filterType'] == 'MARKET_LOT_SIZE':
                self.sell_market_min_order_amt = float(each['minQty'])
                self.sell_market_max_order_amt = float(each['maxQty'])
            elif each['filterType'] == 'MIN_NOTIONAL':
                self.min_order_value = float(each['minNotional'])


class BinanceMarketClient(BaseMarketClient):
    exclude_list = []

    def __init__(self, api=None, **kwargs):
        super().__init__(**kwargs)
        if api:
            self.api: Spot = api
            self.update_symbols_info()

    def get_all_symbols_info(self):
        return {
            info['symbol']: Symbol(info)
            for info in self.api.exchange_info()['symbols']
            if info['symbol'].endswith('USDT')
            and not re.search('\d', info['symbol'])
            and info['symbol'] not in []
        }

    def get_market_tickers(self, symbol=None, all_info=False):
        if all_info:
            return self.api.ticker_24hr(symbol)
        return self.api.ticker_price(symbol)

    def get_candlestick(self, symbol, interval: str, limit=10):
        if interval.endswith('day'):
            interval = interval.replace('day', 'd')
        elif interval.endswith('min'):
            interval = interval.replace('min', 'm')
        elif interval.endswith('hour'):
            interval = interval.replace('hour', 'h')
        elif interval.endswith('week'):
            interval = interval.replace('week', 'w')
        elif interval.endswith('mon'):
            interval = interval.replace('mon', 'M')
        return self.api.klines(symbol, interval, limit=limit)

    @timeout_handle({})
    def get_price(self) -> 'dict[str, float]':
        return {
            pair['symbol']: float(pair['price'])
            for pair in self.get_market_tickers()
        }

    @timeout_handle({})
    def get_vol(self) -> 'dict[str, float]':
        return {
            pair['symbol']: float(pair['quoteVolume'])
            for pair in self.get_market_tickers(all_info=True)
        }


class BinanceUser(BaseUser):
    user_type = 'Binance'
    MarketClient = BinanceMarketClient

    def __init__(self, access_key, secret_key, buy_amount, wxuid):
        self.api = Spot(key=access_key, secret=secret_key)
        super().__init__(access_key, secret_key, buy_amount, wxuid)
        self.market_client.api = self.api
        self.market_client.update_symbols_info()
        self.listen_key = ListenKey(self.api)
        self.websocket = SpotWebsocketClient()
        self.websocket.start()
        self.api.new_order = self.api.new_order_test

    def get_account_id(self) -> int:
        return 0

    def get_order(self, order_id):
        symbol = self.orders[order_id].symbol
        return self.api.get_order(symbol, orderId=order_id)

    def cancel_order(self, order_id):
        symbol = self.orders[order_id].symbol
        self.api.cancel_order(symbol, orderId=order_id)

    def start(self, **kwargs):
        self.listen_key.check()
        self.websocket.user_data(self.listen_key.key, 1, self.user_data_callback)

        for coin_info in self.api.coin_info():
            currency = coin_info['coin']
            self.available_balance[currency] = float(coin_info['free'])
            self.balance[currency] = float(coin_info['freeze']) + float(coin_info['locked']) + self.available_balance[currency]
            self.balance_update_time[currency] = 0

        usdt = self.balance['USDT']
        if isinstance(self.buy_amount, str) and self.buy_amount.startswith('/'):
            self.buy_amount = max(math.floor(usdt / float(self.buy_amount[1:])), 5)
        else:
            self.buy_amount = float(self.buy_amount)

    def user_data_callback(self, update):
        try:
            print(update)
            if 'e' not in update:
                return

            if update['e'] == 'outboundAccountPosition':
                self.balance_callback(update)
            elif update['e'] == 'executionReport':
                self.trade_callback(update)
        except Exception as e:
            self.error_callback('user')(e)

    def balance_callback(self, update):
        change_time = update['E']

        for sub_update in update['B']:
            currency = sub_update['a']
            if currency not in self.balance or change_time > self.balance_update_time[currency]:
                self.available_balance[currency] = float(sub_update['f'])
                self.balance[currency] = float(sub_update['f'])+float(sub_update['l'])
                self.balance_update_time[currency] = change_time

    def trade_callback(self, update):
        @retry(tries=3, delay=0.01)
        def _warpper(update):
            symbol = update['s']
            direction = update['S']
            etype = update['x']
            order_id = update['i']

            if order_id in self.orders:
                summary = self.orders[order_id]
            else:
                summary = OrderSummary(order_id, symbol, direction)
                self.orders[order_id] = summary

            try:
                if etype == 'NEW':
                    summary.create(update)

                elif etype == 'TRADE':
                    summary.update(update)

                elif etype == 'CANCELED':
                    summary.cancel_update(update)

            except Exception as e:
                if not isinstance(e, KeyError):
                    logger.error(f"{direction} {etype} | Error: {type(e)} {e}")
                raise e

        try:
            update['from'] = 'binance'
            _warpper(update)
        except Exception as e:
            if not isinstance(e, KeyError):
                logger.error(f"max tries | {type(e)} {e}")

    def error_callback(self, prefix):
        def warpper(error):
            logger.error(f'[{prefix}] {error}')
        return warpper

    def buy(self, target: Target, vol):
        symbol = target.symbol
        vol = target.check_amount(max(
            vol,
            target.min_order_value
        ))
        now = int(time.time())
        order = dict(
            symbol=symbol,
            side='BUY',
            type='MARKET',
            quoteOrderQty=vol,
            timestamp=now,
            newOrderRespType='ACK'
        )
        try:
            response = self.api.new_order(**order)
            order_id = response['orderId']
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'BUY')
                self.orders[order_id] = order_summary

            order_summary.created_vol = vol
            order_summary.remain = vol
            self.buy_id.append(order_id)
            
            logger.debug(f'Speed {vol} USDT to buy {target.symbol[:-4]}')
            return order_summary
        except Exception as e:
            logger.error(e)
            # if order_id in self.orders:
            #     del self.orders[order_id]
            return None

    def buy_limit(self, target: Target, vol, price=None):
        if not price:
            price = target.get_target_buy_price()
        else:
            price = target.check_price(price)
        symbol = target.symbol
        amount = target.check_amount(max(
            vol / price,
            target.limit_order_min_order_amt
        ))
        now = int(time.time())
        order = dict(
            symbol=symbol,
            side='BUY',
            type='LIMIT',
            timeInForce='GTC',
            quantity=amount,
            price=price,
            timestamp=now,
            newOrderRespType='ACK'
        )
        # order_id = -1

        try:
            response = self.api.new_order(**order)
            order_id = response['orderId']
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'buy')
                self.orders[order_id] = order_summary

            order_summary.created_amount = amount
            order_summary.created_price = price
            order_summary.remain = amount
            self.buy_id.append(order_id)
            
            logger.debug(f'Buy {vol} {symbol[:-4]}')
            return order_summary
        except Exception as e:
            logger.error(e)
            # if order_id in self.orders:
            #     del self.orders[order_id]
            return None

    def sell(self, target: Target, amount):
        symbol = target.symbol
        amount = target.check_amount(max(
            amount,
            target.sell_market_min_order_amt
        ))
        now = int(time.time())
        order = dict(
            symbol=symbol,
            side='SELL',
            type='MARKET',
            quantity=amount,
            timestamp=now,
            newOrderRespType='ACK'
        )

        try:
            response = self.api.new_order(**order)
            order_id = response['orderId']
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'sell')
                self.orders[order_id] = order_summary

            order_summary.created_amount = amount
            order_summary.remain = amount
            self.sell_id.append(order_id)
            
            logger.debug(f'Sell {amount} {symbol[:-4]} with market price')
            return order_summary
        except Exception as e:
            # order_summary.error(e)
            logger.error(e)
            raise Exception(e)

    def sell_limit(self, target: Target, amount, price, ioc=False):
        price = target.check_price(price)
        symbol = target.symbol
        amount = target.check_amount(max(
            amount,
            target.limit_order_min_order_amt
        ))
        now = int(time.time())
        order = dict(
            symbol=symbol,
            side='SELL',
            type='LIMIT',
            timeInForce='GTC',
            quantity=amount,
            price=price,
            timestamp=now,
            newOrderRespType='ACK'
        )

        try:
            response = self.api.new_order(**order)
            order_id = response['orderId']
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'sell')
                self.orders[order_id] = order_summary

            order_summary.created_amount = amount
            order_summary.created_price = price
            order_summary.remain = amount
            self.sell_id.append(order_id)
            logger.debug(f'Sell {amount} {symbol[:-4]} with price {price}')
            return order_summary
        except Exception as e:
            logger.error(e)
            raise Exception(e)
