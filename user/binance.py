from utils import logger, user_config, datetime
from binance.spot import Spot
# from apscheduler.schedulers.background import BackgroundScheduler as Scheduler
from apscheduler.schedulers.gevent import GeventScheduler as Scheduler
from binance.websocket.spot.websocket_client import SpotWebsocketClient
from retry import retry
from target import BaseTarget as Target
from order import OrderSummary
from user import BaseUser, BaseMarketClient
from user.binance_model import ListenKey, Candlestick, Symbol, OrderDetail, Ticker

import re
import time
import math


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
            and info['status'] == 'TRADING'
            and not 'DOWN' in info['symbol']
            and not re.search('\d', info['symbol'])
            and info['symbol'] not in []
        }

    def get_market_tickers(self, symbol=None, all_info=False):
        if all_info:
            raw_tickers = self.api.ticker_24hr(symbol)
        else:
            raw_tickers = self.api.ticker_price(symbol)
        return [Ticker(raw_ticker) for raw_ticker in raw_tickers]

    def get_candlestick(self, symbol, interval: str, limit=10):
        if interval.endswith('day'):
            interval = interval.replace('day', 'd')
        elif interval.endswith('min'):
            if interval == '60min':
                interval = '1h'
            else:
                interval = interval.replace('min', 'm')
        elif interval.endswith('hour'):
            interval = interval.replace('hour', 'h')
        elif interval.endswith('week'):
            interval = interval.replace('week', 'w')
        elif interval.endswith('mon'):
            interval = interval.replace('mon', 'M')
        raw_klines = self.api.klines(symbol, interval, limit=limit)
        return [Candlestick(kline) for kline in reversed(raw_klines)]


class BinanceUser(BaseUser):
    user_type = 'Binance'
    MarketClient = BinanceMarketClient
    min_usdt_amount = 11
    fee_rate = 0.001

    def __init__(self, access_key, secret_key, buy_amount, wxuid):
        datetime.Tz.tz_num = 0
        self.api = Spot(key=access_key, secret=secret_key)
        super().__init__(access_key, secret_key, buy_amount, wxuid)
        self.market_client.api = self.api
        self.market_client.update_symbols_info()
        self.listen_key = ListenKey(self.api)
        self.websocket = SpotWebsocketClient()
        # self.api.new_order = self.api.new_order_test
        self.scheduler = Scheduler(job_defaults={'max_instances': 5}, timezone=datetime.Tz.get_tz())
        self.scheduler.add_job(self.listen_key.check, 'interval', minutes=5)
        self.scheduler.add_job(self.api.ping, 'interval', seconds=5)
        self.scheduler.start()
        self.websocket.start()

    @classmethod
    def init_users(cls, num=-1):
        users = super().init_users(num=num)
        ACCOUNT_ID = user_config.get('setting', f'BinanceID')
        TEST = user_config.getboolean('setting', 'Test')

        ids = [int(id.strip()) for id in ACCOUNT_ID.split(',')]
        if num == -1 and TEST:
            ids = ids[:1]
        else:
            ids = [ids[num]]

        for id, user in zip(ids, users):
            user.account_id = id
        return users

    def get_account_id(self) -> int:
        return -1

    def get_order(self, order_id):
        symbol = self.orders[order_id].symbol
        raw_order = self.api.get_order(symbol, orderId=order_id)
        return OrderDetail(raw_order)

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
                    summary.update(update, self.fee_rate)

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
