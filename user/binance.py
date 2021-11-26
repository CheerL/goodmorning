from utils import logger, user_config, datetime
from binance.spot import Spot
# from apscheduler.schedulers.background import BackgroundScheduler as Scheduler
from apscheduler.schedulers.twisted import TwistedScheduler as Scheduler
from binance.websocket.spot.websocket_client import SpotWebsocketClient
from retry import retry
from target import BaseTarget as Target
from order import OrderSummary
from user.base import BaseUser, BaseMarketClient
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

    def get_market_tickers(self, symbol=None, all_info=False, raw=False):
        if all_info:
            raw_tickers = self.api.ticker_24hr(symbol)
        else:
            raw_tickers = self.api.ticker_price(symbol)
        if raw:
            return raw_tickers
        return [Ticker(raw_ticker) for raw_ticker in raw_tickers]


    def get_candlestick(self, symbol, interval: str, limit=10, start_ts=None, end_ts=None):
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
        
        if start_ts and end_ts:
            raw_klines = []
            start_time = start_ts * 1000
            end_time = end_ts * 1000 - 1
            while True:
                klines = self.api.klines(symbol, interval, startTime=start_time, endTime=end_time, limit=1000)
                if klines:
                    raw_klines.extend(klines)
                    start_time = klines[-1][0] + 1

                    if len(klines) < 1000:
                        break
                    elif start_time > end_time:
                        break
                else:
                    break

        else:
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
        self.market.api = self.api
        self.market.update_symbols_info()
        self.listen_key = ListenKey(self.api)
        self.websocket = SpotWebsocketClient()
        self.scheduler = Scheduler(job_defaults={'max_instances': 5}, timezone=datetime.Tz.get_tz())
        

    @classmethod
    def init_users(cls, num=-1):
        users = super().init_users(num=num)
        ACCOUNT_ID = user_config.get('setting', 'BinanceID')
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

    def get_order(self, symbol, order_id):
        raw_order = self.api.get_order(symbol, orderId=order_id)
        return OrderDetail(raw_order)

    def cancel_order(self, symbol, order_id):
        self.api.cancel_order(symbol, orderId=order_id)

    def start(self, **kwargs):
        self.scheduler.start()
        self.websocket.start()
        self.scheduler.add_job(self.listen_key.check, 'interval', seconds=3, args=[self])
        self.scheduler.add_job(self.api.ping, 'interval', seconds=5)
        self.listen_key.check(self)
        self.update_currency()

        usdt = self.balance['USDT']
        if isinstance(self.buy_amount, str) and self.buy_amount.startswith('/'):
            self.buy_amount = max(math.floor(usdt / float(self.buy_amount[1:])), 5)
        else:
            self.buy_amount = float(self.buy_amount)

    def update_currency(self, currency=''):
        def _update_currency(_balance):
            _currency = _balance['coin']
            self.available_balance[_currency] = float(_balance['free'])
            self.balance[_currency] = float(_balance['freeze']) + float(_balance['locked']) + self.available_balance[_currency]
            self.balance_update_time[_currency] = now

        all_coin_info = self.api.coin_info()
        now = int(time.time())

        if currency:
            for coin_info in all_coin_info:
                if coin_info['coin'] == currency:
                    _update_currency(coin_info)
                    break
        else:
            for coin_info in all_coin_info:
                _update_currency(coin_info)

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
            if (currency not in self.balance
                or change_time > self.balance_update_time[currency]
            ):
                self.available_balance[currency] = float(sub_update['f'])
                self.balance[currency] = float(sub_update['f'])+float(sub_update['l'])
                self.balance_update_time[currency] = change_time
                # logger.info(f'{currency} update, available {self.available_balance[currency]}, total {self.balance[currency]}')

    def trade_callback(self, update):
        @retry(tries=3, delay=0.01)
        def _warpper(update):
            symbol = update['s']
            direction = update['S'].lower()
            etype = update['x']
            order_id = update['i']

            if order_id in self.orders:
                summary = self.orders[order_id]
            else:
                summary = OrderSummary(order_id, symbol, direction)
                self.orders[order_id] = summary

            summary.fee_rate = self.fee_rate
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
            logger.debug(f'Buy {vol}U {target.symbol[:-4]} with market price')
            response = self.api.new_order(**order)
            order_id = response['orderId']
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'buy')
                order_summary.created_ts = order_summary.ts = time.time() - 5
                self.orders[order_id] = order_summary

            order_summary.created_vol = vol
            order_summary.remain = vol
            self.buy_id.append(order_id)
            
            return order_summary
        except Exception as e:
            logger.error(e)
            # if order_id in self.orders:
            #     del self.orders[order_id]
            return None

    def buy_limit(self, target: Target, vol, price=None):
        if not price:
            price = target.get_target_buy_price()

        str_price = target.check_price(price, True)
        price = float(str_price)
        symbol = target.symbol
        amount = target.check_amount(max(
            vol / price,
            target.limit_order_min_order_amt
        ))
        ice_amount = target.check_amount(amount * 0.9)
        now = int(time.time())
        order = dict(
            symbol=symbol,
            side='BUY',
            type='LIMIT',
            timeInForce='GTC',
            icebergQty=ice_amount,
            quantity=amount,
            price=str_price,
            timestamp=now,
            newOrderRespType='ACK'
        )
        # order_id = -1

        try:
            logger.debug(f'Buy {vol} {symbol[:-4]} with price {str_price}')
            response = self.api.new_order(**order)
            order_id = response['orderId']
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'buy')
                order_summary.created_ts = order_summary.ts = time.time() - 5
                self.orders[order_id] = order_summary

            order_summary.created_amount = amount
            order_summary.created_price = price
            order_summary.remain = amount
            self.buy_id.append(order_id)
            
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
            logger.debug(f'Sell {amount} {symbol[:-4]} with market price')
            response = self.api.new_order(**order)
            order_id = response['orderId']
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'sell')
                order_summary.created_ts = order_summary.ts = time.time() - 5
                self.orders[order_id] = order_summary

            order_summary.created_amount = amount
            order_summary.remain = amount
            self.sell_id.append(order_id)
            
            return order_summary
        except Exception as e:
            # order_summary.error(e)
            logger.error(e)
            raise Exception(e)

    def sell_limit(self, target: Target, amount, price, ioc=False):
        str_price = target.check_price(price, True)
        price = float(str_price)
        symbol = target.symbol
        amount = target.check_amount(max(
            amount,
            target.limit_order_min_order_amt
        ))
        now = int(time.time())
        ice_amount = target.check_amount(amount * 0.9)
        order = dict(
            symbol=symbol,
            side='SELL',
            type='LIMIT',
            timeInForce='GTC',
            quantity=amount,
            icebergQty=ice_amount,
            price=str_price,
            timestamp=now,
            newOrderRespType='ACK'
        )

        try:
            logger.debug(f'Sell {amount} {symbol[:-4]} with price {price}')
            response = self.api.new_order(**order)
            order_id = response['orderId']
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'sell')
                order_summary.created_ts = order_summary.ts = time.time() - 5
                self.orders[order_id] = order_summary

            order_summary.created_amount = amount
            order_summary.created_price = price
            order_summary.remain = amount
            self.sell_id.append(order_id)
            return order_summary
        except Exception as e:
            logger.error(e)
            raise Exception(e)

    def get_asset(self):
        self.update_currency()
        prices = self.market.get_market_tickers()
        asset = self.balance['USDT']
        for price in prices:
            if price.symbol.endswith('USDT'):
                currency = price.symbol[:-4]
                if currency in self.balance:
                    asset += self.balance[currency] * price.close
        return datetime.ts2date(), asset

    def get_asset_history(self, limit=30):
        asset_his = []
        snapshot = self.api.account_snapshot('SPOT', limit=limit, recvWindow=6000)
        btc_prices = self.market.get_candlestick('BTCUSDT', '1day', limit=limit)
        for day, btc in zip(snapshot['snapshotVos'], reversed(btc_prices)):
            ts = int(day['updateTime']/1000+1)
            asset = float(day['data']['totalAssetOfBtc']) * btc.open
            for each in day['data']['balances']:
                if each['locked'] != '0':
                    locked = float(each['locked'])
                    if each['asset'] == 'USDT':
                        asset += locked
                    else:
                        [kline] = self.market.get_candlestick(each['asset']+'USDT', '1min', start_ts=ts-1, end_ts=ts+1)
                        asset += locked * kline.open
            asset_his.append((datetime.ts2date(ts), asset))
        return asset_his
    
    def withdraw_usdt(self, address: str, amount: float):
        self.api.withdraw('USDT', amount, address, network='TRX')