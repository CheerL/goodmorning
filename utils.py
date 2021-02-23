import configparser
from logging import log
import math
import os
import time

from logger import create_logger, WxPusher

import pytz
from huobi.client.account import AccountClient
from huobi.client.generic import GenericClient
from huobi.client.market import MarketClient as _MarketClient
from huobi.client.trade import TradeClient
from huobi.client.algo import AlgoClient
from huobi.constant import *
from huobi.utils import *

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, 'config.ini')
LOG_PATH = os.path.join(ROOT, 'log', 'trade.log')

logger = create_logger('goodmorning', LOG_PATH)
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

TOKEN = config.get('setting', 'Token')
BEFORE = config.getint('setting', 'Before')
BOOT_PRECENT = config.getfloat('setting', 'BootPrecent')
AFTER = config.getint('setting', 'After')
BATCH_SIZE = config.getint('setting', 'Batchsize')
MAX_AFTER = config.getint('setting', 'MaxAfter')
MIN_VOL = config.getfloat('setting', 'MinVol')
MIDNIGHT_INTERVAL = config.getfloat('setting', 'MidnightInterval')

class MarketClient(_MarketClient):
    exclude_list = ['htusdt', 'btcusdt', 'bsvusdt', 'bchusdt', 'etcusdt', 'ethusdt']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        generic_client = GenericClient()

        self.price_record = {}
        self.symbols_info = {
            info.symbol: info
            for info in generic_client.get_exchange_symbols()
            if info.symbol.endswith('usdt') and info.symbol not in self.exclude_list
        }

    def get_price(self):
        market_data = self.get_market_tickers()
        price = {
            pair.symbol: (pair.close, pair.vol)
            for pair in market_data
            if pair.symbol in self.symbols_info
        }
        return price

    def get_vol(self, symbol):
        [kline] = self.get_candlestick(symbol, CandlestickInterval.MIN1, 1)
        return kline.vol

    def get_base_price(self, target_time):
        while True:
            now = time.time()
            if now < target_time - 310:
                logger.info('Wait 5mins')
                time.sleep(300)
            else:
                base_price = self.get_price()
                if now > target_time - BEFORE:
                    base_price_time = now
                    logger.debug(f'Get base price successfully')
                    break
                else:
                    time.sleep(1)
        
        return base_price, base_price_time

    def get_increase(self, initial_price):
        price = self.get_price()
        increase = [
            (
                symbol, close,
                round((close - initial_price[symbol][0]) / initial_price[symbol][0] * 100, 4),
                round(vol - initial_price[symbol][1], 4)
            )
            for symbol, (close, vol) in price.items()
        ]
        increase = sorted(increase, key=lambda pair: pair[2], reverse=True)
        return increase, price

    def get_big_increase(self, increase):
        big_increase = [
            item for item in increase
            if item[2] > BOOT_PRECENT
        ][:BATCH_SIZE]

        if big_increase:
            big_increase_vol = [self.get_vol(item[0]) for item in big_increase]
            big_increase = [
                (symbol, close, increment, vol)
                for ((symbol, close, increment, _), vol) in zip(big_increase, big_increase_vol)
                if vol > MIN_VOL
            ]

        return big_increase

    def handle_big_increase(self, big_increase, base_price):
        targets = []
        for symbol, now_price, target_increase, vol in big_increase:
            init_price, _ = base_price[symbol]
            target = self.symbols_info[symbol]
            self.price_record.setdefault(symbol, init_price)
            targets.append(target)
            logger.debug(f'Find target: {symbol.upper()}, initial price {init_price}, now price {now_price} , increase {target_increase}%, vol {vol} USDT')
        return targets

    def get_target(self, target_time, base_price, base_price_time):
        while True:
            now = time.time()
            increase, price = self.get_increase(base_price)
            big_increase = self.get_big_increase(increase)

            if big_increase:
                targets = self.handle_big_increase(big_increase, base_price)
                break
            elif now > target_time + MAX_AFTER:
                logger.warning(f'Fail to find target in {MAX_AFTER}s, exit')
                targets = []
                break
            else:
                logger.info('\t'.join([
                    f'{index+1}. {symbol.upper()} {increment}% {vol} USDT'
                    for index, (symbol, _, increment, vol) in enumerate(increase[:3])
                ]))
                if now - base_price_time > AFTER:
                    base_price_time = now
                    base_price = price
                    logger.info('User now base price')
                time.sleep(0.1)

        return targets

    def get_target_midnight(self, target_time, base_price, interval=MIDNIGHT_INTERVAL, unstop=False):
        while True:
            now = time.time()
            increase, price = self.get_increase(base_price)
            big_increase = self.get_big_increase(increase)

            if big_increase:
                targets = self.handle_big_increase(big_increase, base_price)
                break
            elif not unstop and now > target_time + interval:
                logger.warning(f'Fail to find target in {interval}s')
                targets = []
                break
            else:
                logger.info('\t'.join([
                    f'{index+1}. {symbol.upper()} {increment}% {vol} USDT'
                    for index, (symbol, _, increment, vol) in enumerate(increase[:3])
                ]))
                time.sleep(0.05)


        return targets, price

    @staticmethod
    def _precent_modify(t):
        return max(min(0.5 * t, 0.9), 0.5)

class User:
    def __init__(self, access_key, secret_key, buy_amount, wxuid):
        self.account_client = AccountClient(api_key=access_key, secret_key=secret_key)
        self.trade_client = TradeClient(api_key=access_key, secret_key=secret_key)
        self.algo_client = AlgoClient(api_key=access_key, secret_key=secret_key)
        self.access_key = access_key
        self.sercet_key = secret_key
        self.buy_amount = buy_amount
        self.wxuid = wxuid

        self.account_id = next(filter(
            lambda account: account.type=='spot' and account.state =='working',
            self.account_client.get_accounts()
        )).id

        self.balance = {}
        self.buy_order_list = []
        self.sell_order_list = []
        self.buy_id = []
        self.sell_id = []
        self.sell_algo_id = []

    @staticmethod
    def _check_amount(amount, symbol_info):
        precision_num = 10 ** symbol_info.amount_precision
        return math.floor(amount * precision_num) / precision_num

    @staticmethod
    def _check_price(price, symbol_info):
        precision_num = 10 ** symbol_info.price_precision
        return math.floor(price * precision_num) / precision_num

    def buy(self, targets, amounts):
        buy_order_list = [{
            "symbol": target.symbol,
            "account_id": self.account_id,
            "order_type": OrderType.BUY_MARKET,
            "source": OrderSource.API,
            "price": 1,
            "amount": self._check_amount(max(
                amount,
                target.min_order_value
            ), target)}
            for target, amount in zip(targets, amounts)
            if amount > 0
        ]

        self.buy_id.extend(self.trade_client.batch_create_order(buy_order_list))
        logger.debug(f'User {self.account_id} buy report')
        for order in buy_order_list:
            logger.debug(f'Speed {order["amount"]} USDT to buy {order["symbol"][:-4].upper()}')

        self.buy_order_list.extend(buy_order_list)

    def sell(self, targets, amounts):
        sell_order_list = [{
            "symbol": target.symbol,
            "account_id": self.account_id,
            "order_type": OrderType.SELL_MARKET,
            "source": OrderSource.API,
            "price": 1,
            "amount": self._check_amount(max(
                amount,
                target.min_order_amt,
                target.sell_market_min_order_amt
            ), target)}
            for target, amount in zip(targets, amounts)
            if amount > 0
        ]

        self.sell_id.extend(self.trade_client.batch_create_order(sell_order_list))
        logger.debug(f'User {self.account_id} sell report')
        for order in sell_order_list:
            logger.debug(f'Sell {order["amount"]} {order["symbol"][:-4].upper()} with market price')

        self.sell_order_list.extend(sell_order_list)

    def sell_algo(self, targets, amounts, price_record, rate):
        for target, amount in zip(targets, amounts):
            if amount <= 0:
                continue

            symbol = target.symbol
            stop_price = str(self._check_price(rate * price_record[symbol], target))
            amount = str(self._check_amount(max(
                amount,
                target.min_order_amt,
                target.sell_market_min_order_amt
            ), target))
            client_id = (symbol + stop_price + str(time.time())).replace('.', '_')
            sell_order_id = self.algo_client.create_order(
                account_id=self.account_id, symbol=symbol, order_side=OrderSide.SELL,
                order_type='market', stop_price=stop_price, order_size=amount,
                client_order_id=client_id
            )
            order = {
                "symbol": symbol,
                "price": stop_price,
                "amount": amount,
                "id": sell_order_id
            }
            self.sell_algo_id.append(client_id)
            self.sell_order_list.append(order)
            logger.debug(f'Sell {order["amount"]} {order["symbol"][:-4].upper()} with market price')

    def cancel_algo_and_sell(self):
        open_orders = self.algo_client.get_open_orders() or []
        if open_orders:
            open_ids = [order.clientOrderId for order in open_orders]
            self.algo_client.cancel_orders(open_ids)
            logger.info('Cancel algo orders')

            sell_order_list = [{
                "symbol": order.symbol,
                "account_id": self.account_id,
                "order_type": OrderType.SELL_MARKET,
                "source": OrderSource.API,
                "price": 1,
                "amount": float(order.orderSize)
                } for order in open_orders
            ]

            self.sell_id.extend(self.trade_client.batch_create_order(sell_order_list))
            logger.debug(f'User {self.account_id} sell report')
            for order in sell_order_list:
                logger.debug(f'Sell {order["amount"]} {order["symbol"][:-4].upper()} with market price')

            self.sell_order_list.extend(sell_order_list)
            self.sell_algo_id = [list(set(self.sell_algo_id)-set(open_ids))]


    def get_balance(self, targets):
        target_currencies = [target.base_currency for target in targets]
        self.balance = {
            currency.currency: float(currency.balance)
            for currency in self.account_client.get_balance(self.account_id)
            if currency.currency in target_currencies and currency.type == 'trade'
        }

    def check_balance(self, targets):
        self.get_balance(targets)

        logger.debug(f'User {self.account_id} balance report')
        for target, order in zip(targets, self.buy_order_list):
            target_balance = self.balance[target.base_currency]
            if target_balance > 10 ** -target.amount_precision:
                logger.debug(f'Get {target_balance} {target.base_currency.upper()} with average price {order["amount"] / target_balance}')
            else:
                logger.debug(f'Get 0 {target.base_currency.upper()}')

    def report(self):
        buy_order = [self.trade_client.get_order(order.order_id) for order in self.buy_id]
        sell_order = [self.trade_client.get_order(order.order_id) for order in self.sell_id] \
                     + [self.trade_client.get_order_by_client_order_id(order_id) for order_id in self.sell_algo_id]
        buy_info = [{
            'symbol': order.symbol,
            'time': strftime(order.finished_at / 1000, fmt='%Y-%m-%d %H:%M:%S.%f'),
            'price': round(float(order.filled_cash_amount) / float(order.filled_amount), 6),
            'amount': round(float(order.filled_amount), 6),
            'fee': round(float(order.filled_fees), 6),
            'currency': order.symbol[:-4].upper(),
            'vol': float(order.filled_cash_amount)
        } for order in buy_order
        if order.state == 'filled'
        ]
        sell_info = [{
            'symbol': order.symbol,
            'time': strftime(order.finished_at / 1000, fmt='%Y-%m-%d %H:%M:%S.%f'),
            'price': round(float(order.filled_cash_amount) / float(order.filled_amount), 6),
            'amount': round(float(order.filled_amount), 6),
            'fee': round(float(order.filled_fees), 6),
            'currency': order.symbol[:-4].upper(),
            'vol': float(order.filled_cash_amount)
        } for order in sell_order
        if order.state == 'filled'
        ]
        pay = round(sum([each['vol'] for each in buy_info]), 4)
        income = round(sum([each['vol'] for each in sell_info]), 4)
        profit = round(income - pay, 4)
        precent = round(profit / pay * 100, 4)

        logger.info(f'REPORT for user {self.account_id}')
        logger.info('Buy')
        for each in buy_info:
            currency = each['currency']
            symbol_name = '/'.join([currency, 'USDT'])
            vol = each['vol']
            amount = each['amount']
            price = each['price']
            fee = each['fee']
            logger.info(f'{symbol_name}: use {vol} USDT, get {amount} {currency}, price {price}, fee {fee} {currency}, at {each["time"]}')

        logger.info('Sell')
        for each in sell_info:
            currency = each['currency']
            symbol_name = '/'.join([currency, 'USDT'])
            vol = each['vol']
            amount = each['amount']
            price = each['price']
            fee = each['fee']
            logger.info(f'{symbol_name}: use {amount} {currency}, get {vol} USDT, price {price}, fee {fee} USDT, at {each["time"]}')

        logger.info(f'Totally pay {pay} USDT, get {income} USDT, profit {profit} USDT, {precent}%')

        if self.wxuid:
            summary = f'{strftime(time.time())} 本次交易支出 {pay}, 收入 {income}, 利润 {profit}, 收益率 {precent}%'
            msg = '''
### 买入记录

| 币种 | 时间 |价格 | 成交量 | 成交额 | 手续费 |
| ---- | ---- | ---- | ---- | ---- | ---- |
''' + \
'\n'.join([
    f'| {each["currency"]} | {each["time"]} | {each["price"]} | {each["amount"]} | {each["vol"]} | {each["fee"]} |'
    for each in buy_info
]) + '''
### 卖出记录

| 币种 | 时间 | 价格 | 成交量 | 成交额 | 手续费 |
| ---- | ---- | ---- | ---- | ---- | ---- |
''' + \
'\n'.join([
    f'| {each["currency"]} | {each["time"]} | {each["price"]} | {each["amount"]} | {each["vol"]} | {each["fee"]} |'
    for each in sell_info
]) + f'''
### 总结
            
- 支出: **{pay} USDT**

- 收入: **{income} USDT**

- 利润: **{profit} USDT**

- 收益率: **{precent} %**
'''
            wxpush(content=msg, uids=[self.wxuid], content_type=3, summary=summary)


def strftime(timestamp, tz_name='Asia/Shanghai', fmt='%Y-%m-%d %H:%M:%S'):
    tz = pytz.timezone(tz_name)
    utc_time = pytz.utc.localize(
        pytz.datetime.datetime.utcfromtimestamp(timestamp)
    )
    return utc_time.astimezone(tz).strftime(fmt)

def wxpush(content, uids, content_type=1, summary=None):
    WxPusher.send_message(content, uids=uids, token=TOKEN, content_type=content_type, summary=summary or content[:20])

def get_target_time():
    TIME = config.get('setting', 'Time')
    
    now = time.time()

    if TIME.startswith('*/'):
        TIME = int(TIME[2:])
        target_time = (now // (TIME * 60) + 1) * (TIME * 60)
    elif TIME.startswith('+'):
        TIME = int(TIME[1:])
        target_time = now + TIME
    else:
        hour_second = 60 * 60
        day_second = 24 * hour_second
        day_time = now // day_second * day_second
        target_list = [
            day_time + round((float(t) - 8) % 24 * hour_second)
            for t in TIME.split(',')
        ]
        target_list = sorted([
            t + day_second if now > t else t
            for t in target_list
        ])
        target_time = target_list[0]

    logger.debug(f'Target time is {strftime(target_time)}')
    return target_time

def initial():
    ACCESSKEY = config.get('setting', 'AccessKey')
    SECRETKEY = config.get('setting', 'SecretKey')
    BUY_AMOUNT = config.get('setting', 'BuyAmount')
    WXUIDS = config.get('setting', 'WxUid')
    TEST = config.getboolean('setting', 'Test')
    market_client = MarketClient()
    access_keys = [key.strip() for key in ACCESSKEY.split(',')]
    secret_keys = [key.strip() for key in SECRETKEY.split(',')]
    buy_amounts = [float(amount.strip()) for amount in BUY_AMOUNT.split(',')]
    wxuids = [uid.strip() for uid in WXUIDS.split(',')]

    users = [User(*user_data) for user_data in zip(access_keys, secret_keys, buy_amounts, wxuids)]
    if TEST:
        users = users[:1]

    target_time = get_target_time()

    return users, market_client, target_time