import configparser
import math
import os
import time

from logger import create_logger

import pytz
from huobi.client.account import AccountClient
from huobi.client.generic import GenericClient
from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient
from huobi.constant import *
from huobi.utils import *

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, 'config.ini')
LOG_PATH = os.path.join(ROOT, 'log', 'trade.log')

logger = create_logger('goodmorning', LOG_PATH)
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

_HOUR_SECOND = 60 * 60
_DAY_SECOND = 24 * _HOUR_SECOND
_TZ = 'Asia/Shanghai'

ACCESSKEY = config.get('setting', 'AccessKey')
SECRETKEY = config.get('setting', 'SecretKey')
TIME = config.get('setting', 'Time')
START_PERCENT = config.getfloat('setting', 'StartPrecent')
BUY_AMOUNT = config.getfloat('setting', 'BuyAmount')
SELL_RATE = config.getfloat('setting', 'SellRate')

def strftime(timestamp, tz_name=_TZ, fmt='%Y-%m-%d %H:%M:%S'):
    tz = pytz.timezone(tz_name)
    utc_time = pytz.utc.localize(
        pytz.datetime.datetime.utcfromtimestamp(timestamp)
    )
    return utc_time.astimezone(tz).strftime(fmt)


def get_target_time():
    now = time.time()
    day_time = now // _DAY_SECOND * _DAY_SECOND
    target_list = [
        day_time + round((float(t) - 8) % 24 * _HOUR_SECOND)
        for t in TIME.split(',')
    ]
    target_list = sorted([
        t + _DAY_SECOND if now > t else t
        for t in target_list
    ])
    target_time = target_list[0]
    return target_time

def get_spot_account_id(account_client):
    accounts = account_client.get_accounts()
    spot_account_ids = [
        account.id for account in accounts
        if account.type=='spot'
        and account.state =='working'
    ]
    return spot_account_ids[0]

def get_initial_price(market_client):
    market_data = market_client.get_market_tickers()
    initial_price = {
        pair.symbol: pair.close
        for pair in market_data
        if pair.symbol.endswith('usdt')
    }
    return initial_price

def get_increase(market_client, initial_price):
    market_data = market_client.get_market_tickers()
    increase = [(
        pair.symbol, pair.close,
        (pair.close - initial_price[pair.symbol])/initial_price[pair.symbol])
        for pair in market_data
        if pair.symbol.endswith('usdt')
    ]
    increase = sorted(increase, key=lambda pair: pair[2], reverse=True)
    return increase

def get_currency(account_client, account_id, currency):
    currencies_info = account_client.get_balance(account_id)
    currency_info = next(filter(
        lambda info: info.currency == currency and info.type == 'trade',
        currencies_info
    ))
    return float(currency_info.balance)

def check_amount(amount, symbol_info):
    precision_num = 10 ** symbol_info.amount_precision
    return math.floor(amount * precision_num) / precision_num


generic_client = GenericClient(api_key=ACCESSKEY, secret_key=SECRETKEY)
account_client = AccountClient(api_key=ACCESSKEY, secret_key=SECRETKEY)
market_client = MarketClient(api_key=ACCESSKEY, secret_key=SECRETKEY, init_log=True)
trade_client = TradeClient(api_key=ACCESSKEY, secret_key=SECRETKEY, init_log=True)
account_id = get_spot_account_id(account_client)
symbols_info = generic_client.get_exchange_symbols()

def main():
    target_time = get_target_time()
    logger.debug(f'Target time is {strftime(target_time)}')


    while True:
        if time.time() > target_time - 5:
            initial_price = get_initial_price(market_client)
            logger.debug(f'Get initial price successfully')
            break
        else:
            if target_time - time.time() > 300:
                logger.info('Wait 5mins')
                time.sleep(300)
            else:
                time.sleep(1)

    while time.time() < target_time:
        pass

    while True:
        try:
            increase = get_increase(market_client, initial_price)
            if increase[0][2] > START_PERCENT:
                target = increase[0]
                symbol = target[0]
                break
        except:
            pass

    symbol_info = next(filter(
        lambda pair: pair.symbol == symbol,
        symbols_info
    ))
    currency = symbol_info.base_currency
    logger.debug(f'Find target, {currency.upper()} increase {round(target[2] * 100, 2)}%')


    buy_amount = check_amount(max(
        BUY_AMOUNT,
        symbol_info.min_order_value
    ), symbol_info)
    buy_id = trade_client.create_spot_order(
        symbol=symbol, account_id=account_id,
        order_type=OrderType.BUY_MARKET,
        amount=buy_amount, price=increase[0][1]
    )
    logger.debug(f'Speed {buy_amount} USDT to buy {currency.upper()}')
    
    while True:
        try:
            balance = get_currency(account_client, account_id, currency)
            if balance > 0:
                logger.debug(f'Get {balance} {currency.upper()}')
                break
        except:
            pass

    base_price = initial_price[symbol]
    sell_price = round(base_price * SELL_RATE, symbol_info.price_precision)
    sell_amount = check_amount(max(
        balance,
        symbol_info.min_order_amt,
        symbol_info.limit_order_min_order_amt
    ), symbol_info)
    sell_id = trade_client.create_spot_order(
        symbol=symbol, account_id=account_id,
        order_type=OrderType.SELL_LIMIT,
        amount=sell_amount, price=sell_price
    )
    logger.debug(f'Sell {sell_amount} {currency.upper()} with price {sell_price}')
    
    time.sleep(120)
    sell_order = trade_client.get_order(sell_id)
    if sell_order.state != 'filled':
        trade_client.cancel_order(symbol, sell_id)
        logger.warning('Sell order doesnt deal, cancel it')
        time.sleep(5)

        left_balance = get_currency(account_client, account_id, currency)
        left_sell_amount = check_amount(max(
            left_balance,
            symbol_info.min_order_amt,
            symbol_info.sell_market_min_order_amt
        ), symbol_info)
        trade_client.create_spot_order(
            symbol=symbol, account_id=account_id,
            order_type=OrderType.SELL_MARKET,
            amount=left_sell_amount, price=sell_price
        )
        logger.debug(f'Sell {left_sell_amount} {currency.upper()} with market price')

    logger.debug('Exit')

if __name__ == '__main__':
    main()

