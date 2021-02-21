import configparser
import math
import os
import time

from logger import create_logger
from collections import namedtuple

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

class User:
    def __init__(self, account_client, trade_client, account_id, access_key, secret_key, balance):
        self.account_client = account_client
        self.trade_client = trade_client
        self.account_id = account_id
        self.access_key = access_key
        self.sercet_key = secret_key = 40

def strftime(timestamp, tz_name='Asia/Shanghai', fmt='%Y-%m-%d %H:%M:%S'):
    tz = pytz.timezone(tz_name)
    utc_time = pytz.utc.localize(
        pytz.datetime.datetime.utcfromtimestamp(timestamp)
    )
    return utc_time.astimezone(tz).strftime(fmt)

def get_target_time():
    TIME = config.get('setting', 'Time')
    
    now = time.time()

    if TIME.startswith('*/'):
        TIME = int(TIME[2:])
        return (now // (TIME * 60) + 1) * (TIME * 60)

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
    return target_time

def check_amount(amount, symbol_info):
    precision_num = 10 ** symbol_info.amount_precision
    return math.floor(amount * precision_num) / precision_num

def get_info():
    ACCESSKEY = config.get('setting', 'AccessKey')
    SECRETKEY = config.get('setting', 'SecretKey')
    generic_client = GenericClient()
    market_client = MarketClient()
    access_keys = [key.strip() for key in ACCESSKEY.split(',')]
    secret_keys = [key.strip() for key in SECRETKEY.split(',')]
    
    users = [
        User(
            account_client=AccountClient(api_key=access_key, secret_key=secret_key),
            trade_client=TradeClient(api_key=access_key, secret_key=secret_key),
            access_key=access_key,
            secret_key=secret_key,
            account_id=None,
            balance=None
        )
        for access_key, secret_key in zip(access_keys, secret_keys)
    ]
    for user in users:
        accounts = user.account_client.get_accounts()
        user.account_id = next(filter(
            lambda account: account.type=='spot' and account.state =='working',
            accounts
        )).id

    symbols_info = {
        info.symbol: info
        for info in generic_client.get_exchange_symbols()
        if info.symbol.endswith('usdt')
    }
    return symbols_info, market_client, users