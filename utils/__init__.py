import configparser
import functools
import threading
import time
import os

import numpy as np
import requests
from huobi.connection.impl.restapi_invoker import session
from huobi.connection.impl.websocket_manage import websocket_connection_handler
from huobi.constant.system import RestApiDefine, WebSocketDefine
from huobi.utils import PrintBasic, input_checker

from utils.logging import create_logger, quite_logger
from utils.parallel import kill_thread
from utils.datetime import ts2time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(ROOT, 'config', 'config.ini')
USER_CONFIG_PATH = os.path.join(ROOT, 'config', 'user.ini')
TEST_CONFIG_PATH = os.path.join(ROOT, 'config', 'config.test.ini')
LOG_PATH = os.path.join(ROOT, 'log', 'trade.log')

URL = 'https://api-aws.huobi.pro'
WS_URL = 'wss://api-aws.huobi.pro'

logger_name = 'loss'
logger = create_logger(logger_name, LOG_PATH)
quite_logger(all_logger=True, except_list=[logger_name])
config = configparser.ConfigParser()
config.read(CONFIG_PATH)
user_config = configparser.ConfigParser()

test_config = configparser.ConfigParser()
if os.path.exists(TEST_CONFIG_PATH):
    test_config.read(TEST_CONFIG_PATH)

if os.path.exists(USER_CONFIG_PATH):
    user_config.read(USER_CONFIG_PATH)
    TEST = user_config.getboolean('setting', 'Test')
    if TEST:
        config = test_config


session._request = session.request
session.request = lambda *args, **kwargs: session._request(timeout=1, *args, **kwargs)
WebSocketDefine.Uri = WS_URL
RestApiDefine.Url = URL
PrintBasic.print_basic = lambda data, name=None: None
input_checker.reg_ex = "[ _`~!@#$%^&()+=|{}':;',\\[\\].<>/?~！@#￥%……&（）——+|{}【】‘；：”“’。，、？]|\n|\t"


def get_rate(a, b, k=5):
    if b and k==-1:
        return a/b-1
    elif b:
        return round((a/b)-1, k)
    else:
        return 0

def get_target_time():
    TIME = config.get('time', 'TIME')
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

    logger.info(f'Target time is {ts2time(target_time)}')
    return target_time

def timeout_handle(value):
    def wrapper(func):
        @functools.wraps(func)
        def sub_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.Timeout:
                return value
        return sub_wrapper
    return wrapper

def kill_all_threads():
    for manage in websocket_connection_handler.values():
        kill_thread(manage._WebsocketManage__thread)

    for thread in threading.enumerate():
        kill_thread(thread)

def get_boll(price, m):
    if not len(price):
        return [0] * len(m)

    sma = np.mean(price) #price.mean()
    std = np.std(price)  #price.std()
    return [sma+k*std for k in m]
