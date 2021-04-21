import configparser
import functools
import os
import threading
import time

import pytz
import requests
from huobi.connection.impl.restapi_invoker import session
from huobi.connection.impl.websocket_manage import websocket_connection_handler
from huobi.constant.system import RestApiDefine, WebSocketDefine
from huobi.utils import PrintBasic

from logger import create_logger
from parallel import kill_thread

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, 'config', 'config.ini')
USER_CONFIG_PATH = os.path.join(ROOT, 'config', 'user.ini')
LOG_PATH = os.path.join(ROOT, 'log', 'trade.log')

URL = 'https://api-aws.huobi.pro'
WS_URL = 'wss://api-aws.huobi.pro'

logger = create_logger('goodmorning', LOG_PATH)
config = configparser.ConfigParser()
config.read(CONFIG_PATH)
user_config = configparser.ConfigParser()

if os.path.exists(USER_CONFIG_PATH):
    user_config.read(USER_CONFIG_PATH)


session._request = session.request
session.request = lambda *args, **kwargs: session._request(timeout=1, *args, **kwargs)
WebSocketDefine.Uri = WS_URL
RestApiDefine.Url = URL
PrintBasic.print_basic = lambda data, name=None: None


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

    logger.info(f'Target time is {strftime(target_time)}')
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
        # if isinstance(thread, WebSocketWatchDog):
        kill_thread(thread)
