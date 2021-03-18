import configparser
import ctypes
import functools
import os
import threading
import time

import pytz
import requests
from huobi.connection.impl.restapi_invoker import session
from huobi.connection.impl.websocket_manage import websocket_connection_handler
from huobi.connection.impl.websocket_watchdog import WebSocketWatchDog
from huobi.constant.system import WebSocketDefine, RestApiDefine
from logger import WxPusher, create_logger



ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, 'config.ini')
LOG_PATH = os.path.join(ROOT, 'log', 'trade.log')
URL = 'https://api-aws.huobi.pro'
WS_URL = 'wss://api-aws.huobi.pro'

logger = create_logger('goodmorning', LOG_PATH)
config = configparser.ConfigParser()
config.read(CONFIG_PATH)
session._request = session.request
session.request = lambda *args, **kwargs: session._request(timeout=5, *args, **kwargs)
WebSocketDefine.Uri = WS_URL
RestApiDefine.Url = URL

TOKEN = config.get('setting', 'Token')

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

def ws_url(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        self.__kwargs['url'] = WS_URL
        result = func(self, *args, **kwargs)
        self.__kwargs['url'] = URL
        return result
    return wrapper

def kill_thread(thread):
    thread._reset_internal_locks(False)
    thread_id = ctypes.c_long(thread._ident)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.py_object(SystemExit)) 
    if res > 1: 
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0) 
        print('Exception raise failure') 

def kill_all_threads():
    for manage in websocket_connection_handler.values():
        kill_thread(manage._WebsocketManage__thread)

    for thread in threading.enumerate():
        if isinstance(thread, WebSocketWatchDog):
            kill_thread(thread)
