import configparser
import os
import time

import pytz

from logger import WxPusher, create_logger

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, 'config.ini')
LOG_PATH = os.path.join(ROOT, 'log', 'trade.log')

logger = create_logger('goodmorning', LOG_PATH)
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

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
