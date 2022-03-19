import pytz
import datetime
import functools
import time

TZ_DICT = {
    0: pytz.timezone('UTC'),
    8: pytz.timezone('Asia/Shanghai')
}

class Tz:
    tz_num = 8

    @classmethod
    def get_tz(cls):
        return TZ_DICT[cls.tz_num]

def force_tz(tz_num=0):
    def wrapper(func):
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            ori_tz_num = Tz.tz_num
            Tz.tz_num = tz_num
            try:
                result = func(*args, **kwargs)
            finally:
                Tz.tz_num = ori_tz_num
            return result
        return _wrapper
    return wrapper

def date2dt(date_str=''):
    return time2dt(date_str, '%Y-%m-%d')

def date2ts(date_str=''):
    return date2dt(date_str).timestamp()

def time2dt(time_str='', fmt='%Y-%m-%d %H:%M:%S'):
    if time_str:
        dt = datetime.datetime.strptime(time_str+f'+0{Tz.tz_num}00', fmt+'%z')
    else:
        dt = datetime.datetime.now()
    return dt.astimezone(tz=Tz.get_tz())

def time2ts(time_str='', fmt='%Y-%m-%d %H:%M:%S'):
    return time2dt(time_str, fmt).timestamp()

def ts2dt(ts=0):
    if ts:
        dt = datetime.datetime.fromtimestamp(ts)
    else:
        dt = datetime.datetime.now()
    return dt.astimezone(tz=Tz.get_tz())

def ts2time(ts=0, fmt='%Y-%m-%d %H:%M:%S'):
    return ts2dt(ts).strftime(fmt)


def ts2date(ts=0):
    return ts2time(ts, '%Y-%m-%d')

def ts2level_hour(ts=0, level_ts=86400):
    if ts == 0:
        ts = time.time()
    return ts2time(ts // level_ts * level_ts, '%Y-%m-%d-%H')