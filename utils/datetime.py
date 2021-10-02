import pytz
import datetime

TZ_DICT = {
    0: 'UTC',
    8: 'Asia/Shanghai'
}

class TzConfig:
    tz_num = 8

def date2dt(date_str=''):
    return time2dt(date_str, '%Y-%m-%d')

def date2ts(date_str=''):
    return date2dt(date_str).timestamp()

def time2dt(time_str='', fmt='%Y-%m-%d %H:%M:%S'):
    tz = pytz.timezone(TZ_DICT[TzConfig.tz_num])
    if time_str:
        dt = datetime.datetime.strptime(time_str+f'+0{TzConfig.tz_num}00', fmt+'%z')
    else:
        dt = datetime.datetime.now()
    return dt.astimezone(tz=tz)

def time2ts(time_str='', fmt='%Y-%m-%d %H:%M:%S'):
    return time2dt(time_str, fmt).timestamp()

def ts2dt(ts=0):
    tz = pytz.timezone(TZ_DICT[TzConfig.tz_num])
    if ts:
        dt = datetime.datetime.fromtimestamp(ts)
    else:
        dt = datetime.datetime.now()
    return dt.astimezone(tz=tz)

def ts2time(ts=0, fmt='%Y-%m-%d %H:%M:%S'):
    return ts2dt(ts).strftime(fmt)

def ts2date(ts=0):
    return ts2time(ts, '%Y-%m-%d')
