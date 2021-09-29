import pytz
import datetime

def date2dt(date_str=''):
    tz = pytz.timezone('Asia/Shanghai')
    if date_str:
        dt = datetime.datetime.strptime(date_str+'+0800', '%Y-%m-%d%z')
    else:
        dt = datetime.datetime.now()
    return dt.astimezone(tz=tz)

def date2ts(date_str=''):
    return date2dt(date_str).timestamp()

def ts2dt(ts=0):
    tz = pytz.timezone('Asia/Shanghai')
    if ts:
        dt = datetime.datetime.fromtimestamp(ts)
    else:
        dt = datetime.datetime.now()
    return dt.astimezone(tz=tz)

def ts2time(ts=0, fmt='%Y-%m-%d %H:%M:%S'):
    return ts2dt(ts).strftime(fmt)

def ts2date(ts=0):
    return ts2time(ts, '%Y-%m-%d')
