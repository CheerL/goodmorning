import datetime
import os
import sqlite3
import time

import requests
from wxpusher.wxpusher import BASEURL
from wxpusher.wxpusher import WxPusher as _WxPusher

from utils import ROOT, config, user_config, strftime, logger
from retry import retry

TOKEN = user_config.get('setting', 'Token')

class WxPusher(_WxPusher):
    @classmethod
    def send_message(cls, content, **kwargs):
        """Send Message."""
        payload = {
            'appToken': cls._get_token(kwargs.get('token')),
            'content': content,
            'summary': kwargs.get('summary', content[:19]),
            'contentType': kwargs.get('content_type', 0),
            'topicIds': kwargs.get('topic_ids', []),
            'uids': kwargs.get('uids', []),
            'url': kwargs.get('url'),
        }
        url = f'{BASEURL}/send/message'
        return requests.post(url, json=payload).json()

@retry(tries=5, delay=1, logger=logger)
def wxpush(content, uids, content_type=1, summary=None):
    WxPusher.send_message(content, uids=uids, token=TOKEN, content_type=content_type, summary=summary or content[:20])

def get_profit(account_id):
    path = os.path.join(ROOT, 'log', 'profit.sqlite')
    if not os.path.exists(path):
        return 0, 0
    else:
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
        cursor.execute(f'''
        SELECT SUM (profit) FROM profit WHERE
        account=\'{account_id}\'
        ''')
        total_profit = round(cursor.fetchone()[0], 4)
        day = datetime.date.fromtimestamp(time.time())
        month = day.strftime('%Y-%m')
        cursor.execute(f'''
        SELECT SUM (profit) FROM profit WHERE
        (account=\'{account_id}\' AND month=\'{month}\')
        ''')
        month_profit = round(cursor.fetchone()[0], 4)
        cursor.close()
        conn.close()
        return total_profit, month_profit

def add_profit(account_id, pay, income, profit, percent, now=None):
    if not now:
        now = time.time()

    path = os.path.join(ROOT, 'log', 'profit.sqlite')
    if not os.path.exists(path):
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE profit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account varchar(20),
            month varchar(20),
            time DATETIME,
            pay REAL,
            income REAL,
            profit REAL,
            percent REAL
        )
        ''')
    else:
        conn = sqlite3.connect(path)
        cursor = conn.cursor()

    day = datetime.date.fromtimestamp(now)
    month = day.strftime('%Y-%m')
    cursor.execute(f'''
    INSERT into profit (account, time, pay, income, profit, percent, month) values 
    (\'{account_id}\', \'{now}\', \'{pay}\',
    \'{income}\',\'{profit}\', \'{percent}\', \'{month}\')
    ''')
    cursor.close()
    conn.commit()
    conn.close()

def wx_report(wxuid, pay, income, profit, percent, buy_info, sell_info, total_profit, month_profit):
    if not wxuid:
        return
    summary = f'{strftime(time.time())} 本次交易支出 {pay}, 收入 {income}, 利润 {profit}, 收益率 {percent}%'
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
- 收益率: **{percent} %**
- 月累计收益: **{month_profit} USDT**
- 总累计收益: **{total_profit} USDT**
'''
    wxpush(content=msg, uids=[wxuid], content_type=3, summary=summary)