from utils import datetime

import requests
from wxpusher.wxpusher import BASEURL, WxPusher as _WxPusher
from dataset.pgsql import Record, get_session, Profit, Message

from utils import user_config, logger
from retry import retry

TOKEN = user_config.get('setting', 'Token')

class WxPusher(_WxPusher):
    token = TOKEN

    @classmethod
    def send_message(cls, content, **kwargs):
        """Send Message."""
        payload = {
            'appToken': cls.token,
            'content': content,
            'summary': kwargs.get('summary', content[:19]),
            'contentType': kwargs.get('content_type', 0),
            'topicIds': kwargs.get('topic_ids', []),
            'uids': kwargs.get('uids', []),
            'url': kwargs.get('url'),
        }
        url = f'{BASEURL}/send/message'
        return requests.post(url, json=payload).json()

    @classmethod
    def query_user(cls, page=1, page_size=20, uid=None):
        """Query users."""
        payload = {
            'appToken': cls.token,
            'page': page,
            'pageSize': page_size,
            'uid': uid if uid else ''
        }
        url = f'{BASEURL}/fun/wxuser/v2'
        return requests.get(url, params=payload).json()

    @classmethod
    def get_user_name(cls, uid):
        result = cls.query_user(uid=uid)
        name  = result['data']['records'][0]['nickName']
        return name


def wx_push(content, uids, content_type=1, summary=None):
    @retry(tries=5, delay=1, logger=logger)
    def _wx_push(content, uids, content_type, summary):
        WxPusher.send_message(content, uids=uids, content_type=content_type, summary=summary)

    summary = summary or content[:20]
    _wx_push(content, uids, content_type, summary)

    with get_session() as session:
        message = Message(summary=summary, msg=content, uids=';'.join(uids), msg_type=content_type)
        session.add(message)
        session.commit()


@retry(tries=5, delay=1, logger=logger)
def wx_name(uid):
    return WxPusher.get_user_name(uid=uid)

def get_profit(account_id):
    with get_session() as session:
        month = datetime.ts2time('%Y-%m')
        total_profit = Profit.get_sum_profit(session, account_id)
        month_profit = Profit.get_sum_profit(session, account_id, month)
        return total_profit, month_profit

def add_profit(account_id, pay, income, profit, percent, now=0):
    day = datetime.ts2date(now)
    month = datetime.ts2time(now, fmt='%Y-%m')
    with get_session() as session:
        session.add(Profit(
            account=account_id,
            month=month,
            time=now,
            pay=pay,
            income=income,
            profit=profit,
            percent=percent
        ))
        session.commit()


@retry(tries=5, delay=0.2)
@datetime.force_tz(8)
def wx_loss_report(user_type, wxuid, username, report_info, usdt, day_profit, month_profit, all_profit):
    if not wxuid:
        return

    float_profit = sum([each[4] for each in report_info['holding']])
    summary = f'{datetime.ts2time()} {user_type} {username} 收益报告 | 当前浮盈{float_profit:.3f}U 已实现收益{all_profit:.3f}U'
    msg = f'''
### {user_type}用户

{username}
''' + ('''

### 新买入

| 币种 | 时间 | 价格 | 成交量 | 成交额 |
| ---- | ---- | ---- | ---- | ---- |
''' + '\n'.join([
    f'| {each[1]} | {each[0]} | {each[4]:.6g} | {each[2]:.4f} | {each[3]:.3f} |'
    for each in report_info['new_buy']
]) if report_info['new_buy'] else '') + ('''

### 新卖出

| 币种 | 时间 | 价格 | 买入价 | 成交量 | 成交额 | 收益 | 收益率 |
| ---- | ---- | ---- | ---- | ---- | ---- |  ---- | ---- |
''' + '\n'.join([
    f'| {each[1]} | {each[0]} | {each[4]:.6g} | {each[7]:.6g} | {each[2]:.4f} | {each[3]:.3f} | {each[5]:.3f} | {each[6]:.2%} |'
    for each in report_info['new_sell']
]) if report_info['new_sell'] else '') + (
'''
### 当前挂单

| 币种 | 时间 | 方向 | 价格 | 未成交量 | 
| ---- | ---- |  ---- | ---- | ---- |
''' + '\n'.join([
    f'| {each[1]} | {each[0]} | {"买入" if each[4]=="buy" else "卖出"} | {each[3]:.6g} | {each[2]:.4f} |'
    for each in report_info['opening']
]) if report_info['opening'] else '') + ('''

### 当前持有

| 币种 | 买入日期 | 成本价 | 现价 | 浮盈 | 浮盈率 | 数量 |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
''' + '\n'.join([
    f'| {each[0]} | {each[6]} | {each[2]:.6g} | {each[3]:.6g} | {each[4]:.3f} | {each[5]:.2%} | {each[1]:.4f} |'
    for each in report_info['holding']
]) if report_info['holding'] else '') + f'''

### 总结

- 剩余可用: **{usdt:.3f} USDT**

- 浮盈: **{float_profit:.3f} USDT**

- 日收益: **{day_profit:.3f} USDT**

- 合计日收益: **{float_profit+day_profit:.3f} USDT**

- 月收益: **{month_profit:.3f} USDT**

- 总收益: **{all_profit:.3f} USDT**
'''

    wx_push(content=msg, uids=wxuid, content_type=3, summary=summary)

@retry(tries=5, delay=0.2)
def wx_report(account_id, wxuid, username, pay, income, profit, percent, buy_info, sell_info, total_profit, month_profit):
    if not wxuid:
        return

    summary = f'{datetime.ts2time()} {username} 今日支出 {pay}, 收入 {income}, 利润 {profit}, 收益率 {percent}%'
    msg = f'''
### 用户
 
{username}


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
    wx_push(content=msg, uids=wxuid, content_type=3, summary=summary)

    with get_session() as session:
        profit_id = Profit.get_id(session, account_id, pay, income)
        buy_records = Record.from_record_info(buy_info, profit_id, 'buy')
        sell_records = Record.from_record_info(sell_info, profit_id, 'sell')
        session.add_all(buy_records + sell_records)
        session.commit()
