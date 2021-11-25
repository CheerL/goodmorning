import argparse
import math
from utils import user_config
from user.huobi import HuobiUser
from user.binance import BinanceUser
from client.loss import LossDealerClient as DClient
from dataset.redis import Redis
from report import wx_withdraw_report

BN_ADDR = user_config.get('BN_ADDR')
HB_ADDR = user_config.get('HB_ADDR')

def binance2huobi(bn_addr, hb_addr, num):
    currency = 'USDT'
    [user] = BinanceUser.init_users(num=num)
    user.update_currency(currency)
    amount = math.floor(user.available_balance[currency])-1
    if amount > 500:
        # date = datetime.ts2date()
        user.withdraw_usdt(hb_addr, amount)
        redis = Redis()
        key = f'{bn_addr}-{hb_addr}'
        old_amount = redis.get(key)
        if old_amount:
            old_amount = float(old_amount.decode())
            amount += old_amount

        redis.set(key, amount)
        wx_withdraw_report(user.wxuid, bn_addr, hb_addr, 'b2h', amount, currency)

def huobi2binance(bn_addr, hb_addr, num):
    currency = 'usdt'
    [bn_user] = BinanceUser.init_users(num=num)
    [hb_usdt] = HuobiUser.init_users(num=num)
    dealer = DClient(bn_user)
    targets, _ = dealer.find_targets()
    if targets:
        redis = Redis()
        key = f'{bn_addr}-{hb_addr}'
        amount = redis.get(key)
        amount = float(amount.decode())
        hb_usdt.withdraw_usdt(bn_addr, amount)
        redis.delete(key)
        wx_withdraw_report(hb_usdt.wxuid, bn_addr, hb_addr, 'h2b', amount, currency)
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--type', default='b2h', type=str)
    parser.add_argument('-n', '--num', default=0, type=int)

    args = parser.parse_args()

    if args.type == 'b2h':
        binance2huobi(BN_ADDR, HB_ADDR, args.num)
    elif args.type == 'h2b':
        huobi2binance(BN_ADDR, HB_ADDR, args.num)
