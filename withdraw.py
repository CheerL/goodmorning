import argparse
import math
from user.huobi import HuobiUser
from user.binance import BinanceUser
from client.loss import LossDealerClient as DClient
from dataset.redis import Redis
from report import wx_withdraw_report
from utils import user_config, config

BN_ADDR = user_config.get('setting', 'BN_ADDR')
HB_ADDR = user_config.get('setting', 'HB_ADDR')
MAX_VOL = config.getfloat('withdraw', 'MAX_VOL')
MIN_VOL = config.getfloat('withdraw', 'MIN_VOL')

def binance2huobi(bn_addr, hb_addr, num):
    currency = 'USDT'
    [user] = BinanceUser.init_users(num=num)
    user.update_currency(currency)
    amount = math.floor(user.available_balance[currency])-1
    if amount > MIN_VOL:
        # date = datetime.ts2date()
        redis = Redis()
        key = f'{bn_addr}-{hb_addr}'
        old_amount = redis.get(key)
        if old_amount:
            old_amount = float(old_amount.decode())
            # amount += old_amount
        else:
            old_amount = 0

        amount = min(amount, MAX_VOL - old_amount)
        user.withdraw_usdt(hb_addr, amount)
        redis.set(key, amount + old_amount)
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
