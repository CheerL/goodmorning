import argparse
import math
from user.huobi import HuobiUser
from user.binance import BinanceUser
from client.loss import LossDealerClient as DClient
from dataset.redis import Redis
from report import wx_withdraw_report
from utils import user_config, config, logger

BN_ADDR = user_config.get('setting', 'BN_ADDR')
HB_ADDR = user_config.get('setting', 'HB_ADDR')
MAX_VOL = config.getfloat('withdraw', 'MAX_VOL')
MIN_VOL = config.getfloat('withdraw', 'MIN_VOL')

def binance2huobi(bn_addr, hb_addr, num):
    try:
        currency = 'USDT'
        [user] = BinanceUser.init_users(num=num)
        user.update_currency(currency)
        amount = math.floor(user.available_balance[currency])-1
        logger.info(f'Binance account has {amount}U')
        if amount > MIN_VOL:
            redis = Redis()
            key = f'{bn_addr}-{hb_addr}'
            old_amount = redis.get(key)
            old_amount = float(old_amount.decode()) if amount else 0
            amount = min(amount, MAX_VOL - old_amount)
            logger.info(f'{old_amount}U has been transfered from Huobi to Binance')
            logger.info(f'Withdraw {amount}U to Binance')
            user.withdraw_usdt(hb_addr, amount)
            redis.set(key, amount + old_amount)
            wx_withdraw_report(user.wxuid, bn_addr, hb_addr, 'b2h', amount, currency)
    except Exception as e:
        logger.error(e)

def huobi2binance(bn_addr, hb_addr, num):
    try:
        currency = 'usdt'
        [bn_user] = BinanceUser.init_users(num=num)
        [hb_usdt] = HuobiUser.init_users(num=num)
        dealer = DClient(bn_user)
        targets, _ = dealer.find_targets()
        redis = Redis()
        key = f'{bn_addr}-{hb_addr}'
        amount = redis.get(key)
        amount = float(amount.encode()) if amount else 0
        logger.info(f'{amount}U has been transfered from Huobi to Binance')
        if targets and amount:
            logger.info(f'Withdraw {amount}U to Binance')
            hb_usdt.withdraw_usdt(bn_addr, amount)
            redis.delete(key)
            wx_withdraw_report(hb_usdt.wxuid, bn_addr, hb_addr, 'h2b', amount, currency)
    except Exception as e:
        logger.error(e)

def manual_withdraw(type, bn_addr, hb_addr, num, amount):
    try:
        if type == 'b2h':
            [user] = BinanceUser.init_users(num=num)
            user.withdraw_usdt(hb_addr, amount)
            logger.info(f'Withdraw {amount}U to Huobi')
            wx_withdraw_report(user.wxuid, bn_addr, hb_addr, type, amount, 'USDT')
        elif type == 'h2b':
            [user] = HuobiUser.init_users(num=num)
            user.withdraw_usdt(bn_addr, amount)
            logger.info(f'Withdraw {amount}U to Binance')
            wx_withdraw_report(user.wxuid, bn_addr, hb_addr, type, amount, 'USDT')
    except Exception as e:
        logger.info(e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--type', default='b2h', type=str)
    parser.add_argument('-n', '--num', default=0, type=int)
    parser.add_argument('--manual', action='store_true', default=False)
    parser.add_argument('--amount', default=0, type=float)
    parser.add_argument('--bn_addr', default=BN_ADDR)
    parser.add_argument('--hb_addr', default=HB_ADDR)

    args = parser.parse_args()

    if args.manual and args.amount:
        manual_withdraw(args.type, args.bn_addr, args.hb_addr, args.num, args.amount)
    else:
        if args.type == 'b2h':
            binance2huobi(args.bn_addr, args.hb_addr, args.num)
        elif args.type == 'h2b':
            huobi2binance(args.bn_addr, args.hb_addr, args.num)
