from huobi.constant.definition import *
from utils import logger, config, get_target_time
from user import User
from market import MarketClient
import time
import itertools

SELL_INTERVAL = config.getfloat('setting', 'SellInterval')
SELL_AFTER = config.getfloat('setting', 'SellAfter')
MIDNIGHT = config.getboolean('setting', 'Midnight')
MIDNIGHT_INTERVAL = config.getfloat('setting', 'MidnightInterval')


def initial():
    ACCESSKEY = config.get('setting', 'AccessKey')
    SECRETKEY = config.get('setting', 'SecretKey')
    BUY_AMOUNT = config.get('setting', 'BuyAmount')
    WXUIDS = config.get('setting', 'WxUid')
    TEST = config.getboolean('setting', 'Test')
    market_client = MarketClient()
    access_keys = [key.strip() for key in ACCESSKEY.split(',')]
    secret_keys = [key.strip() for key in SECRETKEY.split(',')]
    buy_amounts = [float(amount.strip()) for amount in BUY_AMOUNT.split(',')]
    wxuids = [uid.strip() for uid in WXUIDS.split(',')]

    users = [User(*user_data) for user_data in zip(access_keys, secret_keys, buy_amounts, wxuids)]
    if TEST:
        users = users[:1]

    target_time = get_target_time()

    return users, market_client, target_time

def cancel_and_sell_after(users, targets, t):
    while time.time() < t:
        for user, target in itertools.product(users, targets):
            open_orders = user.trade_client.get_open_orders(target.symbol, user.account_id, OrderSide.SELL)
            if open_orders:
                time.sleep(1)
                break
        else:
            logger.info('No open orders')
            break
    else:
        logger.info('Time to cancel')

    for user in users:
        user.cancel_and_sell(targets)

def main():
    users, market_client, target_time = initial()
    base_price, base_price_time = market_client.get_base_price(target_time)

    if MIDNIGHT or target_time % (24*60*60) == 16*60*60:
        logger.info('Midnight model')
        targets_1 = market_client.get_target(
            target_time, base_price, change_base=False, interval=MIDNIGHT_INTERVAL, unstop=True
        )
        if targets_1:
            for user in users:
                user.buy(targets_1, [user.buy_amount for _ in targets_1])
            for user in users:
                user.check_balance(targets_1)
            for user in users:
                sell_amounts = [user.balance[target.base_currency] for target in targets_1]
                user.sell_limit(targets_1, sell_amounts)

        targets_2 = market_client.get_target(
            time.time(), base_price, change_base=False, interval=MIDNIGHT_INTERVAL
        )
        if targets_2:
            for user in users:
                user.buy(targets_2, [user.buy_amount for _ in targets_2])
            for user in users:
                user.check_balance(targets_2)
            for user in users:
                sell_amounts = [user.balance[target.base_currency] for target in targets_2]
                user.sell_limit(targets_2, sell_amounts)

        targets_3 = market_client.get_target(
            time.time(), base_price, change_base=False, interval=MIDNIGHT_INTERVAL
        )
        if targets_3:
            for user in users:
                user.buy(targets_3, [user.buy_amount for _ in targets_3])
            for user in users:
                user.check_balance(targets_3)
            for user in users:
                sell_amounts = [user.balance[target.base_currency] for target in targets_3]
                user.sell_limit(targets_3, sell_amounts)

        buy_time = time.time()
        targets = list(set(targets_1+targets_2+targets_3))
        if not targets:
            logger.warning('No targets in 3 tries, exit')
            return

        # cancel_after(users, buy_time + SELL_AFTER)
    else:
        logger.info('General model')
        targets = market_client.get_target(target_time, base_price, base_price_time)
        if not targets:
            logger.info('Exit')
            return

        for user in users:
            user.buy(targets, [user.buy_amount for _ in targets])

        buy_time = time.time()

        for user in users:
            user.check_balance(targets)

        for user in users:
            sell_amounts = [user.balance[target.base_currency] for target in targets]
            user.sell_limit(targets, sell_amounts)

    cancel_and_sell_after(users, targets, buy_time + SELL_AFTER)

    for user in users:
        user.report()


if __name__ == '__main__':
    main()

