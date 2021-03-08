from huobi.constant.definition import *
from utils import logger, config, get_target_time
from user import User
from market import MarketClient
from parallel import run_thread
import time

SELL_INTERVAL = config.getfloat('setting', 'SellInterval')
SELL_AFTER = config.getfloat('setting', 'SellAfter')
MIDNIGHT = config.getboolean('setting', 'Midnight')
MIDNIGHT_INTERVAL = config.getfloat('setting', 'MidnightInterval')
MIDNIGHT_SELL_AFTER = config.getfloat('setting', 'MidnightSellAfter')


def initial():
    ACCESSKEY = config.get('setting', 'AccessKey')
    SECRETKEY = config.get('setting', 'SecretKey')
    BUY_AMOUNT = config.get('setting', 'BuyAmount')
    WXUIDS = config.get('setting', 'WxUid')
    TEST = config.getboolean('setting', 'Test')
    market_client = MarketClient()
    access_keys = [key.strip() for key in ACCESSKEY.split(',')]
    secret_keys = [key.strip() for key in SECRETKEY.split(',')]
    buy_amounts = [amount.strip() for amount in BUY_AMOUNT.split(',')]
    wxuids = [uid.strip() for uid in WXUIDS.split(',')]

    users = [User(*user_data) for user_data in zip(access_keys,
                                                   secret_keys, buy_amounts, wxuids)]
    if TEST:
        users = users[:1]

    target_time = get_target_time()
    if MIDNIGHT or target_time % (24*60*60) == 16*60*60:
        market_client.midnight = True

    return users, market_client, target_time

def cancel_and_sell_after(users, targets, t):
    while time.time() < t:
        open_orders = []

        run_thread([(
            lambda user, targets: open_orders.extend(user.get_open_orders(targets)),
            (user, targets)
            ) for user in users
        ])
        if open_orders:
            time.sleep(1)
        # for user in users:
        #     if user.get_open_orders(targets):
        #         time.sleep(1)
        #         break
        else:
            logger.info('No open orders')
            break
    else:
        logger.info('Time to cancel')

    run_thread([(
        lambda user, targets: user.cancel_and_sell(targets),
        (user, targets, )
    ) for user in users], is_lock=True)
    # for user in users:
    #     user.cancel_and_sell(targets)

def buy_and_sell(user, targets):
    user.buy(targets, [user.buy_amount for _ in targets])
    user.check_balance(targets)
    sell_amounts = [user.balance[target.base_currency] for target in targets]
    user.sell_limit(targets, sell_amounts)

def main():
    users, market_client, target_time = initial()
    base_price, base_price_time = market_client.get_base_price(target_time)
    market_client.exclude_expensive(base_price)

    if market_client.midnight:
        logger.info('Midnight model')
        targets_1 = market_client.get_target(
            target_time, base_price, change_base=False, interval=MIDNIGHT_INTERVAL, unstop=True
        )
        if targets_1:
            run_thread([
                (buy_and_sell, (user, targets_1, ))
                for user in users
            ], is_lock=False)

        targets_2 = market_client.get_target(
            time.time(), base_price, change_base=False, interval=MIDNIGHT_INTERVAL
        )
        if targets_2:
            run_thread([
                (buy_and_sell, (user, targets_2, ))
                for user in users
            ], is_lock=False)

        targets_3 = market_client.get_target(
            time.time(), base_price, change_base=False, interval=MIDNIGHT_INTERVAL
        )
        if targets_3:
            run_thread([
                (buy_and_sell, (user, targets_3, ))
                for user in users
            ], is_lock=False)

        targets = list(set(targets_1+targets_2+targets_3))
        if not targets:
            logger.warning('No targets in 3 tries, exit')
            return

        cancel_and_sell_after(users, targets, target_time + MIDNIGHT_SELL_AFTER)
    else:
        logger.info('General model')
        targets = market_client.get_target(
            target_time, base_price, base_price_time)

        if not targets:
            logger.info('Exit')
            return

        run_thread([
            (buy_and_sell, (user, targets, ))
            for user in users
        ], is_lock=False)

        buy_time = time.time()
        cancel_and_sell_after(users, targets, buy_time + SELL_AFTER)

    run_thread([(lambda user: user.report(), (user, )) for user in users])


if __name__ == '__main__':
    main()
