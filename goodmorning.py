import time

from huobi.constant.definition import *

from market import MarketClient
from parallel import run_thread
from user import User
from utils import config, get_target_time, logger, user_config

SELL_AFTER = config.getfloat('setting', 'SellAfter')

def init_users():
    ACCESSKEY = user_config.get('setting', 'AccessKey')
    SECRETKEY = user_config.get('setting', 'SecretKey')
    BUY_AMOUNT = user_config.get('setting', 'BuyAmount')
    WXUIDS = user_config.get('setting', 'WxUid')
    access_keys = [key.strip() for key in ACCESSKEY.split(',')]
    secret_keys = [key.strip() for key in SECRETKEY.split(',')]
    buy_amounts = [amount.strip() for amount in BUY_AMOUNT.split(',')]
    wxuids = [uid.strip() for uid in WXUIDS.split(',')]

    users = [User(*user_data) for user_data in zip(access_keys,
                                                   secret_keys, buy_amounts, wxuids)]
    return users
def initial():
    users = init_users()
    TEST = config.getboolean('setting', 'Test')
    market_client = MarketClient()
    
    if TEST:
        users = users[:1]

    target_time = get_target_time()
    return users, market_client, target_time

def cancel_and_sell_after(users, targets, t):
    while time.time() < t:
        time.sleep(1)
        open_orders = []
        run_thread([(
            lambda user, targets: open_orders.extend(user.get_open_orders(targets)),
            (user, targets)
            ) for user in users
        ])

        if not open_orders:
            logger.info('No open orders')
            break
    else:
        logger.info('Time to cancel')

    run_thread([(
        lambda user, targets: user.cancel_and_sell(targets),
        (user, targets, )
    ) for user in users], is_lock=True)

def buy_and_sell(user, targets):
    user.buy(targets, [user.buy_amount for _ in targets])
    user.check_balance(targets)
    sell_amounts = [user.balance[target.base_currency] for target in targets]
    user.sell_limit(targets, sell_amounts)

def main():
    users, market_client, target_time = initial()
    base_price, base_price_time = market_client.get_base_price(target_time)
    market_client.exclude_expensive(base_price)

    targets = []
    while True:
        tmp_targets = market_client.get_target(target_time, base_price, change_base=False, unstop=True)
        if tmp_targets:
            run_thread([
                (buy_and_sell, (user, tmp_targets, ))
                for user in users
            ], is_lock=False)
            targets.extend(tmp_targets)
        else:
            break

    if not targets:
        logger.warning('No targets, exit')
        return

    cancel_and_sell_after(users, targets, target_time + SELL_AFTER)
    run_thread([(lambda user: user.report(), (user, )) for user in users])


if __name__ == '__main__':
    main()
