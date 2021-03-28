import time

# from huobi.constant.definition import *

from market import MarketClient
from parallel import run_thread
from user import User
from utils import config, get_target_time, logger

SELL_AFTER = config.getfloat('setting', 'SellAfter')
TOKEN = config.get('setting', 'Token')

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
    return users, market_client, target_time

def main():
    users, market_client, target_time = initial()
    base_price, base_price_time = market_client.get_base_price(target_time)
    market_client.exclude_expensive(base_price)

    targets = []
    while True:
        tmp_targets = market_client.get_target(target_time, base_price, change_base=False, unstop=True)
        if tmp_targets:
            run_thread([(user.buy_and_sell, (tmp_targets, )) for user in users], is_lock=False)
            targets.extend(tmp_targets)
        else:
            break

    if not targets:
        logger.warning('No targets, exit')
        return

    while time.time() < target_time + SELL_AFTER:
        time.sleep(1)

    logger.info('Time to cancel')
    run_thread([(user.cancel_and_sell, (targets, )) for user in users], is_lock=True)
    run_thread([(user.report, ()) for user in users], is_lock=True)


if __name__ == '__main__':
    main()
