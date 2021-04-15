import time

from parallel import run_process
from wampyapp import DealerClient as Client
from utils import config, kill_all_threads, logger, user_config
from market import MarketClient
# from goodmorning import init_users
from retry import retry
from user import User

SELL_AFTER = config.getfloat('setting', 'SellAfter')

@retry(tries=5, delay=1, logger=logger)
def init_users():
    ACCESSKEY = user_config.get('setting', 'AccessKey')
    SECRETKEY = user_config.get('setting', 'SecretKey')
    BUY_AMOUNT = user_config.get('setting', 'BuyAmount')
    WXUIDS = user_config.get('setting', 'WxUid')
    TEST = user_config.getboolean('setting', 'Test')

    access_keys = [key.strip() for key in ACCESSKEY.split(',')]
    secret_keys = [key.strip() for key in SECRETKEY.split(',')]
    buy_amounts = [amount.strip() for amount in BUY_AMOUNT.split(',')]
    wxuids = [uid.strip() for uid in WXUIDS.split(',')]

    users = [User(*user_data) for user_data in zip(access_keys,
                                                   secret_keys, buy_amounts, wxuids)]
                                                   
    if TEST:
        users = users[:1]
    return users

@retry(tries=5, delay=1, logger=logger)
def init_dealer(user):
    market_client = MarketClient()
    client = Client(market_client, user)
    client.start()
    return client

def main(user: User):
    logger.info('Start run sub process')
    client = init_dealer(user)
    client.wait_to_run()

    sell_time = client.target_time + SELL_AFTER
    time.sleep(max(sell_time - time.time() - 5, 0.5))

    while time.time() < sell_time:
        pass

    client.stop()
    logger.info('Time to cancel')
    user.cancel_and_sell(client.targets.values())
    time.sleep(2)
    user.report()
    kill_all_threads()


if __name__ == '__main__':
    logger.info('Dealer')
    users = init_users()
    time.sleep(20)
    run_process([(main, (user,), user.username) for user in users], is_lock=True, limit_num=len(users)+2)
