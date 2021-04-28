import time
import sys

from wampyapp import DealerClient, DealerClientV2, State
from utils.parallel import run_process
from utils import config, kill_all_threads, logger, user_config
from apscheduler.schedulers.gevent import GeventScheduler as Scheduler
from market import MarketClient
from retry import retry
from user import User

SECOND_SELL_AFTER = config.getfloat('setting', 'SecondSellAfter')
DEALER_STOP = config.getint('setting', 'DealerStop')

@retry(tries=5, delay=1, logger=logger)
def init_users() -> 'list[User]':
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
def init_dealer(user, Client):
    market_client = MarketClient()
    client = Client(market_client, user)
    client.start()
    return client

def main(user: User):
    is_v2 = len(sys.argv) > 1 and sys.argv[1] == 'V2'
    logger.info('Start run sub process')

    scheduler = Scheduler()
    if is_v2:
        client: DealerClientV2 = init_dealer(user, DealerClientV2)
        scheduler.add_job(client.check_sell, trigger='interval', hour=1)
    else:
        client: DealerClient = init_dealer(user, DealerClient)
        scheduler.add_job(client.high_sell_handler, args=['', 0], trigger='cron', hour=0, minute=0, second=int(SECOND_SELL_AFTER))
        scheduler.add_job(client.state_handler, args=[State.STARTED], trigger='cron', hour=0, minute=0, second=int(DEALER_STOP))
    scheduler.start()

    client.wait_state(State.RUNNING)
    client.wait_state(State.STARTED)

    scheduler.shutdown()
    client.stop()
    logger.info('Time to cancel')
    user.cancel_and_sell(client.targets.values())
    time.sleep(2)
    user.report()
    kill_all_threads()


if __name__ == '__main__':
    logger.info('Dealer')
    users = init_users()
    run_process([(main, (user,), user.username) for user in users], is_lock=True, limit_num=len(users)+2)
