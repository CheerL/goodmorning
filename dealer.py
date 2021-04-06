import time

from parallel import run_process
from wampyapp import DealerClient as Client
from utils import config, kill_all_threads, logger
from market import MarketClient
from goodmorning import init_users
from retry import retry
from user import User

SELL_AFTER = config.getfloat('setting', 'SellAfter')

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
    time.sleep(max(sell_time - time.time() - 5, 1))

    while time.time() < sell_time:
        pass

    client.stop()
    logger.info('Time to cancel')
    user.cancel_and_sell(client.targets)
    time.sleep(2)
    user.report()
    kill_all_threads()


if __name__ == '__main__':
    logger.info('Dealer')
    users = init_users()
    time.sleep(25)
    run_process([(main, (user,), user.username) for user in users], is_lock=True, limit_num=len(users)+2)
