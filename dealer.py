import time

from parallel import run_thread
from wampyapp import DealerClient as Client
from utils import config, kill_all_threads, logger

from goodmorning import initial

SELL_AFTER = config.getfloat('setting', 'SellAfter')

def buy_and_sell(user, targets):
    user.buy(targets, [user.buy_amount for _ in targets])
    user.check_balance(targets)
    sell_amounts = [user.balance[target.base_currency] for target in targets]
    user.sell_limit(targets, sell_amounts)


def main():
    users, market_client, target_time = initial()
    client = Client(market_client, users)
    client.start()

    sell_time = target_time + SELL_AFTER
    time.sleep(max(sell_time - time.time() - 5, 1))

    while time.time() < sell_time:
        pass

    client.stop()
    logger.info('Time to cancel')
    run_thread([(user.cancel_and_sell, (client.targets, ))
                for user in users], is_lock=True)
    run_thread([(user.report, ()) for user in users], is_lock=True)
    kill_all_threads()


if __name__ == '__main__':
    main()
