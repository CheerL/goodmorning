
import argparse
import time

from retry.api import retry

from target import LossTarget as Target
from utils import config, kill_all_threads, logger
from utils.parallel import run_process
from user import LossUser  as User
from client.dealer import LossDealerClient as Client
from apscheduler.schedulers.gevent import GeventScheduler as Scheduler

def main(user: User):
    @retry(tries=30, delay=1)
    def buy_targets():
        assert client.targets, 'No targets'

        for target in client.targets.values():
            client.buy_limit_target(target)

    user.start()
    client = Client.init_dealer(user)
    scheduler = Scheduler()
    scheduler.add_job(client.find_targets, kwargs={'end': 0}, trigger='cron', hour=23, minute=59, second=0)
    scheduler.add_job(client.watch_targets, trigger='cron', hour=23, minute=59, second=0)
    scheduler.add_job(buy_targets, trigger='cron', hour=23, minute=59, second=0)

    client.wait_state(10)
    kill_all_threads()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num', default=-1, type=int)
    args = parser.parse_args()

    logger.info('Dealer')
    users = User.init_users(num=args.num)
    run_process([(main, (user,), user.username) for user in users], is_lock=True)
