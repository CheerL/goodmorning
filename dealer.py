import time
import datetime
import argparse

from client.wampy.dealer import MorningDealerClient as Client, State
from utils.parallel import run_process
from utils import config, kill_all_threads, logger, user_config
from apscheduler.schedulers.gevent import GeventScheduler as Scheduler
from user import User
from target import MorningTarget as Target

TEST = user_config.getboolean('setting', 'Test')
LOW_STOP_PROFIT_TIME = int(config.getfloat('time', 'LOW_STOP_PROFIT_TIME'))
FINAL_STOP_PROFIT_TIME = int(config.getfloat('time', 'FINAL_STOP_PROFIT_TIME'))
CHECK_SELL_TIME = int(config.getint('time', 'CHECK_SELL_TIME'))
CLEAR_TIME = int(config.getint('time', 'CLEAR_TIME'))

def main(user: User):
    try:
        logger.info('Start run sub process')
        client = Client.init_dealer(user)
        scheduler = Scheduler()
        client.user.start()
        client.wait_state(State.RUNNING)
        client.user.set_start_asset()
        if TEST:
            now = datetime.datetime.now() + datetime.timedelta(seconds=5)
            low_stop_profit_time = now + datetime.timedelta(seconds=LOW_STOP_PROFIT_TIME)
            check_sell_time = now + datetime.timedelta(seconds=CHECK_SELL_TIME)
            buy_price_sell_time = now + datetime.timedelta(seconds=FINAL_STOP_PROFIT_TIME)
            end_time = now + datetime.timedelta(seconds=CLEAR_TIME + 12)
            scheduler.add_job(client.stop_profit_handler, args=['', 0], trigger='cron', hour=low_stop_profit_time.hour, minute=low_stop_profit_time.minute, second=low_stop_profit_time.second)
            scheduler.add_job(client.check_and_sell, args=[True], trigger='cron', hour=check_sell_time.hour, minute=check_sell_time.minute, second=check_sell_time.second)
            scheduler.add_job(client.sell_in_buy_price, args=[], trigger='cron', hour=buy_price_sell_time.hour, minute=buy_price_sell_time.minute, second=buy_price_sell_time.second)
            scheduler.add_job(client.state_handler, args=[1], trigger='cron', hour=end_time.hour, minute=end_time.minute, second=end_time.second)
        else:
            scheduler.add_job(client.stop_profit_handler, args=['', 0], trigger='cron', hour=0, minute=0, second=LOW_STOP_PROFIT_TIME)
            scheduler.add_job(client.check_and_sell, args=[True], trigger='cron', hour=0, minute=0, second=CHECK_SELL_TIME)
            scheduler.add_job(client.sell_in_buy_price, args=[], trigger='cron', hour=0, minute=0, second=FINAL_STOP_PROFIT_TIME)
            scheduler.add_job(client.state_handler, args=[1], trigger='cron', hour=0, minute=0, second=CLEAR_TIME + 12)
        scheduler.start()
        #run_thread([(client.check_all_stop_profit, ())], False)
        client.wait_state(State.STARTED)
    except Exception as e:
        logger.error(e)
    finally:
        client.stop()
        logger.info('Time to cancel')
        for target in client.targets.values():
            user.cancel_and_sell(target)
        time.sleep(0.2)
        client.check_and_sell(limit=False)
        time.sleep(2)
        user.report()
        kill_all_threads()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num', default=-1, type=int)
    args = parser.parse_args()

    logger.info('Dealer')
    users = User.init_users(num=args.num)
    run_process([(main, (user,), user.username) for user in users], is_lock=True)
