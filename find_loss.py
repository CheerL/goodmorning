
import argparse
import time

from retry.api import retry
from threading import Timer
from target import LossTarget as Target
from utils import config, kill_all_threads, logger
from utils.parallel import run_process
from utils.datetime import date2ts, ts2date
from user import LossUser  as User
from client.dealer import LossDealerClient as Client
from apscheduler.schedulers.gevent import GeventScheduler as Scheduler
from order import OrderSummaryStatus
from dataset.pgsql import Order as OrderSQL
from websocket_handler import replace_watch_dog, WatchDog

SELL_UP_RATE = config.getfloat('loss', 'SELL_UP_RATE')
MAX_DAY = config.getint('loss', 'MAX_DAY')
PRICE_INTERVAL = config.getfloat('loss', 'PRICE_INTERVAL')

def main(user: User):
    @retry(tries=10, delay=1, logger=logger)
    def sell_targets(date=None):
        date = date or client.date
        clear_date = ts2date(date2ts(date) - MAX_DAY * 86400)
        clear_targets = client.targets.get(clear_date, {})
        tickers = client.market_client.get_market_tickers()
        
        for ticker in tickers:
            symbol = ticker.symbol
            if symbol in clear_targets:
                target = clear_targets[symbol]
                sell_price = ticker.close*(1-SELL_UP_RATE)
                if target.own_amount * sell_price > 5:
                    client.cancel_and_sell_limit_target(target, sell_price, 6)

        for target in client.targets.get(date, {}).values():
            sell_price = max(target.sell_price, target.now_price*(1-SELL_UP_RATE))
            if target.own_amount * target.price > 5:
                client.cancel_and_sell_limit_target(target, sell_price, 4)
                Timer(
                    60, client.cancel_and_sell_limit_target,
                    args=[target, target.sell_price, 5]
                ).start()

    def set_targets(end=0):
        targets, date = client.find_targets(end=end)
        targets = client.user.filter_targets(targets)
        client.targets[date] = targets
        client.date = max(client.targets.keys())

        for target in client.targets[client.date].values():
            client.buy_limit_target(target)

    def update_targets(end=1):
        date = ts2date(time.time()-end*86400)
        symbols = client.targets[date].keys()
        targets, _ = client.find_targets(symbols=symbols, end=end)
        for symbol, target in targets.items():
            now_target = client.targets[date][symbol]
            now_target.set_init_price(target.init_price)
            client.cancel_and_buy_limit_target(now_target, target.init_price)

    watch_dog = replace_watch_dog()
    user.start(watch_dog)
    client = Client.init_dealer(user)
    scheduler = watch_dog.scheduler
    scheduler.add_job(set_targets, trigger='cron', hour=23, minute=59, second=0)
    scheduler.add_job(update_targets, trigger='cron', hour=0, minute=0, second=10)
    scheduler.add_job(sell_targets, trigger='cron', hour=23, minute=57, second=0)
    scheduler.add_job(client.report, trigger='cron', hour='0,8-23', second=0, kwargs={'force': False})
    scheduler.add_job(client.report, trigger='cron', hour='0,8,12,16,20', minute=30, kwargs={'force': True})
    scheduler.add_job(client.watch_targets, 'interval', seconds=PRICE_INTERVAL)

    client.resume()
    for summary in client.user.orders.copy().values():
        print(summary.order_id, summary.symbol, summary.label, summary.vol, summary.aver_price, summary.status)

    for target in client.targets.get(client.date, {}).values():
        print(target.symbol, target.date, target.own_amount, target.buy_price, target.buy_price * target.own_amount)

    # client.watch_targets()
    client.wait_state(1)

    target = client.targets['2021-09-27']['zksusdt']
    client.sell_limit_target(target, target.sell_price)

    kill_all_threads()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num', default=-1, type=int)
    args = parser.parse_args()

    logger.info('Dealer')
    users = User.init_users(num=args.num)
    run_process([(main, (user,), user.username) for user in users], is_lock=True)
