
import argparse
import time

from retry.api import retry
from threading import Timer
from target import LossTarget as Target
from utils import config, kill_all_threads, logger
from utils.parallel import run_process
from user import LossUser  as User
from client.dealer import LossDealerClient as Client
from apscheduler.schedulers.gevent import GeventScheduler as Scheduler
from order import OrderSummaryStatus
from dataset.pgsql import Order

def main(user: User):
    @retry(tries=10, delay=1, logger=logger)
    def final_sell_targets():
        assert client.targets, 'No targets'

        @retry(tries=5, delay=0.05)
        def cancel_callback(summary):
            target.selling = 0
            target.set_sell(summary.amount)
            client.sell_limit_target(target, target.sell_price, selling_level=3)
            Order.cancel_order(summary, client.user.account_id)

        @retry(tries=5, delay=0.05)
        def cancel(summary):
            if summary.status in [OrderSummaryStatus.CREATED, OrderSummaryStatus.PARTIAL_FILLED]:
                client.user.trade_client.cancel_order(summary.symbol, summary.order_id)

        for target in client.targets.values():
            summary = client.sell_limit_target(target, max(target.sell_price, target.price), selling_level=3)
            if summary:
                summary.add_cancel_callback(cancel_callback, [summary])
                Timer(60, cancel, [summary]).start()

        # client.targets.clear()
        # client.buy_id.clear()
        # client.sell_id.clear()

    def set_targets():
        targets = client.find_targets(end=0)
        client.targets = targets

        client.watch_targets()
        
        for target in client.targets.values():
            client.buy_limit_target(target)

    def update_targets():
        targets = client.find_targets(end=1)
        for symbol, target in targets.items():
            if symbol in client.targets:
                now_target = client.targets[symbol]
                now_target.set_init_price(target.init_price)
                client.cancel_and_buy_limit_target(now_target, target.init_price)
            else:
                client.targets[symbol] = target
                client.buy_limit_target(target, target.init_price)
        
        for symbol, target in client.targets.items():
            if symbol not in targets:
                client.cancel_and_sell_limit_target(target, target.buy_price, 3, direction='sell' if target.selling else 'buy')

    user.start()
    client = Client.init_dealer(user)
    scheduler = Scheduler()
    scheduler.add_job(set_targets, trigger='cron', hour=23, minute=59, second=0)
    scheduler.add_job(update_targets, trigger='cron', hour=0, minute=0, second=10)
    scheduler.add_job(final_sell_targets, trigger='cron', hour=23, minute=57, second=0)
    scheduler.start()
    # client.watch_targets()

    client.wait_state(10)
    kill_all_threads()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num', default=-1, type=int)
    args = parser.parse_args()

    logger.info('Dealer')
    users = User.init_users(num=args.num)
    run_process([(main, (user,), user.username) for user in users], is_lock=True)
