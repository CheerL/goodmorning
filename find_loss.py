
import argparse
import time

from user.huobi import HuobiUser  as User
from client.loss_dealer import LossDealerClient as Client

from retry import retry
from threading import Timer
from utils import config, kill_all_threads, logger
from utils.datetime import date2ts, ts2date


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
        targets = client.filter_targets(targets)
        client.targets[date] = targets
        client.date = max(client.targets.keys())

        for target in client.targets[client.date].values():
            client.buy_limit_target(target)

    def update_targets(end=1):
        def cancel(target):
            order_id_list = list(client.user.orders.keys())
            for order_id in order_id_list:
                summary = client.user.orders[order_id]
                if (summary.symbol == target.symbol and summary.order_id in client.user.buy_id 
                    and summary.label == target.date and summary.status in [1, 2]
                ):
                    try:
                        client.user.trade_client.cancel_order(summary.symbol, summary.order_id)
                        logger.info(f'Cancel open buy order for {target.symbol}')
                    except Exception as e:
                        logger.error(f'{summary.order_id} {summary.status} {summary.symbol} {e}')
                        # break

        date = ts2date(time.time()-end*86400)
        symbols = client.targets[date].keys()
        targets, _ = client.find_targets(symbols=symbols, end=end)
        for symbol, target in targets.items():
            now_target = client.targets[date][symbol]
            now_target.set_init_price(target.init_price)
            client.cancel_and_buy_limit_target(now_target, target.init_price)
            Timer(3600, cancel, args=[now_target]).start()
    
    user.start()
    client = Client.init_dealer(user)
    user.scheduler.add_job(set_targets, trigger='cron', hour=23, minute=59, second=0)
    user.scheduler.add_job(update_targets, trigger='cron', hour=0, minute=0, second=10)
    user.scheduler.add_job(sell_targets, trigger='cron', hour=23, minute=57, second=0)

    user.scheduler.add_job(client.report, trigger='cron', hour='9-11,13-15,17-19,21-23', minute=0, second=0, kwargs={'force': False})
    user.scheduler.add_job(client.report, trigger='cron', hour='0,8,12,16,20', minute=10, kwargs={'force': True})
    user.scheduler.add_job(client.watch_targets, 'interval', seconds=PRICE_INTERVAL)

    client.resume()
    logger.info('Finish loading data')
    # for summary in client.user.orders.copy().values():
    #     print(summary.order_id, summary.symbol, summary.label, summary.vol, summary.aver_price, summary.status)

    # for target in client.targets.get(client.date, {}).values():
    #     print(target.symbol, target.date, target.own_amount, target.buy_price, target.buy_price * target.own_amount)

    client.wait_state(10)

    kill_all_threads()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num', default=-1, type=int)
    args = parser.parse_args()

    logger.info('Start Loss Strategy')
    [user] = User.init_users(num=args.num)
    main(user)
