
import argparse
import time


from user.huobi import HuobiUser
from user.binance import BinanceUser
from utils import config, kill_all_threads, logger, datetime, user_config
from client.loss_dealer import LossDealerClient as Client

from retry import retry
from threading import Timer



SELL_UP_RATE = config.getfloat('loss', 'SELL_UP_RATE')
MAX_DAY = config.getint('loss', 'MAX_DAY')
PRICE_INTERVAL = config.getfloat('loss', 'PRICE_INTERVAL')
EXCHANGE = user_config.get('setting', 'Exchange')

def main(user):
    @retry(tries=10, delay=1, logger=logger)
    def sell_targets(date=None):
        logger.info('Start to sell')
        date = date or client.date
        clear_date = datetime.ts2date(datetime.date2ts(date) - MAX_DAY * 86400)
        clear_targets = {
            symbol: target for symbol, target in
            client.targets.get(clear_date, {}).items()
            if target.own and target.own_amount > target.sell_market_min_order_amt
        }
        clear_symbols = ",".join([
            target.symbol for target in clear_targets.values() if target.own
        ])
        tickers = client.market_client.get_market_tickers()
        logger.info(f'Clear day {clear_date}, targets are {clear_symbols}')
        
        for ticker in tickers:
            symbol = ticker.symbol
            if symbol in clear_targets:
                target = clear_targets[symbol]
                sell_price = ticker.close*(1-SELL_UP_RATE)
                client.cancel_and_sell_limit_target(target, sell_price, 6)

        logger.info(f'Sell targets of last day {date}')
        for target in client.targets.get(date, {}).values():
            if not target.own or target.own_amount < target.sell_market_min_order_amt:
                target.own = False
                continue

            sell_price = max(target.sell_price, target.now_price*(1-SELL_UP_RATE))
            client.cancel_and_sell_limit_target(target, sell_price, 4)
            Timer(
                60, client.cancel_and_sell_limit_target,
                args=[target, target.sell_price, 5]
            ).start()


    def set_targets(end=0):
        logger.info('Start to find new targets')
        targets, date = client.find_targets(end=end)
        targets = client.filter_targets(targets)
        client.targets[date] = targets
        client.date = max(client.targets.keys())

        for target in client.targets[client.date].values():
            client.buy_target(target)

    def update_targets(end=1):
        def cancel(target):
            logger.info(f'Too long time, stop to buy {target.symbol}')
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

        logger.info('Start to update today\'s targets')
        date = datetime.ts2date(time.time()-end*86400)
        symbols = client.targets[date].keys()
        targets, _ = client.find_targets(symbols=symbols, end=end)
        for symbol, target in targets.items():
            now_target = client.targets[date][symbol]
            now_target.set_init_price(target.init_price)
            client.cancel_and_buy_limit_target(now_target, target.init_price)
            Timer(3600, cancel, args=[now_target]).start()
    
    logger.info(f'Start Loss Strategy. Run {user.user_type}')
    user.start()
    user.scheduler.add_job(set_targets, trigger='cron', hour=23, minute=59, second=0)
    user.scheduler.add_job(update_targets, trigger='cron', hour=0, minute=0, second=10)
    user.scheduler.add_job(sell_targets, trigger='cron', hour=23, minute=57, second=0)

    client = Client.init_dealer(user)
    user.scheduler.add_job(client.watch_targets, 'interval', seconds=PRICE_INTERVAL)
    user.scheduler.add_job(client.report, trigger='cron', hour='9-11,13-15,17-19,21-23', minute=0, second=0, kwargs={'force': False})
    user.scheduler.add_job(client.report, trigger='cron', hour='0,8,12,16,20', minute=5, kwargs={'force': True})

    client.resume()
    logger.info('Finish loading data')

    # print(user.orders)
    # print(client.targets)
    # for summary in client.user.orders.copy().values():
    #     print(summary.order_id, summary.symbol, summary.label, summary.vol, summary.aver_price, summary.status)
    # for target in client.targets.get(client.date, {}).values():
    #     print(target.symbol, target.date, target.own_amount, target.buy_price, target.buy_price * target.own_amount)
    client.wait_state(10)

    kill_all_threads()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--type', default='', type=str)
    parser.add_argument('-n', '--num', default=0, type=int)
    args = parser.parse_args()

    User_dict = {User.user_type: User for User in [BinanceUser, HuobiUser]}

    if args.type and args.type in User_dict:
        exchange = args.type
    elif EXCHANGE in User_dict:
        exchange = EXCHANGE
    else:
        exchange = list(User_dict.keys())[0]

    [user] = User_dict[exchange].init_users(num=args.num)
    main(user)
