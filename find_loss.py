
import argparse

from user.huobi import HuobiUser
from user.binance import BinanceUser
from utils import config, kill_all_threads, logger, datetime, user_config
from client.loss import LossDealerClient as DClient, LossWatcherClient as WClient

PRICE_INTERVAL = config.getfloat('loss', 'PRICE_INTERVAL')
EXCHANGE = user_config.get('setting', 'Exchange')

def main(user, args):
    logger.name = user.username
    user.start()
    client = DClient(user)
    client.resume()
    
    if args.update_asset:
        client.update_asset(args.update_asset)
        kill_all_threads()
        return

    if args.manual_target:
        client.find_targets(end=args.manual_end)
        kill_all_threads()
        return

    user.scheduler.add_job(client.buy_targets, 'cron', hour=23, minute=59, second=10)
    user.scheduler.add_job(client.update_targets, 'cron', hour=0, minute=0, second=10)
    user.scheduler.add_job(client.update_asset, 'cron', hour=0, minute=1, second=0)
    user.scheduler.add_job(client.sell_targets, 'cron', hour=23, minute=57, second=0)
    user.scheduler.add_job(client.watch_targets, 'interval', seconds=PRICE_INTERVAL)

    client.report_scheduler.add_job(client.report, 'cron', minute='*/5', second=0, kwargs={'force': False})
    client.report_scheduler.add_job(client.report, 'cron', hour='0,8,12,16,20', minute=2, kwargs={'force': True})

    if args.manual_buy:
        targets = client.targets.setdefault(args.manual_date, {})
        if args.manual_symbol not in targets:
            end = int((datetime.date2ts() - datetime.date2ts(args.manual_date))/86400)
            targets[args.manual_symbol] = client.find_targets([args.manual_symbol], end, force=True)
        target = targets[args.manual_symbol]
        client.buy_target(target, args.manual_price, args.manual_amount, limit_rate=0)

    if args.manual_sell:
        target = client.targets[args.manual_date][args.manual_symbol]
        client.sell_target(target, args.manual_price, args.manual_amount, 10)
    
    client.report(True, args.report)
    client.wait_state(10)

    kill_all_threads()

def watcher(user):
    user.start()
    client = WClient(user)
    
    user.scheduler.add_job(client.get_targets, 'cron', second='*/30', max_instances=1)
    user.scheduler.add_job(client.update_target_price, 'interval', seconds=PRICE_INTERVAL, max_instances=2)
    
    client.wait_state(10)
    kill_all_threads()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--type', default='', type=str)
    parser.add_argument('-n', '--num', default=0, type=int)
    parser.add_argument('-r', '--report', action='store_true', default=False)
    parser.add_argument('--watcher', action='store_true', default=False)
    parser.add_argument('--manual_sell', action='store_true', default=False)
    parser.add_argument('--manual_buy', action='store_true', default=False)
    parser.add_argument('--manual_date', default='')
    parser.add_argument('--manual_symbol', default='')
    parser.add_argument('--manual_price', default=0, type=float)
    parser.add_argument('--manual_amount', default=0, type=float)
    parser.add_argument('--manual_target', action='store_true', default=False)
    parser.add_argument('--manual_end', default=0, type=int)
    parser.add_argument('--update_asset', default=0, type=int)

    args = parser.parse_args()

    User_dict = {User.user_type: User for User in [BinanceUser, HuobiUser]}

    if args.type and args.type in User_dict:
        exchange = args.type
    elif EXCHANGE in User_dict:
        exchange = EXCHANGE
    else:
        exchange = list(User_dict.keys())[0]

    [user] = User_dict[exchange].init_users(num=args.num)

    if args.watcher:
        watcher(user)
    else:
        main(user, args)
