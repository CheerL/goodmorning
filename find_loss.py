
import argparse

from user.huobi import HuobiUser
from user.binance import BinanceUser
from utils import config, kill_all_threads, logger, datetime, user_config
from client.loss_dealer import LossDealerClient as Client

PRICE_INTERVAL = config.getfloat('loss', 'PRICE_INTERVAL')
EXCHANGE = user_config.get('setting', 'Exchange')

def main(user):
    user.start()
    client = Client.init_dealer(user)
    user.scheduler.add_job(client.buy_targets, 'cron', hour=23, minute=59, second=0)
    user.scheduler.add_job(client.update_targets, 'cron', hour=0, minute=0, second=10)
    user.scheduler.add_job(client.sell_targets, 'cron', hour=23, minute=57, second=0)
    user.scheduler.add_job(client.watch_targets, 'interval', seconds=PRICE_INTERVAL)

    client.report_scheduler.add_job(client.report, 'cron', hour='9-11,13-15,17-19,21-23', minute=0, second=0, kwargs={'force': False})
    client.report_scheduler.add_job(client.report, 'cron', hour='0,8,12,16,20', minute=5, kwargs={'force': True})

    client.resume()

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
