import time
import datetime
import argparse
from order import OrderSummaryStatus

from wampyapp import DealerClient as Client, State
from utils.parallel import run_process
from utils import config, kill_all_threads, logger, user_config, test_config
from apscheduler.schedulers.gevent import GeventScheduler as Scheduler
from market import MarketClient
from retry import retry
from user import User
from huobi.model.trade.order_update_event import OrderUpdateEvent, OrderUpdate
from target import Target
from websocket_handler import replace_watch_dog, WatchDog

TEST = user_config.getboolean('setting', 'Test')
FINAL_STOP_PROFIT_TIME = int(config.getfloat('time', 'FINAL_STOP_PROFIT_TIME'))
CHECK_SELL_TIME = int(config.getint('time', 'CHECK_SELL_TIME'))
CLEAR_TIME = int(config.getint('time', 'CLEAR_TIME'))
CANCEL_BUY_TIME = config.getfloat('time', 'CANCEL_BUY_TIME')


def trade_update_callback(client: Client):
    def warpper(event: OrderUpdateEvent):
        @retry(tries=3, delay=0.01)
        def _warpper(event: OrderUpdateEvent):
            update: OrderUpdate = event.data
            symbol = update.symbol
            direction = 'buy' if 'buy' in update.type else 'sell'
            etype = update.eventType
            order_id = update.orderId
            if direction == 'buy':
                if order_id not in client.user.buy_id:
                    client.user.buy_id.append(order_id)
            else:
                if order_id not in client.user.sell_id:
                    client.user.sell_id.append(order_id)

            try:
                if etype == 'creation':
                    empty_pos = -1
                    for i, summary in enumerate(client.user.orders[direction][symbol]):
                        if summary.order_id == order_id:
                            summary.create(update)
                            break
                        elif summary.order_id == None and empty_pos == -1:
                            empty_pos = i
                    else:
                        summary = client.user.orders[direction][symbol][empty_pos]
                        summary.create(update)

                elif etype == 'trade':
                    for summary in client.user.orders[direction][symbol]:
                        if summary.order_id == order_id:
                            summary.update(update)
                            if update.orderStatus == 'filled' and summary.filled_callback:
                                summary.filled_callback(*summary.filled_callback_args)
                            break

                elif etype == 'cancellation':
                    for summary in client.user.orders[direction][symbol]:
                        if summary.order_id == order_id:
                            summary.cancel_update(update)
                            if summary.cancel_callback:
                                summary.cancel_callback(*summary.cancel_callback_args)
                            break
            except Exception as e:
                if not isinstance(e, KeyError):
                    logger.error(f"{direction} {etype} | {client.user.orders[direction].keys()} | Error: {type(e)} {e}")
                raise e

        try:
            _warpper(event)
        except Exception as e:
            if not isinstance(e, KeyError):
                logger.error(f"max tries | {type(e)} {e}")

    return warpper

def error_callback(watch_dog: WatchDog):
    def warpper(name):
        def inner_warpper(error):
            logger.error(f'[{name}] {error}')
            if 'Connection is already closed' in error.error_message:
                wm = watch_dog.websocket_manage_dict[name]
                watch_dog.close_and_wait_reconnect(wm)

        return inner_warpper
    return warpper

def error_ping(watch_dog: WatchDog):
    for wm in watch_dog.websocket_manage_list:
        if wm: wm.send('{"action": "test"}')

def check_orders(client: Client):
    # TODO
    for symbol, summary_list in client.user.orders['buy'].items():
        for summary in summary_list.copy():
            order_id = summary.order_id
            if not order_id:
                continue
            
            try:
                order = client.user.trade_client.get_order(order_id)
            except Exception as e:
                logger.info(e)
                continue
            
            status = OrderSummaryStatus.map_dict[order.state]
            if order.amount == 0 and summary.amount == 0 and summary.status < status:
                summary.status = status

            elif order.amount > 0 and summary.amount / order.amount < 0.97:
                summary.limit = 'limit' in order.type
                summary.amount = order.filled_amount
                summary.vol = order.filled_cash_amount
                summary.aver_price = summary.vol / summary.amount
                summary.fee = order.filled_fees
                summary.status = status
                summary.ts = (order.finished_at or order.created_at) / 1000

                if summary.limit:
                    summary.remain_amount = summary.created_amount - summary.amount
                else:
                    summary.remain_amount = summary.created_vol - summary.vol
            else:
                continue

            if summary.state == OrderSummaryStatus.FILLED and summary.filled_callback:
                summary.filled_callback(*summary.filled_callback_args)
            elif summary.state == OrderSummaryStatus.CANCELED and summary.canceled_callback:
                summary.canceled_callback(*summary.canceled_callback_args)
            elif summary.state == OrderSummaryStatus.PARTIAL_FILLED and time.time() > order.created_at / 1000 + CANCEL_BUY_TIME:
                client.user.trade_client.cancel_order(symbol, order_id)



@retry(tries=5, delay=1, logger=logger)
def init_users(num=-1) -> 'list[User]':
    ACCESSKEY = user_config.get('setting', 'AccessKey')
    SECRETKEY = user_config.get('setting', 'SecretKey')
    BUY_AMOUNT = user_config.get('setting', 'BuyAmount')
    WXUIDS = user_config.get('setting', 'WxUid')
    TEST = user_config.getboolean('setting', 'Test')

    
    access_keys = [key.strip() for key in ACCESSKEY.split(',')]
    secret_keys = [key.strip() for key in SECRETKEY.split(',')]
    buy_amounts = [amount.strip() for amount in BUY_AMOUNT.split(',')]
    wxuids = [uid.strip() for uid in WXUIDS.split(',')]
    
    if num == -1:
        users = [User(*user_data) for user_data in zip(access_keys, secret_keys, buy_amounts, wxuids)]
    else:
        users = [User(access_keys[num], secret_keys[num], buy_amounts[num], wxuids[num])]
                                                   
    if TEST:
        users = users[:1]
    return users

@retry(tries=5, delay=1, logger=logger)
def init_dealer(user) -> Client:
    market_client = MarketClient()
    client = Client(market_client, user)
    client.start()
    return client

def main(user: User, watch_dog: WatchDog):
    try:
        logger.info('Start run sub process')
        client = init_dealer(user)
        scheduler = Scheduler()
        client.user.start(trade_update_callback(client), error_callback(watch_dog))
        watch_dog.after_connection_created(['account', 'trade'], [None, (check_orders, (client,))])
        watch_dog.scheduler.add_job(error_ping, "interval", max_instances=1, seconds=0.5, args=[watch_dog])
        client.wait_state(State.RUNNING)
        client.user.set_start_asset()
        if TEST:
            now = datetime.datetime.now() + datetime.timedelta(seconds=5)
            check_sell_time = now + datetime.timedelta(seconds=CHECK_SELL_TIME)
            buy_price_sell_time = now + datetime.timedelta(seconds=FINAL_STOP_PROFIT_TIME)
            end_time = now + datetime.timedelta(seconds=CLEAR_TIME + 12)
            scheduler.add_job(client.check_and_sell, args=[True], trigger='cron', hour=check_sell_time.hour, minute=check_sell_time.minute, second=check_sell_time.second)
            scheduler.add_job(client.sell_in_buy_price, args=[], trigger='cron', hour=buy_price_sell_time.hour, minute=buy_price_sell_time.minute, second=buy_price_sell_time.second)
            scheduler.add_job(client.state_handler, args=[1], trigger='cron', hour=end_time.hour, minute=end_time.minute, second=end_time.second)
        else:
            scheduler.add_job(client.check_and_sell, args=[True], trigger='cron', hour=0, minute=0, second=CHECK_SELL_TIME)
            scheduler.add_job(client.sell_in_buy_price, args=[], trigger='cron', hour=0, minute=0, second=FINAL_STOP_PROFIT_TIME)
            scheduler.add_job(client.state_handler, args=[1], trigger='cron', hour=0, minute=0, second=CLEAR_TIME + 12)
        scheduler.start()
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
    parser.add_argument('-n', '--num', default=0, type=int)
    args = parser.parse_args()
    logger.info('Dealer')

    watch_dog = replace_watch_dog(heart_beat_limit_ms=2000, reconnect_after_ms=100)
    users = init_users(num=args.num)
    run_process([(main, (user, watch_dog), user.username) for user in users], is_lock=True)
