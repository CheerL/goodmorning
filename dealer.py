import time
import datetime
import argparse

from wampyapp import DealerClient as Client, State
from utils import config, kill_all_threads, logger, user_config
from apscheduler.schedulers.gevent import GeventScheduler as Scheduler
from market import MarketClient
from retry import retry
from user import User
from huobi.model.trade.order_update_event import OrderUpdateEvent, OrderUpdate
from websocket_handler import replace_watch_dog, WatchDog
from order import OrderSummaryStatus

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
                                summary.filled_callback = None
                            break

                elif etype == 'cancellation':
                    for summary in client.user.orders[direction][symbol]:
                        if summary.order_id == order_id:
                            summary.cancel_update(update)
                            if summary.cancel_callback:
                                summary.cancel_callback(*summary.cancel_callback_args)
                                summary.cancel_callback = None
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

# def print_order(self: Order) -> str:
#     print(f'<Order id={self.id}, symbol={self.symbol}, status={self.state}, type={self.type}, amount={self.filled_amount}, price={self.price}, created_at={self.created_at}, finished_at={self.finished_at}>')

def check_orders(client: Client):
    for direction, orders in client.user.orders.items():
        for symbol, summary_list in orders.items():
            for summary in summary_list.copy():
                order_id = summary.order_id
                if not order_id or summary.status in [OrderSummaryStatus.FILLED, OrderSummaryStatus.CANCELED]:
                    continue
                
                try:
                    order = client.user.trade_client.get_order(order_id)
                except Exception as e:
                    logger.info(e)
                    continue

                # print(summary)
                # print_order(order)
                order.amount = float(order.amount)
                order.filled_amount = float(order.filled_amount)
                order.filled_cash_amount = float(order.filled_cash_amount)
                order.filled_fees = float(order.filled_fees)

                status = OrderSummaryStatus.map_dict[order.state]
                if order.filled_amount == 0 and summary.amount == 0:
                    summary.status = max(status, summary.status)

                elif order.amount > 0 and summary.amount / order.amount < 0.97:
                    # print('update')
                    summary.limit = 'limit' in order.type
                    summary.amount = order.filled_amount
                    summary.vol = order.filled_cash_amount
                    summary.aver_price = summary.vol / summary.amount
                    summary.fee = order.filled_fees
                    summary.status = status
                    summary.ts = (order.finished_at or order.created_at) / 1000

                    if order.type != 'buy-market':
                        summary.remain_amount = summary.created_amount - summary.amount
                    else:
                        summary.remain_amount = summary.created_vol - summary.vol
                else:
                    continue
                
                try:
                    if summary.status == OrderSummaryStatus.FILLED and summary.filled_callback:
                        # print('filled_callback')
                        summary.filled_callback(*summary.filled_callback_args)
                        summary.filled_callback = None
                    elif summary.status == OrderSummaryStatus.CANCELED and summary.cancel_callback:
                        # print('canceled_callback')
                        summary.cancel_callback(*summary.cancel_callback_args)
                        summary.cancel_callback = None
                    elif summary.status == OrderSummaryStatus.PARTIAL_FILLED and direction == 'buy' and time.time() > order.created_at / 1000 + CANCEL_BUY_TIME:
                        # print('cancel')
                        client.user.trade_client.cancel_order(symbol, order_id)
                except Exception as e:
                    logger.error(e)



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
        logger.name = user.username
        logger.info('Start run sub process')
        client = init_dealer(user)
        scheduler = Scheduler()
        client.user.start(trade_update_callback(client), error_callback(watch_dog))
        watch_dog.after_connection_created(['account', 'trade'], [None, (check_orders, (client,))])
        watch_dog.scheduler.add_job(error_ping, "interval", max_instances=1, seconds=0.5, args=[watch_dog])
        client.wait_state(State.RUNNING)
        # client.user.set_start_asset()
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
        try:
            for target in client.targets.values():
                user.cancel_and_sell(target)
        except Exception as e:
            logger.error(e)

        watch_dog.close_and_wait_reconnect(watch_dog.websocket_manage_dict['account'])
        time.sleep(0.5)
        watch_dog.close_and_wait_reconnect(watch_dog.websocket_manage_dict['trade'])
        time.sleep(1.5)
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
    [user] = init_users(num=args.num)
    main(user, watch_dog)
