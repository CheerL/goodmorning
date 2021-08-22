import time
import argparse

from wampyapp import DealerClient as Client, State
from utils.parallel import run_process, run_thread
from utils import config, kill_all_threads, logger, user_config
from apscheduler.schedulers.gevent import GeventScheduler as Scheduler
from market import MarketClient
from retry import retry
from user import User
from huobi.model.trade.order_update_event import OrderUpdateEvent, OrderUpdate
from target import Target

LOW_STOP_PROFIT_TIME = int(config.getfloat('time', 'LOW_STOP_PROFIT_TIME'))
FINAL_STOP_PROFIT_TIME = int(config.getfloat('time', 'FINAL_STOP_PROFIT_TIME'))
CHECK_SELL_TIME = int(config.getint('time', 'CHECK_SELL_TIME'))
CLEAR_TIME = int(config.getint('time', 'CLEAR_TIME'))



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
                    for summary in client.user.orders[direction][symbol]:
                        if summary.order_id == None:
                            summary.create(update)
                            break

                elif etype == 'trade':
                    for summary in client.user.orders[direction][symbol]:
                        if summary.order_id == order_id:
                            summary.update(update)
                            if update.orderStatus == 'filled' and summary.filled_callback:
                                summary.filled_callback(*summary.filled_callback_args)
                                if symbol not in client.targets:
                                    target = Target(
                                        symbol, summary.aver_price, time.time(), client.high_stop_profit
                                    )
                                    target.set_info(client.market_client.symbols_info[symbol])
                                    target.set_buy_price(summary.aver_price)
                                    client.targets[symbol] = target
                            break

                elif etype == 'cancellation':
                    for summary in client.user.orders[direction][symbol]:
                        if summary.order_id == order_id:
                            summary.cancel_update(update)
                            if summary.cancel_callback:
                                summary.cancel_callback(*summary.cancel_callback_args)
                            break
            except Exception as e:
                logger.error(f"{direction} {etype} | {client.user.orders[direction].keys()} | Error: {type(e)} {e}")
                raise e

        try:
            _warpper(event)
        except Exception as e:
            logger.error(f"max tries | {type(e)} {e}")

    return warpper

def error_callback(symbol):
    def warpper(error):
        logger.error(f'[{symbol}] {error}')
    
    return warpper

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

def main(user: User):
    try:
        logger.info('Start run sub process')
        client = init_dealer(user)

        scheduler = Scheduler()
        scheduler.add_job(client.stop_profit_handler, args=['', 0], trigger='cron', hour=0, minute=0, second=LOW_STOP_PROFIT_TIME)
        scheduler.add_job(client.check_and_sell, args=[True], trigger='cron', hour=0, minute=0, second=CHECK_SELL_TIME)
        scheduler.add_job(client.sell_in_buy_price, args=[], trigger='cron', hour=0, minute=0, second=FINAL_STOP_PROFIT_TIME)
        scheduler.add_job(client.state_handler, args=[1], trigger='cron', hour=0, minute=0, second=CLEAR_TIME + 2)
        scheduler.start()
        client.user.start(trade_update_callback(client), error_callback('order'))

        client.wait_state(State.RUNNING)
        client.user.set_start_asset()
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
    users = init_users(num=args.num)
    run_process([(main, (user,), user.username) for user in users], is_lock=True)
