import time

from wampyapp import DealerClient as Client, State
from utils.parallel import run_process
from utils import config, kill_all_threads, logger, user_config
from apscheduler.schedulers.gevent import GeventScheduler as Scheduler
from market import MarketClient
from retry import retry
from user import User
from huobi.model.trade.order_update_event import OrderUpdateEvent, OrderUpdate

LOW_STOP_PROFIT_TIME = int(config.getfloat('time', 'LOW_STOP_PROFIT_TIME'))
STOP_PROFIT_SLEEP = int(config.getint('time', 'STOP_PROFIT_SLEEP'))



def trade_update_callback(client: Client):
    def warpper(event: OrderUpdateEvent):
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
                        break

            elif etype == 'cancellation':
                for summary in client.user.orders[direction][symbol]:
                    if summary.order_id == order_id:
                        summary.cancel_update(update)
                        if summary.cancel_callback:
                            summary.cancel_callback(*summary.cancel_callback_args)
                        break
        except Exception as e:
            logger.error(e)

    return warpper

def error_callback(symbol):
    def warpper(error):
        logger.error(f'[{symbol}] {error}')
    
    return warpper

@retry(tries=5, delay=1, logger=logger)
def init_users() -> 'list[User]':
    ACCESSKEY = user_config.get('setting', 'AccessKey')
    SECRETKEY = user_config.get('setting', 'SecretKey')
    BUY_AMOUNT = user_config.get('setting', 'BuyAmount')
    WXUIDS = user_config.get('setting', 'WxUid')
    TEST = user_config.getboolean('setting', 'Test')

    access_keys = [key.strip() for key in ACCESSKEY.split(',')]
    secret_keys = [key.strip() for key in SECRETKEY.split(',')]
    buy_amounts = [amount.strip() for amount in BUY_AMOUNT.split(',')]
    wxuids = [uid.strip() for uid in WXUIDS.split(',')]

    users = [User(*user_data) for user_data in zip(access_keys, secret_keys, buy_amounts, wxuids)]
                                                   
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
    logger.info('Start run sub process')
    client = init_dealer(user)

    scheduler = Scheduler()
    scheduler_time = LOW_STOP_PROFIT_TIME - STOP_PROFIT_SLEEP
    scheduler.add_job(client.stop_profit_handler, args=['', 0], trigger='cron', hour=0, minute=0, second=scheduler_time)
    scheduler.start()
    client.user.start(trade_update_callback(client), error_callback('order'))

    client.wait_state(State.RUNNING)
    client.wait_state(State.STARTED)
    client.stop()
    logger.info('Time to cancel')
    for target in client.targets.values():
        user.cancel_and_sell(target)
    time.sleep(2)
    user.report()
    kill_all_threads()


if __name__ == '__main__':
    logger.info('Dealer')
    users = init_users()
    run_process([(main, (user,), user.username) for user in users], is_lock=True)
