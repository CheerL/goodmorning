import sys
import time


from wampyapp import WatcherClient, WatcherMasterClient, State
from huobi.model.market.trade_detail_event import TradeDetailEvent

from market import MarketClient
from utils import config, kill_all_threads, logger
from record import write_redis
from retry import retry
from websocket_handler import replace_watch_dog
from apscheduler.schedulers.blocking import BlockingScheduler as Scheduler


BOOT_RATE = config.getfloat('setting', 'BootRate')
END_RATE = config.getfloat('setting', 'EndRate')
MIN_VOL = config.getfloat('setting', 'MinVol')
SELL_AFTER = config.getfloat('setting', 'SellAfter')
MAX_WAIT = config.getfloat('setting', 'MaxWait')

WATCHER_TASK_NUM = config.getint('setting', 'WatcherTaskNum')
WATCHER_SLEEP = config.getint('setting', 'WatcherSleep')

def check_buy_signal(client: WatcherClient, symbol, vol, open_, price, now, boot_price, end_price):
    if vol < MIN_VOL:
        return

    if boot_price < price < end_price:
        try:
            client.send_buy_signal(symbol, price, open_, now, vol)
        except Exception as e:
            logger.error(e)

def check_sell_signal(client: WatcherClient, symbol, vol, open_, close, now):
    target = client.targets[symbol]
    if not target.own:
        return

    if close > target.high_price:
        try:
            client.send_high_sell_signal(symbol)
        except Exception as e:
            logger.error(e)

    if close < target.sell_least_price and now > target.sell_least_time:
        try:
            client.send_sell_signal(symbol, close, open_, now, vol)
        except Exception as e:
            logger.error(e)

def trade_detail_callback(symbol: str, client: WatcherClient, interval=300):
    def warpper(event: TradeDetailEvent):
        if client.state == State.RUNNING and 0 < event.ts / 1000 - client.target_time < MAX_WAIT:
            detail = event.data[0]
            now = detail.ts / 1000
            last = now // interval
            price = detail.price
            vol = sum([each.price * each.amount for each in event.data])

            if last > info['last']:
                info['last'] = last
                info['open_'] = event.data[-1].price
                info['vol'] = vol
                info['boot_price'] = info['open_'] * (1 + BOOT_RATE / 100)
                info['end_price'] = info['open_'] * (1 + END_RATE / 100)
            else:
                info['vol'] += vol
                
            if symbol in client.targets:
                check_sell_signal(client, symbol, info['vol'], info['open_'], price, now)

            elif now < client.target_time + SELL_AFTER:
                check_buy_signal(client, symbol, info['vol'], info['open_'], price, now, info['boot_price'], info['end_price'])

        write_redis(symbol, event.data)


    info = {
        'last': 0,
        'vol': 0,
        'open_': 0,
        'high': 0,
        'boot_price': 0,
        'end_price': 0
    }
    return warpper

def error_callback(error):
    logger.error(error)

@retry(tries=5, delay=1, logger=logger)
def init_watcher(Client=WatcherClient) -> WatcherClient:
    market_client = MarketClient()
    client = Client(market_client)
    client.start()
    return client

def main():
    is_master = len(sys.argv) > 1 and sys.argv[1] == 'master'
    
    if is_master:
        logger.info('Master watcher')
        client = init_watcher(WatcherMasterClient)
        client.get_task(WATCHER_TASK_NUM)
        scheduler = Scheduler()
        scheduler.add_job(lambda _: client.running(), trigger='cron', hour=23, minute=59, second=30)
        scheduler.add_job(lambda _: client.stopping(), trigger='cron', hour=23, minute=55, second=0)
        scheduler.add_job(lambda _: client.stop_running(), trigger='cron', hour=0, minute=0, second=SELL_AFTER)
        client.starting()
    else:
        logger.info('Sub watcher')
        client = init_watcher(WatcherClient)
        client.wait_state(State.STARTED)
        client.get_task(WATCHER_TASK_NUM)

    if not client.task:
        return

    watch_dog = replace_watch_dog()
    logger.info(f'Watcher task are: {", ".join(client.task)}')
    for i, symbol in enumerate(client.task):
        client.market_client.sub_trade_detail(
            symbol, trade_detail_callback(symbol, client), error_callback
        )
        watch_dog.after_connection_created(symbol)
        if not i % 10:
            time.sleep(0.5)

    client.wait_state(State.STOPPED)
    client.stop()
    kill_all_threads()
    logger.info('Watcher stop')
    # scp_targets(client.targets.keys(), client.target_time)

if __name__ == '__main__':
    main()
