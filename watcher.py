import sys
import time

# from apscheduler.schedulers.blocking import BlockingScheduler as Scheduler
from wampyapp import State, WatcherClient, WatcherMasterClient
from huobi.model.market.trade_detail_event import TradeDetailEvent
from huobi.model.market.trade_detail import TradeDetail
from retry import retry

from market import MarketClient
from utils import config, kill_all_threads, logger
from websocket_handler import replace_watch_dog, WatchDog

BOOT_RATE = config.getfloat('setting', 'BootRate')
END_RATE = config.getfloat('setting', 'EndRate')
MIN_VOL = config.getfloat('setting', 'MinVol')
WATCHER_STOP = config.getfint('setting', 'WatcherStop')
MAX_BUY_WAIT = config.getfloat('setting', 'MaxBuyWait')
MAX_BUY_BACK_RATE = config.getfloat('setting', 'MaxBuyBackRate')

WATCHER_TASK_NUM = config.getint('setting', 'WatcherTaskNum')
WATCHER_SLEEP = config.getint('setting', 'WatcherSleep')

def check_buy_signal(client: WatcherClient, symbol, vol, open_, price, now, boot_price, end_price, start_time, max_back):
    if vol < MIN_VOL or max_back > MAX_BUY_BACK_RATE:
        return

    if boot_price < price < end_price:
        try:
            client.send_buy_signal(symbol, price, open_, now, vol, start_time)
        except Exception as e:
            logger.error(e)

def check_sell_signal(client: WatcherClient, symbol, vol, open_, close, now, start_time, back):
    target = client.targets[symbol]

    if back < 0.001 and now < target.sell_least_time:
        try:
            client.send_delay_sell(symbol, close, back, now)
        except Exception as e:
            logger.error(e)

    if not target.own:
        return

    if close > target.high_price:
        try:
            client.send_high_sell_signal(symbol, start_time)
        except Exception as e:
            logger.error(e)

    elif close < target.sell_least_price:
        try:
            client.send_sell_signal(symbol, close, open_, now, vol, start_time)
        except Exception as e:
            logger.error(e)
    
    

def trade_detail_callback(symbol: str, client: WatcherClient, interval=300, redis=True):
    def warpper(event: TradeDetailEvent):
        if not event.data:
            return
        
        start_time = time.time()
        detail: TradeDetail = event.data[0]
        now = detail.ts / 1000

        if client.state == State.RUNNING and 0 < now - client.target_time < WATCHER_STOP:
            last = now // interval
            price = detail.price
            vol = sum([each.price * each.amount for each in event.data])

            if last > info['last']:
                info['last'] = last
                info['open_'] = event.data[-1].price
                info['vol'] = vol
                info['boot_price'] = info['open_'] * (1 + BOOT_RATE / 100)
                info['end_price'] = info['open_'] * (1 + END_RATE / 100)
                info['high'] = max(info['open_'], price)
            else:
                info['vol'] += vol
                info['high'] = max(info['high'], price)
                info['back'] = 1 - price / info['high']
                info['max_back'] = max(info['max_back'], info['back'])

            if symbol in client.targets:
                check_sell_signal(client, symbol, info['vol'], info['open_'], price, now, start_time, info['back'])

            elif now - client.target_time < MAX_BUY_WAIT:
                check_buy_signal(
                    client, symbol, info['vol'], info['open_'],
                    price, now, info['boot_price'], info['end_price'],
                    start_time, info['max_back']
                )

        if redis:
            client.redis_conn.write_trade(symbol, event.data)


    info = {
        'last': 0,
        'vol': 0,
        'open_': 0,
        'high': 0,
        'boot_price': 0,
        'end_price': 0,
        'max_back': 0,
        'back': 0
    }
    return warpper

def error_callback(symbol):
    def warpper(error):
        logger.error(f'[{symbol}] {error}')
    
    return warpper

def update_symbols(client: WatcherClient, watch_dog: WatchDog):
    new_symbols, _ = client.market_client.update_symbols_info()
    if new_symbols:
        logger.info(f'Find new symbols: {", ".join(new_symbols)}')
        for i, symbol in enumerate(new_symbols):
            client.market_client.sub_trade_detail(
                symbol, trade_detail_callback(symbol, client), error_callback(symbol)
            )
            watch_dog.after_connection_created(symbol)
            if not i % 10:
                time.sleep(0.5)

@retry(tries=5, delay=1, logger=logger)
def init_watcher(Client=WatcherClient) -> WatcherClient:
    market_client = MarketClient()
    client = Client(market_client)
    client.start()
    return client

def main():
    is_master = len(sys.argv) > 1 and sys.argv[1] == 'master'
    is_wait_stop = len(sys.argv) <= 1 or sys.argv[1] != 'nowait'
    watch_dog = replace_watch_dog()

    if is_master:
        logger.info('Master watcher')
        client : WatcherMasterClient = init_watcher(WatcherMasterClient)
        client.get_task(WATCHER_TASK_NUM)
        watch_dog.scheduler.add_job(client.running, trigger='cron', hour=23, minute=59, second=30)
        watch_dog.scheduler.add_job(client.stop_running, trigger='cron', hour=0, minute=int(WATCHER_STOP/60), second=0)
        watch_dog.scheduler.add_job(client.stopping, trigger='cron', hour=23, minute=56, second=0)
        watch_dog.scheduler.add_job(update_symbols, trigger='cron', minute='*/5', kwargs={'client': client, 'watch_dog': watch_dog})
        client.starting()
    else:
        logger.info('Sub watcher')
        client : WatcherClient = init_watcher(WatcherClient)
        client.wait_state(State.STARTED)
        client.get_task(WATCHER_TASK_NUM)

    if not client.task:
        return

    logger.info(f'Watcher task are: {", ".join(client.task)}')
    for i, symbol in enumerate(client.task):
        client.market_client.sub_trade_detail(
            symbol, trade_detail_callback(symbol, client, redis=is_wait_stop), error_callback
        )
        watch_dog.after_connection_created(symbol)
        if not i % 10:
            time.sleep(0.5)

    client.wait_state(State.RUNNING)
    client.wait_state(State.STARTED)
    if is_wait_stop:
        client.wait_state(State.STOPPED)

    client.stop()
    kill_all_threads()
    logger.info('Watcher stop')

if __name__ == '__main__':
    main()
