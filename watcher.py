import sys
import time

from huobi.constant.definition import CandlestickInterval

from market import MarketClient
from utils import config, get_target_time, kill_all_threads, logger

from wampyapp import WatcherClient, WatcherMasterClient
from retry import retry

BOOT_PERCENT = config.getfloat('setting', 'BootPercent')
END_PERCENT = config.getfloat('setting', 'EndPercent')
MIN_VOL = config.getfloat('setting', 'MinVol')
UNSTOP_MAX_WAIT = config.getfloat('setting', 'UnstopMaxWait')

WATCHER_TASK_NUM = config.getint('setting', 'WatcherTaskNum')
WATCHER_SLEEP = config.getint('setting', 'WatcherSleep')

def check_buy_signal(client, symbol, kline):
    now = kline.ts / 1000
    if now < client.target_time or now > client.target_time + UNSTOP_MAX_WAIT:
        return

    vol = kline.tick.vol
    if vol < MIN_VOL:
        return

    close = kline.tick.close
    open_ = kline.tick.open
    increase = round((close - open_) / open_ * 100, 4)
    if increase < BOOT_PERCENT or increase > END_PERCENT:
        return

    try:
        client.send_buy_signal(symbol, close, open_, now)
    except Exception as e:
        logger.error(e)

def check_sell_signal(client, symbol, kline):
    target = client.market_client.targets[symbol]
    if not target.own:
        return

    now = kline.ts / 1000
    if now < target.sell_least_time:
        return
    
    close = kline.tick.close
    open_ = kline.tick.open
    if close > target.sell_least_price:
        return

    try:
        client.send_sell_signal(symbol, close, open_, now)
    except Exception as e:
        logger.error(e)

def kline_callback(symbol, client):
    def warpper(kline):
        if not client.run:
            return

        if symbol in client.market_client.targets.keys():
            check_sell_signal(client, symbol, kline)
        else:
            check_buy_signal(client, symbol, kline)
    return warpper

def error_callback(error):
    print('?')
    logger.error(error)

@retry(tries=5, delay=1, logger=logger)
def init_watcher(Client):
    market_client = MarketClient()
    client = Client(market_client)
    client.start()
    return client

def main():
    WATCHER_MODE = sys.argv[1]
    if WATCHER_MODE == 'master':
        logger.info('Master watcher')
        Client = WatcherMasterClient

    elif WATCHER_MODE == 'sub':
        logger.info('Sub watcher')
        Client = WatcherClient

    client = init_watcher(Client)
    client.wait_to_run()

    task = client.get_task(WATCHER_TASK_NUM)
    if not task:
        return

    logger.info(f'Watcher task is:\n{", ".join(task)}')
    for symbol in task:
        client.market_client.sub_candlestick(
            symbol, CandlestickInterval.DAY1,
            kline_callback(symbol, client), error_callback
        )

    logger.info(f"Watcher stop after {WATCHER_SLEEP}s")
    time.sleep(WATCHER_SLEEP)

    client.stop()
    kill_all_threads()
    logger.info('Watcher stop')

if __name__ == '__main__':
    main()
