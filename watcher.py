import errno
import sys
import socket
import time

from huobi.constant.definition import CandlestickInterval

from market import MarketClient
# from parallel import run_thread
from utils import config, get_target_time, kill_all_threads, logger
from target import Target

from wampyapp import WatcherClient, WatcherMasterClient


BOOT_PERCENT = config.getfloat('setting', 'BootPercent')
END_PERCENT = config.getfloat('setting', 'EndPercent')
MIN_VOL = config.getfloat('setting', 'MinVol')
UNSTOP_MAX_WAIT = config.getfloat('setting', 'UnstopMaxWait')

WATCHER_TASK_NUM = config.getint('setting', 'WatcherTaskNum')
WATCHER_MASTER_SLEEP = config.getint('setting', 'WatcherMasterSleep')


def check_buy_signal(client, symbol, kline):
    vol = kline.tick.vol
    if vol < MIN_VOL:
        return

    close = kline.tick.close
    open_ = kline.tick.open
    increase = round((close - open_) / open_ * 100, 4)
    if increase < BOOT_PERCENT or increase > END_PERCENT:
        return
    
    now = time.time()
    if now < client.target_time or now > client.target_time + UNSTOP_MAX_WAIT:
        return

    try:
        logger.info(f'Buy {symbol} at {close}')
        client.send_buy_signal(symbol, close, open_)
        client.market_client.targets[symbol] = Target(symbol, close, open_, now)
    except Exception:
        client.close()

def check_sell_signal(client, symbol, kline):
    target = client.market_client.targets[symbol]
    now = time.time()
    if now < target.sell_least_time:
        return
    
    close = kline.tick.close
    open_ = kline.tick.open
    if close > target.sell_least_price:
        return

    try:
        logger.info(f'Sell {symbol} at {close}')
        client.send_sell_signal(symbol, close, open_)
        del client.market_client.targets[symbol]
    except Exception:
        client.close()

def kline_callback(symbol, client):
    def warpper(kline):
        if symbol in client.market_client.targets.keys():
            check_sell_signal(client, symbol, kline)
        else:
            check_buy_signal(client, symbol, kline)
    return warpper

def error_callback(error):
    logger.error(error)


def main():
    WATCHER_MODE = sys.argv[1]
    if WATCHER_MODE == 'master':
        logger.info('Master watcher')
        Client = WatcherMasterClient

    elif WATCHER_MODE == 'sub':
        logger.info('Sub watcher')
        Client = WatcherClient

    market_client = MarketClient()
    target_time = get_target_time()
    client = Client(market_client, target_time)
    client.start()
    task = client.get_task(WATCHER_TASK_NUM)
    if not task:
        return

    logger.info(f'Watcher task is:\n{", ".join(task)}')
    for symbol in task:
        market_client.sub_candlestick(
            symbol, CandlestickInterval.MIN5,
            kline_callback(symbol, client), error_callback
        )

    logger.info(f"Master watcher stop after {WATCHER_MASTER_SLEEP}s")
    time.sleep(WATCHER_MASTER_SLEEP)

    client.stop()
    kill_all_threads()
    logger.info('Master watcher stop')

if __name__ == '__main__':
    main()
