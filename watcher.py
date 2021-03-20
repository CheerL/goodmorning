import errno
import sys
import socket
import time

from huobi.constant.definition import CandlestickInterval

from market import MarketClient
from parallel import run_thread
from rpc_generator import get_watcher_client, get_watcher_server, get_dealer_clients, keep_alive, TClient, Error
from utils import config, get_target_time, kill_all_threads, logger
from target import Target


BOOT_PERCENT = config.getfloat('setting', 'BootPercent')
END_PERCENT = config.getfloat('setting', 'EndPercent')
MIN_VOL = config.getfloat('setting', 'MinVol')
UNSTOP_MAX_WAIT = config.getfloat('setting', 'UnstopMaxWait')

WATCHER_TASK_NUM = config.getint('setting', 'WatcherTaskNum')
WATCHER_MASTER_SLEEP = config.getint('setting', 'WatcherMasterSleep')

market_client = MarketClient()


class WatcherHandler:
    def __init__(self, target_time, dealers):
        self.target_time = target_time
        self.dealers = dealers
        self.symbols = {}

    def buy_signal(self, symbol, price, init_price):
        # print(symbol, price, 'buy')
        # run_thread([(dealer.buy_signal, (symbol, price, init_price, )) for dealer in self.dealers], is_lock=True)
        for dealer in self.dealers:
            dealer.buy_signal(symbol, price, init_price)

    def sell_signal(self, symbol, price, init_price):
        # print(symbol, price, 'sell')
        for dealer in self.dealers:
            dealer.sell_signal(symbol, price, init_price)

    def get_task(self, num):
        task = self.symbols[:num]
        self.symbols = self.symbols[num:]
        return task

    def alive(self):
        return 'alive'


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
        print(symbol, close, 'buy')
        client.buy_signal(symbol, close, open_)
        market_client.targets[symbol] = Target(symbol, close, open_, now)
    except Error:
        client.close()

def check_sell_signal(client, symbol, kline):
    target = market_client.targets[symbol]
    now = time.time()
    if now < target.sell_least_time:
        return
    
    close = kline.tick.close
    open_ = kline.tick.open
    if close > target.sell_least_price:
        return

    try:
        print(symbol, close, 'sell')
        client.sell_signal(symbol, close, open_)
        del market_client.targets[symbol]
    except Error:
        client.close()

def kline_callback(symbol, client):
    def warpper(kline):
        if symbol in market_client.targets.keys():
            check_sell_signal(client, symbol, kline)
        else:
            check_buy_signal(client, symbol, kline)
    return warpper

def error_callback(error):
    logger.error(error, '?')


def main():
    WATCHER_MODE = sys.argv[1]
    if WATCHER_MODE == 'master':
        logger.info('Master watcher')
        target_time = get_target_time()
        dealers = get_dealer_clients()
        for dealer in dealers:
            keep_alive(dealer, is_lock=False)

        handler = WatcherHandler(target_time, dealers)
        
        server = get_watcher_server(handler)
        run_thread([(server.serve, ())], is_lock=False)

        price = market_client.get_price()
        market_client.exclude_expensive(price)
        handler.symbols = sorted(
            market_client.symbols_info.keys(), key=lambda s: price[s])
        client = handler

    elif WATCHER_MODE == 'sub':
        logger.info('Sub watcher')
        client = get_watcher_client()
        client.target_time = get_target_time()
    else:
        pass

    task = client.get_task(WATCHER_TASK_NUM)
    if not task:
        return

    logger.info(f'Watcher task is:\n{", ".join(task)}')
    for symbol in task:
        market_client.sub_candlestick(
            symbol, CandlestickInterval.MIN5, kline_callback(symbol, client), error_callback)

    if WATCHER_MODE == 'master':
        logger.info(f"Master watcher stop after {WATCHER_MASTER_SLEEP}s")
        time.sleep(WATCHER_MASTER_SLEEP)
        logger.info('Master watcher stop')
        # server.trans.close()
        server.close()


    elif WATCHER_MODE == 'sub':
        while True:
            try:
                client.alive()
                time.sleep(1)
            except Error:
                print('lose connection')
                client.close()
                break
        # keep_alive(client)

        logger.info(f'Sub watcher stop')

    kill_all_threads()


if __name__ == '__main__':
    main()
