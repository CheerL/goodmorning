import time
import errno
from rpc_generator import get_watcher_client, get_watcher_server 

from market import BOOT_PERCENT, MIN_VOL, MarketClient
from utils import config, get_target_time, logger, kill_all_threads
from parallel import run_thread
from huobi.constant.definition import CandlestickInterval


BOOT_PERCENT = config.getfloat('setting', 'BootPercent')
END_PERCENT = config.getfloat('setting', 'EndPercent')
MIN_VOL = config.getfloat('setting', 'MinVol')

WATCHER_MODE = config.get('setting', 'WatcherMode')
WATCHER_TASK_NUM = config.getint('setting', 'WatcherTaskNum')
WATCHER_MASTER_SLEEP = config.getint('setting', 'WatcherMasterSleep')

market_client = MarketClient()
class WatcherHandler:
    def __init__(self, target_time):
        self.target_time = target_time
        self.symbols = []

    def buy_signal(self, symbol, price):
        print(symbol, price)

    def get_task(self, num):
        task = self.symbols[:num]
        self.symbols = self.symbols[num:]
        return task

    def alive(self):
        return 'alive'

def error_callback(error):
    logger.error(error)

def kline_callback(symbol, client):
    def warpper(kline):
        if symbol in market_client.target_symbol:
            return

        close = kline.tick.close
        open = kline.tick.open
        vol = kline.tick.vol
        increase = round((close - open) / open * 100, 4)
        if END_PERCENT > increase > BOOT_PERCENT and vol > MIN_VOL:
            try:
                client.buy_signal(symbol, close)
                market_client.target_symbol.append(symbol)
            except IOError as e:
                if e.errno == errno.EPIPE:
                    pass
    return warpper


def main():
    if WATCHER_MODE == 'master':
        logger.info('Master watcher')
        target_time = get_target_time()
        handler = WatcherHandler(target_time)
        client = handler
        server = get_watcher_server(handler)
        run_thread([(server.serve, ())], is_lock=False)

        price = market_client.get_price()
        market_client.exclude_expensive(price)
        handler.symbols = sorted(market_client.symbols_info.keys(), key=lambda s:price[s])
        
        task = handler.get_task(WATCHER_TASK_NUM)
        
    elif WATCHER_MODE == 'sub':
        logger.info('Sub watcher')
        client = get_watcher_client()
        task = client.get_task(WATCHER_TASK_NUM)
        if not task:
            return
    else:
        pass
    
    logger.info(f'Watcher task is {",".join(task)}')
    for symbol in task:
        market_client.sub_candlestick(symbol, CandlestickInterval.MIN5, kline_callback(symbol, client), error_callback)
    
    if WATCHER_MODE == 'master':
        logger.info(f"Master watcher stop after {WATCHER_MASTER_SLEEP}s")
        time.sleep(WATCHER_MASTER_SLEEP)
        logger.info('Master watcher stop')
        kill_all_threads()
        return

    elif WATCHER_MODE == 'sub':
        while True:
            try:
                assert client.alive() == 'alive'
                time.sleep(1)
            except IOError as e:
                if e.errno == errno.EPIPE:
                    break

        logger.info(f'Sub watcher stop')
        kill_all_threads()



if __name__ == '__main__':
    main()
