import sys
import time

from wampyapp import WatcherClient, WatcherMasterClient
from huobi.constant.definition import CandlestickInterval

from market import MarketClient
from utils import config, kill_all_threads, logger

from retry import retry

BOOT_RATE = config.getfloat('setting', 'BootRate')
END_RATE = config.getfloat('setting', 'EndRate')
MIN_VOL = config.getfloat('setting', 'MinVol')
UNSTOP_MAX_WAIT = config.getfloat('setting', 'UnstopMaxWait')

WATCHER_TASK_NUM = config.getint('setting', 'WatcherTaskNum')
WATCHER_SLEEP = config.getint('setting', 'WatcherSleep')

def check_buy_signal(client, symbol, kline):
    now = kline.ts / 1000
    if now < client.target_time + 0.1 or now > client.target_time + UNSTOP_MAX_WAIT:
        return

    vol = kline.tick.vol
    if vol < MIN_VOL:
        return

    close = kline.tick.close
    open_ = kline.tick.open
    increase = round((close - open_) / open_ * 100, 4)
    if increase < BOOT_RATE or increase > END_RATE:
        return

    try:
        client.send_buy_signal(symbol, close, open_, now)
    except Exception as e:
        logger.error(e)

# def check_sell_signal(client, symbol, kline):
#     target = client.market_client.targets[symbol]
#     if not target.own:
#         return

#     now = kline.ts / 1000
#     if now < target.sell_least_time:
#         return
    
#     close = kline.tick.close
#     open_ = kline.tick.open
#     if close > target.sell_least_price:
#         return

#     try:
#         client.send_sell_signal(symbol, close, open_, now)
#     except Exception as e:
#         logger.error(e)

def kline_callback(symbol, client):
    def warpper(kline):
        if not client.run:
            return

        if symbol in client.market_client.targets.keys():
            # check_sell_signal(client, symbol, kline)
            pass
        else:
            check_buy_signal(client, symbol, kline)
    return warpper

def error_callback(error):
    logger.error(error)

@retry(tries=5, delay=1, logger=logger)
def init_watcher(Client):
    market_client = MarketClient()
    client = Client(market_client)
    client.start()
    return client

def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'master':
        logger.info('Master watcher')
        client = init_watcher(WatcherMasterClient)
        task = client.get_task(WATCHER_TASK_NUM)
        client.wait_to_run()
    else:
        time.sleep(25)
        logger.info('Sub watcher')
        client = init_watcher(WatcherClient)
        client.wait_to_run()
        task = client.get_task(WATCHER_TASK_NUM)

    if not task:
        return

    logger.info(f'Watcher task are:\n{", ".join(task)}, {len(task)}')
    for i, symbol in enumerate(task):
        client.market_client.sub_candlestick(
            symbol, CandlestickInterval.MIN1,
            kline_callback(symbol, client), error_callback
        )
        if not i % 10:
            time.sleep(0.5)


    time.sleep(client.target_time - time.time())
    logger.info(f"Watcher stop in {WATCHER_SLEEP}s")
    time.sleep(WATCHER_SLEEP)

    client.stop()
    kill_all_threads()
    logger.info('Watcher stop')

if __name__ == '__main__':
    main()
