import sys
import time

from wampyapp import SELL_AFTER, WatcherClient, WatcherMasterClient
from huobi.constant.definition import CandlestickInterval
from huobi.model.market.candlestick_event import CandlestickEvent

from market import MarketClient
from utils import config, kill_all_threads, logger
from record import write_kline_csv, get_csv_handler, scp_targets
from retry import retry

BOOT_RATE = config.getfloat('setting', 'BootRate')
END_RATE = config.getfloat('setting', 'EndRate')
MIN_VOL = config.getfloat('setting', 'MinVol')
SELL_AFTER = config.getfloat('setting', 'SellAfter')

WATCHER_TASK_NUM = config.getint('setting', 'WatcherTaskNum')
WATCHER_SLEEP = config.getint('setting', 'WatcherSleep')

def check_buy_signal(client, symbol, kline, now):
    vol = kline.tick.vol
    if vol < MIN_VOL:
        return

    close = kline.tick.close
    open_ = kline.tick.open
    increase = round((close - open_) / open_ * 100, 4)

    if BOOT_RATE < increase < END_RATE:
        try:
            client.send_buy_signal(symbol, close, open_, now, vol)
        except Exception as e:
            logger.error(e)

def check_sell_signal(client, symbol, kline, now):
    target = client.market_client.targets[symbol]
    if not target.own or now < target.sell_least_time:
        return
    
    close = kline.tick.close
    open_ = kline.tick.open
    vol = kline.tick.vol

    if close < target.sell_least_price:
        try:
            client.send_sell_signal(symbol, close, open_, now, vol)
        except Exception as e:
            logger.error(e)

def kline_callback(symbol: str, client: WatcherClient):
    def warpper(kline: CandlestickEvent):
        now = kline.ts / 1000
        if not client.run or now < client.target_time:
            return

        if symbol in client.market_client.targets.keys():
            check_sell_signal(client, symbol, kline, now)
            write_kline_csv(csv_path, client.target_time, kline)

        elif now < client.target_time + SELL_AFTER:
            check_buy_signal(client, symbol, kline, now)
            write_kline_csv(csv_path, client.target_time, kline)

    csv_path = get_csv_handler(symbol, client.target_time)
    return warpper

def error_callback(error):
    logger.error(error)

@retry(tries=5, delay=1, logger=logger)
def init_watcher(Client) -> WatcherClient:
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
        logger.info('Sub watcher')
        time.sleep(25)
        client = init_watcher(WatcherClient)
        client.wait_to_run()
        task = client.get_task(WATCHER_TASK_NUM)

    if not task:
        return

    logger.info(f'Watcher task are:\n{", ".join(task)}, {len(task)}')
    for i, symbol in enumerate(task):
        client.market_client.sub_candlestick(
            symbol, CandlestickInterval.MIN5,
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
    scp_targets(client.market_client.targets.keys(), client.target_time)

if __name__ == '__main__':
    main()
