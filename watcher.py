import sys
import time
import csv

from wampyapp import SELL_AFTER, WatcherClient, WatcherMasterClient
from huobi.constant.definition import CandlestickInterval
from huobi.model.market.candlestick_event import CandlestickEvent
from huobi.model.market.trade_detail_event import TradeDetailEvent

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

def check_buy_signal(client, symbol, vol, open_, price, now, boot_price, end_price):
    if vol < MIN_VOL:
        return

    if boot_price < price < end_price:
        try:
            client.send_buy_signal(symbol, price, open_, now, vol)
        except Exception as e:
            logger.error(e)

def check_sell_signal(client, symbol, vol, open_, close, now):
    target = client.market_client.targets[symbol]
    if not target.own or now < target.sell_least_time:
        return

    if close < target.sell_least_price:
        try:
            client.send_sell_signal(symbol, close, open_, now, vol)
        except Exception as e:
            logger.error(e)

# def trade_detail_callback(symbol: str, client: WatcherClient, interval=300):
#     def warpper(event: TradeDetailEvent):
#         if not client.run or event.ts / 1000 < client.target_time:
#             return

#         with open(csv_path, 'a+') as fcsv:
#             writer = csv.writer(fcsv)

#             for detail in reversed(event.data):
#                 now = detail.ts / 1000
#                 last = now // interval
#                 price = detail.price
#                 if last > info['last']:
#                     info['last'] = last
#                     info['high'] = info['open_'] = price
#                     info['vol'] = 0
#                     info['boot_price'] = price * (1 + BOOT_RATE / 100)
#                     info['end_price'] = price * (1 + END_RATE / 100)

#                 info['vol'] += price * detail.amount
#                 info['high'] = max(info['high'], price)

#                 if symbol in client.market_client.targets.keys():
#                     check_sell_signal(client, symbol, info['vol'], info['open_'], price, now)
#                     writer.writerow([
#                         now - target_time, price, info['vol'],
#                         info['open_'], info['high']
#                     ])

#                 elif now < client.target_time + SELL_AFTER:
#                     check_buy_signal(client, symbol, info['vol'], info['open_'], price, now, info['boot_price'], info['end_price'])
#                     writer.writerow([
#                         now - target_time, price, info['vol'],
#                         info['open_'], info['high']
#                     ])

#     info = {
#         'last': 0,
#         'vol': 0,
#         'open_': 0,
#         'high': 0,
#         'boot_price': 0,
#         'end_price': 0
#     }
#     target_time = client.target_time
#     csv_path = get_csv_handler(symbol, target_time)
#     return warpper

def trade_detail_callback(symbol: str, client: WatcherClient, interval=300):
    def warpper(event: TradeDetailEvent):
        if not client.run or event.ts / 1000 < client.target_time:
            return

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
            
        if symbol in client.market_client.targets.keys():
            check_sell_signal(client, symbol, info['vol'], info['open_'], price, now)

        elif now < client.target_time + SELL_AFTER:
            check_buy_signal(client, symbol, info['vol'], info['open_'], price, now, info['boot_price'], info['end_price'])
            
        with open(csv_path, 'a+') as fcsv:
            writer = csv.writer(fcsv)
            writer.writerows([
                [each.ts / 1000 - target_time, each.price, each.amount, each.direction]
                for each in reversed(event.data)
            ])

    info = {
        'last': 0,
        'vol': 0,
        'open_': 0,
        'high': 0,
        'boot_price': 0,
        'end_price': 0
    }
    target_time = client.target_time
    csv_path = get_csv_handler(symbol, target_time)
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
        # client.market_client.sub_candlestick(
        #     symbol, CandlestickInterval.MIN5,
        #     kline_callback(symbol, client), error_callback
        # )
        client.market_client.sub_trade_detail(
            symbol, trade_detail_callback(symbol, client), error_callback
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
