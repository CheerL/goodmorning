import csv
import os
import time

from data2excel import create_excel
from market import MarketClient
from utils import ROOT, get_target_time, kill_all_threads, logger
from target import Target
from huobi.model.market.candlestick_event import CandlestickEvent
from huobi.model.market.trade_detail_event import TradeDetailEvent

DB_PATH = os.path.join(ROOT, 'test', 'db', 'csv')

def writerow(csv_path, row):
    with open(csv_path, 'a+') as fcsv:
        csv.writer(fcsv).writerow(row)

def writerows(csv_path, rows):
    with open(csv_path, 'a+') as fcsv:
        csv.writer(fcsv).writerows(rows)

def get_csv_path(symbol, target_time_str):
    return os.path.join(DB_PATH, f'{target_time_str}_{symbol}.csv')

def get_csv_handler(symbol, target_time):
    target_time_str = time.strftime('%Y-%m-%d-%H', time.localtime(target_time))
    csv_path = get_csv_path(symbol, target_time_str)
    if not os.path.exists(csv_path):
        writerow(csv_path, ['时间', '价格', '成交额', '开盘价', '高'])

    return csv_path

def write_kline_csv(csv_path, target_time: float, kline: CandlestickEvent):
    now = kline.ts / 1000 - target_time
    close = kline.tick.close
    high = kline.tick.high
    open_ = kline.tick.open
    vol = kline.tick.vol
    # fcsv_writer.writerow([now, close, vol, open_, high])
    writerow(csv_path, [now, close, vol, open_, high])
    # print(time.time() - kline.ts / 1000)

def write_detail_csv(csv_path, target_time: float, detail: TradeDetailEvent):
    writerows(csv_path, [[detail_item.ts/1000 - target_time, detail_item.price, detail_item.amount, 0, 0] for detail_item in detail.data])
    # print(time.time() - detail.ts / 1000)

def kline_callback(csv_path, target_time):
    def wrapper(kline):
        now = kline.ts / 1000 - target_time
        close = kline.tick.close
        high = kline.tick.high
        open_ = kline.tick.open
        vol = kline.tick.vol
        # fcsv_writer.writerow([now, close, vol, open_, high])
        writerow(csv_path, [now, close, vol, open_, high])
        # print(time.time() - kline.ts / 1000)
    
    return wrapper

def detail_callback(csv_path, target_time, interval=60):
    def wrapper(detail):
        with open(csv_path, 'a+') as fcsv:
            for detail_item in detail.data:
                now = detail_item.ts // interval
                if now > info['last']:
                    info['last'] = now
                    info['open_'] = detail_item.price
                    info['vol'] = 0
                    info['high'] = info['open_']
                
                info['vol'] += detail_item.price * detail_item.amount
                info['high'] = max(info['high'], detail_item.price)

                csv.writer(fcsv).writerow([detail_item.ts/1000 - target_time, detail_item.price, info['vol'], info['open_'], info['high']])
        # print(time.time() - detail.ts / 1000)
    
    info = {
        'last': 0,
        'vol': 0,
        'open_': 0,
        'high': 0
    }
    interval *= 1000
    return wrapper

def main():
    m = MarketClient()
    target_time = time.time()
    symbol = 'btcusdt'

    kline_path = get_csv_handler('kline', target_time)
    detail_path = get_csv_handler('detail', target_time)
    m.sub_candlestick(symbol, '1min', kline_callback(kline_path, target_time), None)
    m.sub_trade_detail(symbol, detail_callback(detail_path, target_time), None)
    time.sleep(30)
    # close_csv_handler()
    kill_all_threads()

if __name__ == '__main__':
    main()
