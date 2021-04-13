import csv
import os
import time

from data2excel import create_excel
from utils import ROOT, get_target_time

DB_PATH = os.path.join(ROOT, 'test', 'db', 'csv')

def get_csv_path(symbol, target_time):
    target_time_str = time.strftime('%Y-%m-%d-%H', time.localtime(target_time))
    return os.path.join(DB_PATH, f'{target_time_str}_{symbol}.csv')

def get_csv_handler(symbol, target_time):
    csv_path = get_csv_path(symbol, target_time)
    if not os.path.exists(csv_path):
        with open(csv_path, 'a+') as fcsv:
            csv.writer(fcsv).writerow(['time', 'price', 'amount', 'direction'])

    return csv_path

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
    
    info = {
        'last': 0,
        'vol': 0,
        'open_': 0,
        'high': 0
    }
    interval *= 1000
    return wrapper

def scp(file_path):
    os.system(f'scp {file_path} aws:{file_path}')

def scp_targets(targets, target_time):
    for symbol in targets:
        file_path = get_csv_path(symbol, target_time)
        scp(file_path)

def main():
    target_time = get_target_time()
    target_time_str = time.strftime('%Y-%m-%d-%H', time.localtime(target_time))
    create_excel(target_time_str, DB_PATH)

if __name__ == '__main__':
    main()
