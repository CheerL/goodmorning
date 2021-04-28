import csv
import sys
import os
import time
import redis

from dataset.data2excel import create_excel
from utils import ROOT
from huobi.model.market.trade_detail import TradeDetail

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

def write_csv(csv_path, data: 'list[TradeDetail]', target_time):
    with open(csv_path, 'a+') as fcsv:
        writer = csv.writer(fcsv)
        writer.writerows([
            [each.ts / 1000 - target_time, each.price, each.amount, each.direction]
            for each in reversed(data)
        ])

def scp(file_path):
    os.system(f'scp {file_path} aws:{file_path}')

def scp_targets(targets, target_time):
    for symbol in targets:
        file_path = get_csv_path(symbol, target_time)
        scp(file_path)

def main():
    target_time_str = sys.argv[1]
    create_excel(target_time_str, DB_PATH)

if __name__ == '__main__':
    main()
