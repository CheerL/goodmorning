import csv
import os
import time
import redis

from data2excel import create_excel
from utils import ROOT, get_target_time, config, user_config
from huobi.model.market.trade_detail import TradeDetail

DB_PATH = os.path.join(ROOT, 'test', 'db', 'csv')
RHOST = config.get('setting', 'RHost')
RPORT = config.get('setting', 'RPort')
RPASSWORD = user_config.get('setting', 'RPassword')

redis_conn = redis.Redis(host=RHOST, port=RPORT, db=0, password=RPASSWORD)

def get_csv_path(symbol, target_time):
    target_time_str = time.strftime('%Y-%m-%d-%H', time.localtime(target_time))
    return os.path.join(DB_PATH, f'{target_time_str}_{symbol}.csv')

def get_csv_handler(symbol, target_time):
    csv_path = get_csv_path(symbol, target_time)
    if not os.path.exists(csv_path):
        with open(csv_path, 'a+') as fcsv:
            csv.writer(fcsv).writerow(['time', 'price', 'amount', 'direction'])

    return csv_path


def write_redis(symbol: str, data: 'list[TradeDetail]'):
    redis_conn.mset({
        f'trade_{symbol}_{each.ts}_{i}' : f'{each.ts},{each.price},{each.amount},{each.direction}'
        for i, each in enumerate(reversed(data))
    })

def write_target(symbol):
    now_str = time.strftime('%Y-%m-%d-%H', time.localtime())
    name = f'target_{now_str}'
    targets = redis_conn.get(name).decode('utf-8')
    
    if symbol not in targets:
        redis_conn.set(name, ','.join([targets, symbol]))

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
    target_time = get_target_time()
    target_time_str = time.strftime('%Y-%m-%d-%H', time.localtime(target_time))
    create_excel(target_time_str, DB_PATH)

if __name__ == '__main__':
    main()
