import os
import sqlite3
import time
import csv

from market import MarketClient
from utils import ROOT, get_target_time, kill_all_threads, logger

SQL_PATH = os.path.join(ROOT, 'market.db')
target_time = get_target_time()

class SQLMarketClient(MarketClient):

    def handle_big_increase(self, big_increase, base_price):
        targets = []
        for symbol, now_price, target_increase, _ in big_increase:
            self.target_symbol.append(symbol)
            self.sub_candlestick(symbol, '5min', kline_callback(symbol, target_time), lambda e: logger.error(e))
            init_price, _ = base_price[symbol]
            logger.info(f'Find target: {symbol.upper()}, initial price {init_price}, now price {now_price} , increase {target_increase}%')

        return targets

class CSVMarketClient(MarketClient):
    def handle_big_increase(self, big_increase, base_price):
        targets = []
        for symbol, now_price, target_increase, _ in big_increase:
            self.target_symbol.append(symbol)
            self.sub_candlestick(symbol, '5min', kline_callback_csv(symbol, target_time), lambda e: logger.error(e))
            init_price, _ = base_price[symbol]
            logger.info(f'Find target: {symbol.upper()}, initial price {init_price}, now price {now_price} , increase {target_increase}%')

        return targets

def create_conn(path):
    if not os.path.exists(path):
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE market (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol varchar(20),
            time varchar(30),
            price varchar(30),
            vol varchar(30)
        )
        ''')
    else:
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
    return conn, cursor

def kline_callback_csv(symbol, target_time):
    def wrapper(kline):
        now = time.time() - target_time
        close = kline.tick.close
        high = kline.tick.high
        open_ = kline.tick.open
        vol = kline.tick.vol
        increase = close / open_ - 1
        back = 1 - high / close
        
        with open(csv_path, 'a+') as fcsv:
            csv.writer(fcsv).writerow([now, close, vol, increase, high, back])


    target_time_str = time.strftime('%Y-%m-%d-%H', time.localtime(target_time))
    csv_path = os.path.join(ROOT, 'test', 'db', f'{symbol}_{target_time_str}.csv')
    if not os.path.exists(csv_path):
        with open(csv_path, 'a+') as fcsv:
            csv.writer(fcsv).writerow(['时间', '价格', '成交额', '涨幅', '高', '回撤'])

    return wrapper


def kline_callback(symbol, target_time):
    def wrapper(kline):
        conn, cursor = create_conn(sql_path)
        now = time.time()
        close = kline.tick.close
        vol = kline.tick.vol
        cursor.execute(f'INSERT into market (symbol, time, price, vol) values (\'{symbol}\', \'{now}\', \'{close}\', \'{vol}\')')
        cursor.close()
        conn.commit()
        conn.close()


    target_time_str = time.strftime('%Y-%m-%d-%H', time.localtime(target_time))
    sql_path = os.path.join(ROOT, 'test', 'db', f'{symbol}_{target_time_str}.db')

    return wrapper

def main():
    market_client = CSVMarketClient()
    if target_time % (24*60*60) == 16*60*60:
        market_client.midnight = True

    base_price, base_price_time = market_client.get_base_price(target_time)
    market_client.exclude_expensive(base_price)

    for _ in range(5):
        market_client.get_target(time.time(), base_price, base_price_time, change_base=False, interval=10, unstop=False)

    while time.time() < target_time + 300:
        pass

    kill_all_threads()

if __name__ == '__main__':
    main()
