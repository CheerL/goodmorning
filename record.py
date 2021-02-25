import os
import sqlite3
import time

from goodmorning import initial
from utils import ROOT

SQL_PATH = os.path.join(ROOT, 'market.db')

def main():
    if not os.path.exists(SQL_PATH):
        conn = sqlite3.connect(SQL_PATH)
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
        conn = sqlite3.connect(SQL_PATH)
        cursor = conn.cursor()

    _, market_client, target_time = initial()
    
    while time.time() < target_time - 10:
        time.sleep(1)

    # price_list = []
    # time_list = []
    while True:
        now = time.time()
        if now > target_time + 300:
            break

        price = market_client.get_price()
        # price_list.append(price)
        # time_list.append(now)
        for symbol, (close, vol) in price.items():
            cursor.execute(f'INSERT into market (symbol, time, price, vol) values (\'{symbol}\', \'{now}\', \'{close}\', \'{vol}\')')


    # for now, price in zip(time_list, price_list):
        
    cursor.close()
    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
