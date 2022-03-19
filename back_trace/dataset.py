from back_trace import model, func
import numpy as np
from user.binance import BinanceUser
from utils import datetime

def check_klines(path, level='1day'):
    _, level_ts = func.get_level(level)
    klines = model.BaseKlineDict.load(path)
    [u] = BinanceUser.init_users()
    model.Global.user = u

    for symbol in klines.dict():
        data = klines.dict(symbol)
        diff = np.diff(data['id'])
        for i in np.where(diff > level_ts)[0]:
            start_ts = data[i]['id']+level_ts
            end_ts = data[i+1]['id']
            raw_klines = model.Global.user.market.get_candlestick(
                symbol.decode(), level,
                start_ts=start_ts, end_ts=end_ts
            )
            klines.load_from_raw(symbol, raw_klines)
            print(f'insert {symbol} {len(raw_klines)} item from {datetime.ts2time(start_ts)} to {datetime.ts2time(end_ts)}')

        klines.data.sort(order=['symbol', 'id', 'vol'])
        remove_pos = np.array([], dtype=int)
        data = klines.dict(symbol)
        diff = np.diff(data['id'])
        for i in np.where(diff == 0)[0]:
            ts = data[i]['id']
            max_vol = data[data['id']==ts]['vol'].max()
            remove_pos = np.concatenate((
                remove_pos, 
                np.where(
                    (klines.data['id']==ts)&
                    (klines.data['symbol']==symbol)&
                    (klines.data['vol']<max_vol)
                )[0],
                np.where(
                    (klines.data['id']==ts)&
                    (klines.data['symbol']==symbol)&
                    (klines.data['vol']==max_vol)
                )[0][:-1]
            ))

        remove_pos = np.unique(remove_pos).astype(int)
        if remove_pos:
            klines.data = np.delete(klines.data, remove_pos)
            print(f'remove {symbol} {remove_pos}')

    klines.save(path)
