import os
import time
import math
from typing_extensions import final
import numpy as np

from retry import retry

from utils.parallel import run_thread_pool
from utils import get_rate, datetime, logger

from back_trace.model import Param, ContLossList, BaseKlineDict, Klines, Record, Global

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_boll(i, close, n=20, m=2):
    price = close[i-n+1:i+1]
    if not price.size:
        return 0, 0, 0

    sma = price.mean()
    std = price.std()
    return sma, sma+m*std, sma-m*std

def back_trace(
        cont_loss_list, base_klines_dict, param: Param,
        min_vol = 10, fee_rate = 0.002, days = 365, end = 2,
        init_money = 2000, write=False, interval='1min'
    ):
    start_date = datetime.ts2date(time.time()-(days+end)*86400)
    end_date = datetime.ts2date(time.time()-end*86400)
    record_path = f'{ROOT}/back_trace/csv/record_{start_date}_{end_date}.csv'
    money_record_path = f'{ROOT}/back_trace/csv/money_{start_date}_{end_date}.csv'

    data = cont_loss_list.data

    symbol=''
    for i, each in enumerate(data):
        if symbol != each['symbol']:
            symbol = each['symbol']
            cont_loss = cont_loss_days = 0
            is_max_loss=False

        if each['rate'] < min(0, param.min_close_rate) and cont_loss_days ==0:
            cont_loss_days = 1
            cont_loss=each['rate']
            is_max_loss=True
        elif each['rate'] < param.min_close_rate and cont_loss_days > 0:
            start_i = i-cont_loss_days
            cont_loss_days+=1
            cont_loss=each['close']/data[start_i]['open']-1
            is_max_loss=data[start_i:i+1]['rate'].min()>=each['rate']
        else:
            cont_loss=cont_loss_days=0
            is_max_loss=False
        each['cont_loss_days']=cont_loss_days
        each['cont_loss_rate']=cont_loss
        each['is_max_loss']=is_max_loss

    data = data[
        (param.min_buy_vol <= data['vol']) & (data['vol'] <= param.max_buy_vol) &
        (param.min_price <= data['close']) & (data['close'] <= param.max_price) &
        (data['rate'] < param.min_close_rate) &
        # (data['low2']/data['close']-1 <= -0.002) &
        (
            (
                (data['cont_loss_days']==1) &
                (data['close']>data['boll']) &
                (data['cont_loss_rate'] <= param.up_cont_rate)
            ) | (
                # (data['close'] <= data['boll']) &
                (data['cont_loss_rate'] <= param.min_cont_rate) &
                (data['is_max_loss']==1)
            ) | (
                # (data['close'] <= data['boll']) &
                (data['cont_loss_rate'] <= param.break_cont_rate)
            )
        )
    ]

    max_money = last_money = total_money = money = init_money
    max_back_rate = 0
    money_record = []
    record_list = []
    holding_list = []

    init_date = datetime.ts2date(time.time() - (days+end-1) * 86400)
    init_ts = int(datetime.date2ts(init_date))

    for i in range(days+end+1):
        ts = init_ts + i * 86400
        date = datetime.ts2date(ts)

        total_sell_vol = 0
        for record in holding_list.copy():
            if record.sell_time <= ts+86400:
                total_sell_vol += record.sell_vol
                holding_list.remove(record)
        money += total_sell_vol

        targets = np.sort(data[data['date']==date.encode()], order='vol')[::-1]
        up_targets = targets[targets['close']>targets['boll']]
        low_targets = targets[targets['close']<=targets['boll']]
        targets_num = len(targets)

        if targets_num:
            buy_num = int(min(max(targets_num, param.min_num), param.max_num))
            buy_vol = round(money / buy_num, 3)
            if buy_vol < min_vol:
                buy_vol = min_vol
                buy_num = math.floor(money // min_vol)
            
            if buy_vol * buy_num > money:
                buy_vol -= 0.001

            total_buy_vol = 0
            for cont_loss in np.concatenate([low_targets, up_targets])[:buy_num]:
                if not cont_loss['id2']:
                    continue
                
                if cont_loss['symbol'] == b'COCOSUSDT' and 1610668800 <= cont_loss['id'] <= 1611100800:
                    continue

                buy_price, buy_time = get_buy_price_and_time(
                    cont_loss, param, date, interval
                )
                
                if buy_time == 0:
                    continue

                sell_price, sell_time = get_sell_price_and_time(
                    cont_loss, base_klines_dict, param, date, interval, buy_time
                )
                record = Record(
                    cont_loss['symbol'].decode(), buy_price, buy_time, buy_vol, fee_rate=fee_rate
                )
                record.sell(sell_price, sell_time)
                record_list.append(record)
                holding_list.append(record)
                total_buy_vol += buy_vol
        else:
            total_buy_vol = 0

        money -= total_buy_vol

        holding_money = 0
        for record in holding_list:
            try:
                base_klines = base_klines_dict.dict(record.symbol)
                close = base_klines[base_klines['id'] == ts]['close'][0]
                holding_money += record.amount * close
            except Exception as e:
                if 1611100800 <= ts < 1611360000 and record.symbol == 'COCOSUSDT':
                    close = base_klines[base_klines['id'] == 1611014400]['close'][0]
                    holding_money += record.amount * close
                else:
                    print(record.symbol, datetime.ts2time(ts))
                    raise e



        total_money = money + holding_money
        day_profit = total_money - last_money
        day_profit_rate = get_rate(total_money, last_money, -1)
        max_money = max(max_money, total_money)
        day_back_rate = get_rate(total_money, max_money, -1)
        all_rate = get_rate(total_money, init_money, -1)
        money_record.append([
            date, total_buy_vol, total_sell_vol,
            money, holding_money, total_money,
            all_rate, day_profit, day_profit_rate, day_back_rate
        ])
        last_money = total_money

    profit_rate = total_money / init_money
    max_back_rate = min([e[-1] for e in money_record])
    # print(total_money, profit_rate, max_back_rate)

    if write:
        print(total_money, profit_rate, max_back_rate)
        Record.write_csv(record_list, record_path)
        
        with open(money_record_path, 'w+') as f:
            f.write('日期,买入额,卖出额,可用额,持有币价值,总资金,累计收益率,日收益,日收益率,回撤\n')
            for record in money_record:
                f.write(','.join([str(e) for e in record])+'\n')
    return total_money, profit_rate, max_back_rate

@retry(tries=10, delay=30)
def get_detailed_klines(symbol, interval, start_time):
    path = f'{ROOT}/back_trace/npy/detail/{symbol}_{start_time}_{interval}.npy'
    if os.path.exists(path):
        klines =  Klines.load(path)
    else:
        raw_klines = Global.user.market.get_candlestick(
            symbol, interval,
            start_ts=start_time, end_ts=start_time+86400
        )
        raw_klines.reverse()
        klines = Klines()
        klines.load_from_raw(symbol, raw_klines)
        klines.save(path)
    return klines


def buy_detailed_back_trace(symbol, start_time, buy_price, low_price, max_buy_ts, interval='1min'):
    klines = get_detailed_klines(symbol, interval, start_time)
    data = klines.data

    start_ts = data[0]['id']
    stop_buy_ts = start_ts + max_buy_ts

    try:
        low_ts = data[data['high'] >= low_price]['id'][0]
        stop_buy_ts = min(low_ts, stop_buy_ts)
    except:
        pass

    try:
        buy_time = data[(data['low'] <= buy_price) & (data['id'] < stop_buy_ts)]['id'][0]
    except:
        buy_time = 0

    return buy_price, buy_time

def sell_detailed_back_trace_high_fix(symbol, start_time, buy_time, high_price, high_back_price, low_price, low_back_price, stop_loss_price, interval='1min'):
    klines = get_detailed_klines(symbol, interval, start_time)
    data = klines.data
    data = data[data['id'] > buy_time]
    end_ts = data[-1]['id']
    end_price = data[-1]['close']
    # print(datetime.ts2time(data[0]['id']), stop_loss_price)
    try:
        high_ts = data[data['high'] >= high_price]['id'][0]
        high_back_ts = data[(data['low'] <= high_back_price) & (data['id']>high_ts)]['id'][0]
    except:
        high_back_ts = end_ts + 1

    try:
        low_ts = data[data['high'] >= low_price]['id'][0]
        low_back_ts = data[(data['low'] <= low_back_price) & (data['id']>low_ts)]['id'][0]
    except:
        low_back_ts = end_ts + 2

    try:
        stop_loss_ts = data[data['low'] <= stop_loss_price]['id'][0]
        # print(data[data['low'] <= stop_loss_price]['id'])
    except:
        stop_loss_ts = end_ts + 3

    result_dict = {
        end_ts: (end_price, end_ts),
        high_back_ts: (high_back_price, high_back_ts),
        low_back_ts: (low_back_price, low_back_ts),
        stop_loss_ts: (stop_loss_price, stop_loss_ts),
    }
    # print(end_ts, high_back_ts, low_back_ts, stop_loss_ts)
    return result_dict[min(end_ts, high_back_ts, low_back_ts, stop_loss_ts)]
    
def sell_detailed_back_trace(symbol, start_time, buy_time, high_price, high_back_rate, high_hold_time, low_price, low_back_price, stop_loss_price, interval='1min'):
    klines = get_detailed_klines(symbol, interval, start_time)
    data = klines.data
    data = data[data['id'] > buy_time]
    end_ts = data[-1]['id']
    end_price = data[-1]['close']
    open_price = data[0]['open']
    # print(datetime.ts2time(data[0]['id']), stop_loss_price)
    try:
        high_ts = data[data['high'] >= high_price]['id'][0]
        max_high_hold_time = high_ts + high_hold_time
        cum_high_back = high_back_rate * np.maximum.accumulate(data['high']) + (1-high_back_rate) * open_price
        high_back = data[
            (data['low'] <= cum_high_back) & 
            (data['id'] > high_ts) & 
            (data['id'] <= max_high_hold_time)
        ]
        if high_back.size:
            high_back_ts = high_back['id'][0]
            high_back_price = cum_high_back[data['id'] == high_back_ts][0]
        else:
            high_max_hold = data[data['id'] > max_high_hold_time][0]
            high_back_ts = high_max_hold['id']
            high_back_price = high_max_hold['open']
    except:
        high_back_ts = end_ts + 1
        high_back_price = 0

    try:
        low_ts = data[data['high'] >= low_price]['id'][0]
        low_back_ts = data[(data['low'] <= low_back_price) & (data['id']>low_ts)]['id'][0]
    except:
        low_back_ts = end_ts + 2

    try:
        stop_loss_ts = data[data['low'] <= stop_loss_price]['id'][0]
        # print(data[data['low'] <= stop_loss_price]['id'])
    except:
        stop_loss_ts = end_ts + 3

    result_dict = {
        end_ts: (end_price, end_ts),
        high_back_ts: (high_back_price, high_back_ts),
        low_back_ts: (low_back_price, low_back_ts),
        stop_loss_ts: (stop_loss_price, stop_loss_ts),
    }
    # print(end_ts, high_back_ts, low_back_ts, stop_loss_ts)
    return result_dict[min(end_ts, high_back_ts, low_back_ts, stop_loss_ts)]

def sell_detailed_next_back_trace(symbol, start_time, stop_profit_price, stop_loss_price, interval='1min'):
    klines = get_detailed_klines(symbol, interval, start_time)
    data = klines.data
    end_ts = data[-1]['id']
    # print(datetime.ts2time(data[0]['id']), stop_loss_price)
    try:
        stop_profit_ts = data[data['high'] >= stop_profit_price]['id'][0]
    except:
        stop_profit_ts = end_ts + 1

    try:
        stop_loss_ts = data[data['low'] <= stop_loss_price]['id'][0]
        stop_loss_price = min(stop_loss_price, data[data['id']==stop_loss_ts]['open'][0])
        stop_loss_ts -= 1
        # print(data[data['low'] <= stop_loss_price]['id'])
    except:
        stop_loss_ts = end_ts + 3

    result_dict = {
        stop_profit_ts: (stop_profit_price, stop_profit_ts),
        stop_loss_ts: (stop_loss_price, stop_loss_ts),
    }
    # print(end_ts, high_back_ts, low_back_ts, stop_loss_ts)
    return result_dict[min(stop_profit_ts, stop_loss_ts)]

def get_buy_price_and_time(cont_loss, param: Param, date, interval):
    symbol = cont_loss['symbol'].decode()
    params_key = ''.join([
        f'{each:.4f}' for each in
        [
            param.max_buy_ts,
            param.buy_rate,
            param.low_rate
        ]
    ])
    key = f'{symbol}{date}{interval}{params_key}'
    if key in Global.buy_dict:
        return Global.buy_dict[key]

    else:
        close = cont_loss['close']
        buy_price = close * (1 + param.buy_rate)
        low_price = close * (1 + param.low_rate)
        start_time = cont_loss['id2']
        _, buy_time = buy_detailed_back_trace(
            symbol, start_time, buy_price, low_price, param.max_buy_ts, interval=interval
        )

        Global.sell_dict[key] = (buy_price, buy_time)
        return buy_price, buy_time

def get_sell_price_and_time(cont_loss, base_klines_dict, param: Param, date, interval, buy_time):
    symbol = cont_loss['symbol'].decode()
    params_key = ''.join([
        f'{each:.4f}' for each in
        [
            param.max_hold_days,
            param.high_rate,
            param.high_back_rate,
            param.high_hold_time,
            param.low_rate,
            param.low_back_rate,
            param.clear_rate,
            param.final_rate,
            param.stop_loss_rate,
            buy_time
        ]
    ])
    key = f'{symbol}{date}{interval}{params_key}'
    if key in Global.sell_dict:
        return Global.sell_dict[key]

    else:
        close = cont_loss['close']
        # high_price = max(
        #     (cont_loss['open'] + cont_loss['close']) / 2,
        #     close * (1 + high_rate)
        # )
        high_price = close * (1+param.high_rate)
        high_back_price = (1-param.high_back_rate) * close + param.high_back_rate * high_price
        # print(param.high_back_rate, close, high_back_price)
        # high_back_price = close * (1 + 3*low_back_rate)
        low_price = close * (1 + param.low_rate)
        # low_price = (cont_loss['open'] + cont_loss['close']) / 2
        # low_price = min(
        #     (cont_loss['open'] + cont_loss['close']) / 2,
        #     close * (1 + low_rate)
        # )
        low_back_price = close * (1 + param.low_back_rate)
        # low_back_price = (close + low_price) / 2
        clear_price = close * (1 + param.clear_rate)
        final_price = close * (1 + param.final_rate)
        stop_loss_price = close * (1 + param.stop_loss_rate)
        
        # sell_price = cont_loss['close2']
        # sell_time = cont_loss['id2']+86340
        if cont_loss['high2'] < low_price and cont_loss['low2'] > 0:
            if cont_loss['close2'] >= clear_price:
                sell_price = cont_loss['close2']
                sell_time = cont_loss['id2']+86340
            else:
                klines = base_klines_dict.dict(symbol)
                i = np.where(klines['id']==cont_loss['id'])[0][0]
                if i+2 >= len(klines):
                    sell_price = klines[-1]['close']
                    sell_time = klines[-1]['id']+86340
                else:
                    for day_kline in klines[i+2:i+param.max_hold_days+1]:
                        if day_kline['high'] > final_price and day_kline['low'] <= stop_loss_price:
                            sell_price, sell_time = sell_detailed_next_back_trace(
                                symbol, day_kline['id'], final_price, stop_loss_price, interval=interval
                            )
                            break
                        elif day_kline['low'] <= stop_loss_price:
                            sell_price = min(stop_loss_price, day_kline['open'])
                            sell_time = day_kline['id']+86340
                            # print('~', sell_price, datetime.ts2time(sell_time))
                            break
                        elif day_kline['high'] > final_price:
                            sell_price = final_price
                            sell_time = day_kline['id']+86340
                            break
                    else:
                        sell_price = day_kline['close']
                        sell_time = day_kline['id']+86340
                    
        else:
            start_time = cont_loss['id2']
            # sell_price, sell_time = sell_detailed_back_trace(
            #     symbol, start_time, buy_time,
            #     high_price, param.high_back_rate, param.high_hold_time,
            #     low_price, low_back_price, 0, interval=interval
            # )
            sell_price, sell_time = sell_detailed_back_trace_high_fix(
                symbol, start_time, buy_time,
                high_price, high_back_price,
                low_price, low_back_price, 0, interval=interval
            )

        Global.sell_dict[key] = (sell_price, sell_time)
        return sell_price, sell_time

def get_data(days=365, end=2, load=True, min_before=180, klines_dict=None, cont_loss_list=None, filter_=True):
    now = time.time()
    start_date = datetime.ts2date(now-(days+end)*86400)
    end_date = datetime.ts2date(now-end*86400)
    start_ts = int(datetime.date2ts(start_date))
    end_ts = int(datetime.date2ts(end_date))

    special_symbols = [b'AAVEUPUSDT', b'SXPUPUSDT', b'YFIUPUSDT']
    cont_loss_list_path = f'{ROOT}/back_trace/npy/cont_list.npy'
    klines_dict_path = f'{ROOT}/back_trace/npy/base_klines_dict.npy'
    # cont_loss_csv_path = f'{ROOT}/test/csv/cont_loss_{start_date}_{end_date}.csv'
    if klines_dict:
        max_ts = klines_dict.data['id'].max()
        symbols = []
    elif load and os.path.exists(klines_dict_path):
        klines_dict = BaseKlineDict.load(klines_dict_path)
        max_ts = klines_dict.data['id'].max()
        dict_symbols = klines_dict.dict()
        market = Global.user.market
        all_symbols = market.all_symbol_info.keys()
        symbols = [
            each for each in all_symbols
            if each.encode() not in dict_symbols
        ]
        
        for each in dict_symbols:
            max_ts = klines_dict.dict(each)['id'].max()
            # min_ts = klines_dict.dict(each)['id'].min()
            if end_ts > max_ts and each not in special_symbols:
                symbols.append(each.decode())

    else:
        klines_dict = BaseKlineDict()
        max_ts = 0
        market = Global.user.market
        symbols = market.all_symbol_info.keys()

    if len(symbols):
        def worker(symbol):
            try:
                klines = market.get_candlestick(symbol, '1day', start_ts=start_ts, end_ts=end_ts+86400)
                klines_dict.load_from_raw(symbol, klines)
            except Exception as e:
                print(e)

        run_thread_pool([(worker, (symbol,)) for symbol in symbols], True, 4)
        klines_dict.data = np.unique(klines_dict.data)
        klines_dict.data.sort(order=['symbol', 'id', 'vol'])
        pos_list = np.array([], dtype=int)
        for symbol in klines_dict.dict():
            data = klines_dict.dict(symbol)
            for each in data[np.where(np.diff(data['id'])==0)[0]]:
                ts = each['id']
                max_vol = data[data['id']==ts]['vol'].max()
                pos = list(np.where(
                    (klines_dict.data['id']==ts)&
                    (klines_dict.data['symbol']==symbol)&
                    (klines_dict.data['vol']<max_vol)
                )[0])
                pos_list += pos

        klines_dict.data = np.delete(klines_dict.data, pos_list)
        klines_dict.save(klines_dict_path)
    
    if cont_loss_list:
        max_ts = cont_loss_list.data['id'].max()
        min_ts = cont_loss_list.data['id'].min()
        symbols = []
    elif load and os.path.exists(cont_loss_list_path):
        cont_loss_list = ContLossList.load(cont_loss_list_path)
        symbols = []
        for symbol in klines_dict.dict():
            data = klines_dict.dict(symbol)
            max_ts = data['id'].max()
            min_ts = data['id'].min()
            if end_ts > max_ts and symbol not in special_symbols:
                # print(symbol, datetime.ts2time(min_ts),  datetime.ts2time(start_ts))
                symbols.append(symbol)
    else:
        cont_loss_list = ContLossList()
        max_ts = 0
        min_ts = now
        symbols = klines_dict.dict()

    if len(symbols):
        temp_list = []
        for symbol in symbols:
            data = klines_dict.dict(symbol)
            rate_list = data['close']/data['open'] - 1

            cont_loss_days = cont_loss_rate = 0
            for i, rate in enumerate(rate_list):
                if rate < 0:
                    cont_loss_days += 1
                    cont_loss_rate = data[i]['close']/data[i-cont_loss_days+1]['open']-1
                    is_max_loss = rate_list[i-cont_loss_days+1:i+1].min() == rate
                else:
                    cont_loss_days = cont_loss_rate = is_max_loss = 0


                date = datetime.ts2date(data[i]['id'])
                try:
                    _, id2, open2, close2, high2, low2, vol2 = data[i+1]
                except IndexError:
                    id2 = open2 = close2 = high2 = low2 = vol2 = 0

                temp_list.append((
                    *data[i], id2, open2, close2, high2, low2, vol2,
                    date, rate, cont_loss_days, cont_loss_rate, is_max_loss,
                    *get_boll(i, data['close']), i
                ))
        cont_loss_list.data = np.unique(np.concatenate([
            cont_loss_list.data,
            np.array(temp_list, dtype=ContLossList.dtype)
        ]))

        cont_loss_list.data.sort(order=['symbol', 'id'])
        cont_loss_list.save(cont_loss_list_path)

    if filter_:
        data = cont_loss_list.data
        cont_loss_list = ContLossList()
        cont_loss_list.data = data[
            (data['index']>min_before) &
            (data['id']>=start_ts) &
            (data['id']<end_ts)
            # (data['rate']<0)
        ]

    return cont_loss_list, klines_dict

