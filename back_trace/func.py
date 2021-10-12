import os
import time
import math
import numpy as np

from retry import retry

from utils.parallel import run_thread_pool
from utils import get_rate, datetime, logger

from back_trace.model import ContLoss, ContLossList, BaseKlineDict, Klines, Record, Global

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_boll(i, close, n=20, m=2):
    price = close[i-n+1:i+1]
    sma = price.mean()
    std = price.std()
    return sma, sma+m*std, sma-m*std

def find_cont_loss(klines_dict, days, end, min_before=180, with_klines=False):
    cont_loss_list = ContLossList()
    for symbol in np.unique(klines_dict.data['symbol']):
        data = klines_dict.data[klines_dict.data['symbol']==symbol]
        rate_list = data['close']/data['open'] - 1
        temp_list = []

        cont_loss_days = cont_loss_rate = 0
        for i, rate in enumerate(rate_list):
            if rate < 0:
                cont_loss_days += 1
                cont_loss_rate += rate
                is_max_loss = rate_list[i-cont_loss_days+1:i+1].min() == rate
            else:
                cont_loss_days = cont_loss_rate = is_max_loss = 0

            temp_list.append((*data[i], *[*data[3]][1:6], rate, cont_loss_days, cont_loss_rate, is_max_loss))
        
        cont_loss_list.data = np.concatenate([
            cont_loss_rate.data,
            np.array(temp_list, dtype=ContLossList.dtype)
        ])

def back_trace(
        cont_loss_list,
        base_klines_dict,
        min_vol = 10,
        fee_rate = 0.002,
        days = 365,
        end = 2,
        max_hold_days = 14,
        min_num = 5,
        max_num = 50,
        min_buy_vol = 0,
        max_buy_vol = 10000000000,
        min_price = 0,
        max_price = 1,
        high_rate = 0.2,
        low_rate = 0.05,
        low_back_rate = 0.02,
        clear_rate = -0.01,
        final_rate = 0.01,
        stop_loss_rate = -0.99,
        min_cont_days = 2,
        min_cont_rate = -0.15,
        break_cont_rate = -0.2,
        init_money = 2000,
        write=False,
        detailed_interval='1min'
    ):
    start_date = datetime.ts2date(time.time()-(days+end)*86400)
    end_date = datetime.ts2date(time.time()-end*86400)
    record_path = f'{ROOT}/back_trace/csv/record_{start_date}_{end_date}.csv'
    money_record_path = f'{ROOT}/back_trace/csv/money_{start_date}_{end_date}.csv'

    data = cont_loss_list.data
    data = data[
        (data['cont_loss_days'] >= min_cont_days) &
        (min_buy_vol <= data['vol']) & (data['vol'] <= max_buy_vol) &
        (min_price <= data['close']) & (data['close'] <= max_price) &
        # (0 <= cont_loss.kline.close <= 1 or 10 <= cont_loss.kline.close <= 1000) and
        (data['low2']/data['close']-1 <= -0.002) &
        # (data['close'] < data['boll']) &
        (
            # ((data['cont_loss_days']==1) & (data['low']>data['boll']) & (data['cont_loss_rate'] <= min_cont_rate-0.05)) |
            (data['cont_loss_rate'] <= break_cont_rate) |
            ((data['cont_loss_rate'] <= min_cont_rate) & (data['is_max_loss']==1))
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
        targets_num = len(targets)
        if targets_num:
            buy_num = min(max(targets_num, min_num), max_num)
            buy_vol = round(money / buy_num, 3)
            if buy_vol < min_vol:
                buy_vol = min_vol
                buy_num = math.floor(money // min_vol)
            
            if buy_vol * buy_num > money:
                buy_vol -= 0.001

            total_buy_vol = 0
            for cont_loss in targets[:buy_num]:
                if not cont_loss['id2']:
                    continue

                sell_price, sell_time = get_sell_price_and_time(
                    cont_loss, base_klines_dict, date, max_hold_days, detailed_interval,
                    high_rate, low_rate, low_back_rate, clear_rate, final_rate, stop_loss_rate
                )
                record = Record(
                    cont_loss['symbol'].decode(), cont_loss['close'],
                    cont_loss['id']+86399, buy_vol, fee_rate=fee_rate
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
            base_klines = base_klines_dict.dict[record.symbol]
            close = base_klines[base_klines['id'] == ts]['close'][0]
            holding_money += record.amount * close

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
    # 

    path = f'{ROOT}/back_trace/npy/detail/{symbol}_{start_time}_{interval}.npy'
    # if path in Global.klines_dict:
    #     klines = Global.klines_dict[path]
    # elif os.path.exists(path):
    if os.path.exists(path):
        klines =  Klines.load(path)
    else:
        raw_klines = Global.user.market_client.get_candlestick(
            symbol, interval,
            start_ts=start_time, end_ts=start_time+86400
        )
        raw_klines.reverse()
        klines = Klines()
        klines.load_from_raw(symbol, raw_klines)
        klines.save(path)
        # Global.klines_dict[path] = klines
    return klines

def detailed_back_trace(symbol, start_time, high_price, high_back_price, low_price, low_back_price, stop_loss_price, interval='1min'):
    klines = get_detailed_klines(symbol, interval, start_time)
    data = klines.data
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

def detailed_back_trace2(symbol, start_time, stop_profit_price, stop_loss_price, interval='1min'):
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

def get_sell_price_and_time(
    cont_loss, base_klines_dict, date, max_hold_days, detailed_interval,
    high_rate, low_rate, low_back_rate, clear_rate, final_rate, stop_loss_rate
    ):
    key = f'{cont_loss["symbol"].decode()}{date}{detailed_interval}{max_hold_days:02}{high_rate:.4f}{low_rate:.4f}{low_back_rate:.4f}{clear_rate:.4f}{final_rate:.4f}{stop_loss_rate:.4f}'
    if key in Global.sell_dict:
        return Global.sell_dict[key]

    else:
        symbol = cont_loss['symbol'].decode()
        close = cont_loss['close']
        high_price = max(
            (cont_loss['open'] + cont_loss['close']) / 2,
            close * (1 + high_rate)
        )
        high_back_price = (cont_loss['close'] + high_price) / 2
        low_price = close * (1 + low_rate)
        low_back_price = close * (1 + low_back_rate)
        clear_price = close * (1 + clear_rate)
        final_price = close * (1 + final_rate)
        stop_loss_price = close * (1 + stop_loss_rate)

        if cont_loss['high2'] < low_price and cont_loss['low2'] > 0:
            if cont_loss['close2'] >= clear_price:
                sell_price = cont_loss['close2']
                sell_time = cont_loss['id2']+86340
            else:
                klines = base_klines_dict.dict[cont_loss['symbol'].decode()]
                i = np.where(klines['id']==cont_loss['id'])[0][0]
                for day_kline in klines[i+2:i+max_hold_days+1]:
                    if day_kline['high'] > final_price and day_kline['low'] <= stop_loss_price:
                        sell_price, sell_time = detailed_back_trace2(
                            symbol, day_kline['id'], final_price, stop_loss_price, interval=detailed_interval
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
            sell_price, sell_time = detailed_back_trace(
                symbol, start_time, high_price, high_back_price, low_price, low_back_price, 0, interval=detailed_interval
            )

        Global.sell_dict[key] = (sell_price, sell_time)
        return sell_price, sell_time

def get_data(days=365, end=2, load=True, min_before=180, klines_dict=None, cont_loss_list=None, filter_=True):
    now = time.time()
    start_date = datetime.ts2date(now-(days+end)*86400)
    end_date = datetime.ts2date(now-end*86400)
    start_ts = datetime.date2ts(start_date)
    end_ts = datetime.date2ts(end_date)

    cont_loss_list_path = f'{ROOT}/back_trace/npy/cont_list.npy'
    klines_dict_path = f'{ROOT}/back_trace/npy/base_klines_dict.npy'
    # cont_loss_csv_path = f'{ROOT}/test/csv/cont_loss_{start_date}_{end_date}.csv'

    if klines_dict:
        max_ts = klines_dict.data['id'].max()
    elif load and os.path.exists(klines_dict_path):
        klines_dict = BaseKlineDict.load(klines_dict_path)
        max_ts = klines_dict.data['id'].max()
    else:
        klines_dict = BaseKlineDict()
        max_ts = 0

    if max_ts < end_ts:
        def worker(symbol):
            try:
                klines = list(reversed(market.get_candlestick(symbol, '1day', days)))
            except Exception as e:
                print(e)
                return

            klines_dict.load_from_raw(symbol, klines)

        days = min((end_ts - max_ts) // 86400, 1000)
        market = Global.user.market_client
        symbols = market.all_symbol_info.keys()
        run_thread_pool([(worker, (symbol,)) for symbol in symbols], True, 4)
        klines_dict.data.sort(order=['symbol', 'id'])
        klines_dict.save(klines_dict_path)
    
    if cont_loss_list:
        max_ts = cont_loss_list.data['id'].max()
        min_ts = cont_loss_list.data['id'].min()
    elif load and os.path.exists(cont_loss_list_path):
        cont_loss_list = ContLossList.load(cont_loss_list_path)
        max_ts = cont_loss_list.data['id'].max()
        min_ts = cont_loss_list.data['id'].min()
    else:
        cont_loss_list = ContLossList()
        max_ts = 0
        min_ts = now

    if max_ts < end_ts or min_ts > start_ts:
        cont_loss_list = ContLossList()
        for symbol in np.unique(klines_dict.data['symbol']):
            data = klines_dict.data[klines_dict.data['symbol']==symbol]
            rate_list = data['close']/data['open'] - 1
            temp_list = []

            cont_loss_days = cont_loss_rate = 0
            for i, rate in enumerate(rate_list):
                if i < min_before:
                    continue

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
                    *get_boll(i, data['close'])
                ))
            
            cont_loss_list.data = np.concatenate([
                cont_loss_list.data,
                np.array(temp_list, dtype=ContLossList.dtype)
            ])

        cont_loss_list.data.sort(order=['symbol', 'id'])
        cont_loss_list.save(cont_loss_list_path)

    if filter_:
        data = cont_loss_list.data
        cont_loss_list = ContLossList()
        cont_loss_list.data = data[
            # (data['symbol']==b'XRPUSDT') &
            (data['id']>=start_ts) &
            (data['id']<end_ts) &
            (data['rate']<0)
        ]

    return cont_loss_list, klines_dict

