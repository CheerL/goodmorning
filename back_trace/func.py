import os
import time
import math
import numpy as np
from retry import retry
from utils.parallel import run_thread_pool
from utils import get_rate, datetime, get_level, logger
from back_trace.model import Param, ContLossList, BaseKlineDict, Klines, Record, Global

# np.seterr(all='raise')

CHECK_PREVIOUS = False
SELL_AS_BUY = True
BOLL_N = 20
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPECIAL_SYMBOLS = [
    # stable coin
    b'USTUSDT', b'USDCUSDT', b'BUSDUSDT', b'TUSDUSDT',
    b'USDPUSDT', b'SUSDUSDT',
    # else
    b'AAVEUPUSDT', b'SXPUPUSDT', b'YFIUPUSDT',
    b'BCHUPUSDT', b'BZRXUSDT', b'EOSUPUSDT', b'FILUPUSDT',
    b'SUSHIUPUSDT', b'UNIUPUSDT', b'XLMUPUSDT', b'BTTUSDT',
    b'LOKAUSDT', b'SCRTUSDT', b'LTCUPUSDT', b'XTZUPUSDT',
    b'NUUSDT', b'NANOUSDT', b'KEEPUSDT', b'ANYUSDT'
]


def get_real_boll(price, m, eps=1e-5):
    if price.size < BOLL_N-1:
        return 0
    try:
        now = price[-1]
        while True:
            new_price = np.append(price, now)
            sma = new_price.mean()
            std = new_price.std()
            boll = sma + m * std
            if abs(boll/now-1) < eps:
                break
            else:
                now = boll
    except Exception as e:
        print(e)
        print(price, now, new_price, std, sma, m)
        raise e
    return boll


def get_boll(price, m=[2, -2]):
    if not price.size:
        return [0] * (len(m)+1)

    sma = price.mean()
    std = price.std()
    return [sma]+[sma+k*std for k in m]


def get_record_now_rate(record: Record, start_time, now):
    klines = get_detailed_klines(record.symbol, '1min', start_time)
    data = klines.data
    try:
        record.now_price = data[data['id'] == now]['close'].item()
    except:
        print(record, datetime.ts2time(now), start_time)
        record.now_price = 0
    record.now_rate = record.now_price / record.buy_price - 1


def back_trace(
    cont_loss_list: ContLossList, param: Param,
    min_vol=10, fee_rate=0.002, days=365, end=2,
    init_money=2000, write=False, interval='1min', level='1day'
):
    start_date = datetime.ts2date(time.time()-(days+end)*86400)
    end_date = datetime.ts2date(time.time()-end*86400)
    record_path = f'{ROOT}/back_trace/csv/record_{start_date}_{end_date}_{level}.csv'
    money_record_path = f'{ROOT}/back_trace/csv/money_{start_date}_{end_date}_{level}.csv'

    data = cont_loss_list.data
    # data = cont_loss_list.dict('OGNUSDT')
    # print(data[-20:], np.diff(data['id'])[-20:],
    #       datetime.ts2time(data[-1]['id']))

    if param.buy_algo_version in [1, 2]:
        symbol = ''
        for i, each in enumerate(data):
            if symbol != each['symbol']:
                symbol = each['symbol']
                cont_loss = cont_loss_days = 0
                is_max_loss = False

            if each['rate'] < min(0, param.min_close_rate) and cont_loss_days == 0:
                cont_loss_days = 1
                cont_loss = each['rate']
                is_max_loss = True
            elif each['rate'] < param.min_close_rate and cont_loss_days > 0:
                start_i = i-cont_loss_days
                cont_loss_days += 1
                # cont_loss = data[start_i:i+1]['rate'].sum()
                cont_loss = each['close']/data[start_i]['open']-1
                is_max_loss = data[start_i:i+1]['rate'].min() >= each['rate']
            else:
                cont_loss = cont_loss_days = 0
                is_max_loss = False
            each['cont_loss_days'] = cont_loss_days
            each['cont_loss_rate'] = cont_loss
            each['is_max_loss'] = is_max_loss

        if param.buy_algo_version == 1:
            data = data[
                (param.min_buy_vol <= data['vol']) & (data['vol'] <= param.max_buy_vol) &
                (param.min_price <= data['close']) & (data['close'] <= param.max_price) &
                (
                    (
                        (data['cont_loss_days'] == 1) &
                        (data['close'] > data['boll']) &
                        (data['cont_loss_rate'] <= param.up_cont_rate)
                    ) | (
                        # (data['close'] <= data['boll']) &
                        (data['cont_loss_rate'] <= param.min_cont_rate) &
                        (data['is_max_loss'] == 1)
                    ) | (
                        # (data['close'] <= data['boll']) &
                        (data['cont_loss_rate'] <= param.break_cont_rate)
                    )
                )
            ]
        elif param.buy_algo_version == 2:
            data = data[
                (param.min_price <= data['close']) & (data['close'] <= param.max_price) &
                (
                    (
                        (data['close'] > data['boll_tmr']) &
                        (data['cont_loss_days'] == 1) &
                        (data['cont_loss_rate'] <= param.up_cont_rate) &
                        (param.min_buy_vol <= data['vol']) &
                        (data['vol'] <= param.max_buy_vol)
                    ) | (
                        (data['close'] > data['boll_tmr']) &
                        (data['cont_loss_days'] > 2) &
                        (data['cont_loss_rate'] <= param.up_small_cont_rate) &
                        # (data['is_min_loss']==1) &
                        (data['rate'] >= param.up_small_loss_rate) &
                        (data['vol'] >= param.min_up_small_buy_vol)
                    ) | (
                        (data['close'] > data['boll_tmr']) &
                        (data['cont_loss_days'] > 1) &
                        (data['cont_loss_rate'] <= param.up_break_cont_rate) &
                        (param.min_buy_vol <= data['vol']) &
                        (data['vol'] <= param.max_buy_vol)
                    ) | (
                        (data['close'] <= data['boll_tmr']) &
                        (data['cont_loss_rate'] <= param.min_cont_rate) &
                        (data['is_max_loss'] == 1) &
                        (param.min_buy_vol <= data['vol']) &
                        (data['vol'] <= param.max_buy_vol)
                    ) | (
                        (data['close'] <= data['boll_tmr']) &
                        (data['cont_loss_rate'] <= param.break_cont_rate) &
                        (param.min_buy_vol <= data['vol']) &
                        (data['vol'] <= param.max_buy_vol)
                    )
                )
            ]
    elif param.buy_algo_version == 3:
        data = data[
            (param.min_buy_vol <= data['vol-1']) & (data['vol-1'] <= param.max_buy_vol) &
            (param.min_price <= data['open']) & (data['open'] <= param.max_price) &
            # (data['rate'] < param.min_close_rate) &
            # (data['low2']/data['close']-1 <= -0.002) &
            (
                (
                    (0.99 * data['open'] > data['boll_real'])
                    & ((param.buy_rate*data['bollup_real']+(1-param.buy_rate)*data['boll_real']) > data['open'])
                    # & (data['boll_real'] > data['low'])
                    # & (data['close'] < param.high_rate * data['high'] + (1-param.high_rate)*data['low'])
                )
            )
        ]
    elif param.buy_algo_version == 4:
        data = data[
            (param.min_buy_vol <= data['vol']) & (data['vol'] <= param.max_buy_vol) &
            (param.min_price <= data['open']) & (data['open'] <= param.max_price) &
            # (data['rate'] < param.min_close_rate) &
            # (data['low2']/data['close']-1 <= -0.002) &
            (
                (
                    (data['close'] < 0.9*data['bolldown_tmr'])
                    # & ((1/16*data['bollup']+(1-1/16)*data['boll']) > data['open'])
                    # & (data['boll_real'] > data['low'])
                    # & (data['close'] < param.high_rate * data['high'] + (1-param.high_rate)*data['low'])
                )
            )
        ]

    elif param.buy_algo_version == 5:
        symbol = ''
        for i, each in enumerate(data):
            if symbol != each['symbol']:
                symbol = each['symbol']
                cont_loss_days = 0

            elif each['low'] > data[i-1]['low']:
                cont_loss_days += 1
            else:
                cont_loss_days = 0

            each['cont_loss_days'] = cont_loss_days

        data = data[
            (param.min_buy_vol <= data['vol']) & (data['vol'] <= param.max_buy_vol) &
            (param.min_price <= data['close']) & (data['close'] <= param.max_price) &
            (data['cont_loss_days'] >= 2) &
            (data['rate'] > 0) &
            (
                (
                    (data['open'] < data['boll']) &
                    (data['high'] < data['boll'])
                ) | (
                    (data['open'] >= data['boll']) &
                    (data['high'] < data['bollup'])
                )
            )
        ]

    max_money = last_money = total_money = money = init_money
    max_back_rate = 0
    money_record = []
    record_list: list[Record] = []
    holding_list: list[Record] = []

    init_date = datetime.ts2date(time.time() - (days+end-1) * 86400)
    init_ts = int(datetime.date2ts(init_date) + datetime.Tz.tz_num * 3600)

    level_coff, level_ts = get_level(level)
    # print(data, init_date, init_ts)
    for i in range(days*level_coff+1):
        ts = init_ts + i * level_ts
        date = datetime.ts2date(ts)
        # print(ts)
        targets = data[data['id'] == ts]

        # not buy if holding
        if param.buy_algo_version == 5:
            holding_symbol = [record.symbol for record in holding_list]
            targets = [
                target for target in targets if target['symbol'].decode() not in holding_symbol]

        targets_num = len(targets)
        # print(date, targets_num)
        total_buy_vol = 0
        total_sell_vol = 0
        if not targets_num:
            pass
        elif param.buy_algo_version in [1, 2]:
            # print(datetime.ts2time(ts), ts)
            targets = np.sort(targets, order='vol')[::-1]
            up_targets = targets[targets['close'] > targets['boll_tmr']]
            low_targets = targets[targets['close'] <= targets['boll_tmr']]

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

                if ((cont_loss['symbol'] == b'COCOSUSDT' and 1611014400-5*86400 <= cont_loss['id'] <= 1611014400+2*86400)
                    or (cont_loss['symbol'] == b'BTCSTUSDT' and 1615766400-5*86400 <= cont_loss['id'] <= 1615766400+2*86400)
                        or (cont_loss['symbol'] == b'DREPUSDT' and 1616976000-5*86400 <= cont_loss['id'] <= 1616976000+2*86400)):
                    continue

                buy_price, buy_time = get_buy_price_and_time(
                    cont_loss, param, date, interval, level, param.buy_algo_version
                )
                # print(datetime.ts2time(ts, tz=8), buy_price,
                #       datetime.ts2time(buy_time, tz=8) if buy_time > 0 else 0)
                # print(cont_loss)

                if buy_time == 0:
                    continue

                # TODO
                sell_price, sell_time = get_sell_price_and_time(
                    cont_loss, cont_loss_list, param, date, interval,
                    buy_price, buy_time, level, param.sell_algo_version
                )
                record = Record(
                    cont_loss['symbol'].decode(), buy_price, buy_time, buy_vol, fee_rate=fee_rate
                )
                record.sell(sell_price, sell_time)
                record_list.append(record)
                holding_list.append(record)
                total_buy_vol += buy_vol

        elif param.buy_algo_version == 3:
            targets = targets[targets['boll_real'] > targets['low']]
            low_targets_num = len(targets)
            # buy_num = param.max_num
            buy_num = max(min(param.max_num, targets_num), param.min_num)
            buy_vol = round(money / buy_num, 3)

            if buy_vol < min_vol:
                buy_vol = min_vol
                buy_num = math.floor(money // min_vol)

            if buy_vol * buy_num > money:
                buy_vol -= 0.001

            # targets = np.sort(targets, order='vol-1')[::-1][:buy_num]

            waiting_list: list[Record] = []
            holding_list: list[Record] = []
            for cont_loss in targets:
                if ((cont_loss['symbol'] == b'COCOSUSDT' and 1611014400-5*86400 <= cont_loss['id'] <= 1611014400+86400)
                    or (cont_loss['symbol'] == b'BTCSTUSDT' and 1615766400-5*86400 <= cont_loss['id'] <= 1615766400+86400)
                        or (cont_loss['symbol'] == b'DREPUSDT' and 1616976000-5*86400 <= cont_loss['id'] <= 1616976000+86400)):
                    continue

                last = cont_loss_list.data[
                    (cont_loss_list.data['symbol'] == cont_loss['symbol'])
                    & (cont_loss_list.data['id'] == ts-level_ts)
                ]
                if not last.size:
                    continue
                else:
                    last = last[0]

                # if last['close'] < (last['high']+last['low'])/2:
                #     continue
                # if last['open'] > last['boll_real'] > last['low']:
                #     continue

                buy_price, buy_time = get_buy_price_and_time(
                    cont_loss, param, date, interval, level, param.buy_algo_version
                )
                if buy_time == 0:
                    continue

                # TODO
                sell_price, sell_time = get_sell_price_and_time(
                    cont_loss, cont_loss_list, param, date, interval,
                    buy_price, buy_time, level, param.sell_algo_version
                )
                record = Record(
                    cont_loss['symbol'].decode(), buy_price, buy_time, buy_vol, fee_rate=fee_rate
                )
                record.sell(sell_price, sell_time)
                waiting_list.append(record)

            # largest
            # waiting_list = sorted(waiting_list, key=lambda record: -targets[targets['symbol']==record.symbol.encode()]['vol-1'])[:buy_num]
            # smallest
            # waiting_list = sorted(waiting_list, key=lambda record: targets[targets['symbol']==record.symbol.encode()]['vol-1'])[:buy_num]
            # earlist
            # waiting_list = sorted(waiting_list, key=lambda record: record.buy_time if record.buy_time > 0 else 1e10)[:buy_num]
            # latest
            waiting_list = sorted(
                waiting_list, key=lambda record: record.buy_time)

            # print(date, targets_num, low_targets_num, len(waiting_list))
            day_count = 0
            for record in waiting_list:
                for each in holding_list.copy():
                    if each.sell_time <= record.buy_time:
                        each.sell(each.sell_price, each.sell_time)
                        total_sell_vol += each.sell_vol
                        holding_list.remove(each)

                if day_count >= 200:
                    break
                if len(holding_list) < buy_num:
                    record_list.append(record)
                    holding_list.append(record)
                    total_buy_vol += record.buy_vol
                    day_count += 1
                else:
                    # sell most profit
                    for each in holding_list:
                        get_record_now_rate(each, ts, record.buy_time)

                    holding_list = sorted(
                        holding_list, key=lambda x: -x.now_rate)
                    sell_target = holding_list[0]
                    # print(holding_list)
                    if sell_target.now_rate > 0.01:
                        sell_target.sell(
                            sell_target.now_price, record.buy_time)
                        # print(sell_target)
                        holding_list.remove(sell_target)
                        total_sell_vol += sell_target.sell_vol

                        record.buy(record.buy_price, record.buy_time, min(
                            record.buy_vol, sell_target.sell_vol))
                        record_list.append(record)
                        holding_list.append(record)
                        total_buy_vol += record.buy_vol
                        day_count += 1

        elif param.buy_algo_version == 4:
            targets = np.sort(targets, order='vol')[::-1]

            buy_num = int(min(max(targets_num, param.min_num), param.max_num))
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

                if ((cont_loss['symbol'] == b'COCOSUSDT' and 1611014400-5*86400 <= cont_loss['id'] <= 1611014400+86400)
                    or (cont_loss['symbol'] == b'BTCSTUSDT' and 1615766400-5*86400 <= cont_loss['id'] <= 1615766400+86400)
                        or (cont_loss['symbol'] == b'DREPUSDT' and 1616976000-5*86400 <= cont_loss['id'] <= 1616976000+86400)):
                    continue

                buy_price, buy_time = cont_loss['close'], cont_loss['id2']

                if buy_time == 0:
                    continue

                # TODO
                sell_price, sell_time = get_sell_price_and_time(
                    cont_loss, cont_loss_list, param, date, interval, buy_price, buy_time, param.sell_algo_version
                )
                record = Record(
                    cont_loss['symbol'].decode(), buy_price, buy_time, buy_vol, fee_rate=fee_rate
                )
                record.sell(sell_price, sell_time)
                record_list.append(record)
                holding_list.append(record)
                total_buy_vol += buy_vol

        elif param.buy_algo_version == 5:
            # print(datetime.ts2time(ts), ts)
            targets = np.sort(targets, order='vol')[::-1]
            # up_targets = targets[targets['close']>targets['boll_tmr']]
            # low_targets = targets[targets['close']<=targets['boll_tmr']]

            buy_num = int(min(max(targets_num, param.min_num), param.max_num))
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

                if ((cont_loss['symbol'] == b'COCOSUSDT' and 1611014400-5*86400 <= cont_loss['id'] <= 1611014400+2*86400)
                    or (cont_loss['symbol'] == b'BTCSTUSDT' and 1615766400-5*86400 <= cont_loss['id'] <= 1615766400+2*86400)
                        or (cont_loss['symbol'] == b'DREPUSDT' and 1616976000-5*86400 <= cont_loss['id'] <= 1616976000+2*86400)):
                    continue

                buy_price, buy_time = get_buy_price_and_time(
                    cont_loss, param, date, interval, level, param.buy_algo_version
                )

                if buy_time == 0:
                    continue

                sell_price, sell_time = get_sell_price_and_time(
                    cont_loss, cont_loss_list, param, date, interval,
                    buy_price, buy_time, level, param.sell_algo_version
                )
                record = Record(
                    cont_loss['symbol'].decode(), buy_price, buy_time, buy_vol, fee_rate=fee_rate
                )
                record.sell(sell_price, sell_time)
                record_list.append(record)
                holding_list.append(record)
                total_buy_vol += buy_vol

        for each in holding_list.copy():
            if each.sell_time <= ts+level_ts:
                each.sell(each.sell_price, each.sell_time)
                total_sell_vol += each.sell_vol
                holding_list.remove(each)

        holding_money = 0
        for record in holding_list:
            base_klines = cont_loss_list.dict(record.symbol)
            try:
                close = base_klines[base_klines['id'] == ts]['close'][0]
            except Exception as e:
                print(record.symbol, ts, datetime.ts2time(ts))
                close = base_klines[base_klines['id'] <= ts]['close'][-1]
            holding_money += record.amount * close

        money -= total_buy_vol
        money += total_sell_vol
        total_money = money + holding_money
        day_profit = total_money - last_money
        day_profit_rate = get_rate(total_money, last_money, -1)
        max_money = max(max_money, total_money)
        day_back_rate = get_rate(total_money, max_money, -1)
        all_rate = get_rate(total_money, init_money, -1)
        money_record.append([
            date if level == '1day' else datetime.ts2time(
                ts, fmt='%Y-%m-%d %H'),
            total_buy_vol, total_sell_vol, money, holding_money, total_money,
            all_rate, day_profit, day_profit_rate, day_back_rate
        ])
        last_money = total_money

    profit_rate = total_money / init_money
    max_back_rate = min([e[-1] for e in money_record])
    # print(total_money, profit_rate, max_back_rate)

    if write:
        print(total_money, profit_rate, max_back_rate, len(record_list))
        Record.write_csv(record_list, record_path)

        with open(money_record_path, 'w+') as f:
            f.write('日期,买入额,卖出额,可用额,持有币价值,总资金,累计收益率,日收益,日收益率,回撤\n')
            for record in money_record:
                f.write(','.join([str(e) for e in record])+'\n')
    return total_money, profit_rate, max_back_rate


@retry(tries=5, logger=logger)
def get_detailed_klines(symbol, interval, start_time, level):
    def is_full():
        if (
            (file_start_time == 1618876800)
            or (file_start_time == 1619308800)
            or (file_start_time == 1621382400)
            or (file_start_time == 1628812800)
            or (file_start_time == 1632873600)
            or (file_start_time == 1614988800)
            or (file_start_time == 1608508800)
            or (file_start_time == 1613001600)
            or (file_start_time == 1606694400)
            or (file_start_time == 1608854400)
            or (file_start_time == 1593302400)
            or (file_start_time == 1639958400 and symbol == 'BZRXUSDT')
            or (file_start_time == 1617321600 and symbol == 'DREPUSDT')
            or (file_start_time == 1583280000 and symbol == 'FETUSDT')
            or (file_start_time == 1573603200 and symbol == 'HOTUSDT')
            or (file_start_time == 1623974400 and symbol == 'SUNUSDT')
            or (file_start_time == 1623974400 and symbol == 'SUSHIUPUSDT')
            or (file_start_time == 1582070400 and symbol == 'VETUSDT')
        ):
            return False
        return True

    level_coff, _ = get_level(interval)
    file_start_time = int(start_time // 86400 * 86400)
    path = f'{ROOT}/back_trace/npy/detail/{symbol}_{file_start_time}_{interval}.npy'
    # print(path)
    if os.path.exists(path):
        klines = Klines.load(path)
        if len(klines.data) < level_coff:
            if is_full():
                os.remove(path)
                raise Exception(
                    f'Saved klines {path.split("/")[-1].split(".")[0]} are not enough, hope {level_coff} but have {len(klines.data)}')
        if klines.data[0]['id'] != file_start_time:
            if (
                not (file_start_time == 1623974400 and symbol == 'SUNUSDT')
            ):
                os.remove(path)
                raise Exception(
                    f'Saved klines {path.split("/")[-1].split(".")[0]} are not matched, hope {file_start_time} but start at {klines.data[0]["id"]}')
    else:
        try:
            raw_klines = Global.user.market.get_candlestick(
                symbol, interval,
                start_ts=file_start_time, end_ts=file_start_time+86400
            )
        except Exception as e:
            time.sleep(30)
            raise e
        if file_start_time < time.time() - 86400 and len(raw_klines) < level_coff:
            if is_full():
                time.sleep(3)
                raise Exception(
                    f'Klines {path.split("/")[-1].split(".")[0]} are not enough, hope {level_coff} but have {len(raw_klines)}')
        if raw_klines[-1].id != file_start_time:
            if (
                not (file_start_time == 1623974400 and symbol == 'SUNUSDT')
            ):
                time.sleep(3)
                raise Exception(
                    f'Klines {path.split("/")[-1].split(".")[0]} are not matched, hope {file_start_time} but start at {raw_klines[-1].id}')

        raw_klines.reverse()
        klines = Klines()
        klines.load_from_raw(symbol, raw_klines)
        klines.save(path)
    return klines


@retry(tries=2, delay=30)
def rm_detailed_klines(symbol, interval, start_time, level):
    file_start_time = int(start_time // 86400 * 86400)
    path = f'{ROOT}/back_trace/npy/detail/{symbol}_{file_start_time}_{interval}.npy'
    if os.path.exists(path):
        os.remove(path)


def buy_detailed_back_trace(
    symbol, start_time, buy_price, low_price,
    max_buy_ts=0, interval='1min', level='1day'
):
    _, level_ts = get_level(level)
    klines = get_detailed_klines(symbol, interval, start_time, level)
    data = klines.data
    if max_buy_ts == 0:
        max_buy_ts = level_ts - 2 * 60

    stop_buy_ts = start_time + min(level_ts, max_buy_ts)

    try:
        # print(datetime.ts2time(start_time), start_time, datetime.ts2time(int(start_time // 86400 * 86400)), int(start_time // 86400 * 86400), len(data))
        # print(low_price, data['high'].max(), datetime.ts2time(data['id'][0]))
        # print(data[(data['high'] >= low_price)])
        if CHECK_PREVIOUS:
            low_kline = data[
                (data['id'] >= start_time) &
                (data['high'] >= low_price)
            ][0]
        else:
            low_kline = data[
                # (data['id'] >= start_time) &
                (data['high'] >= low_price)
            ][0]
        low_ts = low_kline['id']
        # print(low_ts, stop_buy_ts)
        stop_buy_ts = min(low_ts, stop_buy_ts)
        # print(stop_buy_ts - start_time)
    except Exception as e:
        # print(e)
        pass

    try:
        buy_time = data[
            (data['id'] >= start_time) &
            (data['low'] < buy_price) &
            (data['id'] < stop_buy_ts)
        ]['id'][0]
    except:
        buy_time = 0
    # print(start_time, stop_buy_ts-start_time, buy_time-start_time)

    return buy_price, buy_time


@retry(3)
def sell_detailed_back_trace(
    symbol, start_time, buy_time, high_price, high_back_price,
    low_price, low_back_price, stop_loss_price, interval='1min', level='1day'
):
    _, level_ts = get_level(level)

    klines = get_detailed_klines(symbol, interval, start_time, level)
    data = klines.data
    data = data[(data['id'] > buy_time) & (data['id'] < start_time + level_ts)]
    try:
        end_ts = data[-1]['id']
        end_price = data[-1]['close']
        # print(datetime.ts2time(data[0]['id']), stop_loss_price)
    except Exception as e:
        print(symbol, start_time, buy_time, start_time +
              level_ts, klines.data[-1]['id'])
        rm_detailed_klines(symbol, interval, start_time, level)
        raise e

    try:
        high_ts = data[data['high'] >= high_price]['id'][0]
        high_back_ts = data[(data['low'] <= high_back_price)
                            & (data['id'] > high_ts)]['id'][0]
    except:
        high_back_ts = end_ts + 1

    try:
        low_ts = data[data['high'] >= low_price]['id'][0]
        low_back_ts = data[(data['low'] <= low_back_price)
                           & (data['id'] > low_ts)]['id'][0]
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


def sell_detailed_back_trace_unfixed(
    symbol, start_time, buy_time, high_price,
    high_back_rate, high_hold_time, low_price,
    low_back_price, stop_loss_price, interval='1min', level='1day'
):
    _, level_ts = get_level(level)

    klines = get_detailed_klines(symbol, interval, start_time, level)
    data = klines.data
    data = data[(data['id'] > buy_time) & (data['id'] < start_time + level_ts)]
    end_ts = data[-1]['id']
    end_price = data[-1]['close']
    open_price = data[0]['open']
    # print(datetime.ts2time(data[0]['id']), stop_loss_price)
    try:
        high_ts = data[data['high'] >= high_price]['id'][0]
        max_high_hold_time = high_ts + high_hold_time
        cum_high_back = high_back_rate * \
            np.maximum.accumulate(data['high']) + \
            (1-high_back_rate) * open_price
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
        low_back_ts = data[(data['low'] <= low_back_price)
                           & (data['id'] > low_ts)]['id'][0]
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


def sell_detailed_next_back_trace(
    symbol, start_time, stop_profit_price,
    stop_loss_price, interval='1min', level='1day'
):
    _, level_ts = get_level(level)

    klines = get_detailed_klines(symbol, interval, start_time, level)
    data = klines.data
    data = data[(data['id'] > start_time) & (
        data['id'] < start_time + level_ts)]
    end_ts = data[-1]['id']
    # print(datetime.ts2time(data[0]['id']), stop_loss_price)
    try:
        stop_profit_ts = data[data['high'] >= stop_profit_price]['id'][0]
    except:
        stop_profit_ts = end_ts + 1

    try:
        stop_loss_ts = data[data['low'] <= stop_loss_price]['id'][0]
        stop_loss_price = min(
            stop_loss_price, data[data['id'] == stop_loss_ts]['open'][0])
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


def get_buy_price_and_time(cont_loss, param: Param, date, interval, level, version=1):
    if version == 1:
        return get_buy_price_and_time_v1(cont_loss, param, date, interval, level)
    elif version == 2:
        return get_buy_price_and_time_v2(cont_loss, param, date, interval, level)
    elif version == 3:
        return get_buy_price_and_time_v3(cont_loss, param, date, interval, level)
    elif version == 5:
        return get_buy_price_and_time_v5(cont_loss, param, date, interval, level)


def get_buy_price_and_time_v1(cont_loss, param: Param, date, interval, level):
    symbol = cont_loss['symbol'].decode()

    if True:
        close = cont_loss['close']
        buy_price = close * (1 + param.buy_rate)
        low_price = close * (1 + param.low_rate)
        start_time = cont_loss['id2']
        _, buy_time = buy_detailed_back_trace(
            symbol, start_time, buy_price, low_price, param.max_buy_ts, interval=interval, level=level
        )

        # Global.sell_dict[key] = (buy_price, buy_time)
        return buy_price, buy_time


def get_buy_price_and_time_v2(cont_loss, param: Param, date, interval, level):
    symbol = cont_loss['symbol'].decode()
    _, level_ts = get_level(level)

    if True:
        close = cont_loss['close']
        tmr_mark_price = np.array([
            # pos 0
            cont_loss['bollup_tmr'],
            # pos 1
            cont_loss['bollfake1_tmr'],
            # pos 2
            cont_loss['bollmidup_tmr'],
            # pos 3
            cont_loss['bollfake2_tmr'],
            # pos 4
            cont_loss['boll_tmr'],
            # pos 5
            cont_loss['bollfake3_tmr'],
            # pos 6
            cont_loss['bollmiddown_tmr'],
            # pos 7
            cont_loss['bollfake4_tmr'],
            # pos 8
            cont_loss['bolldown_tmr']
            # pos 9
        ])
        close_pos = tmr_mark_price[tmr_mark_price > close].size
        if close_pos <= 0:
            buy_price = tmr_mark_price[0] * (1+param.buy_up_rate)
        elif close_pos >= tmr_mark_price.size:
            buy_price = close
        elif close_pos == 1:
            buy_price = tmr_mark_price[1] * (1+param.buy_up_rate)
        elif cont_loss['boll'] > cont_loss['open'] > close:
            buy_price = cont_loss['bolldown_tmr'] * (1+param.buy_up_rate)
        else:
            up_price = tmr_mark_price[close_pos-1]
            down_price = tmr_mark_price[close_pos]

            if (close - down_price) / (up_price - down_price) > param.up_near_rate_fake and not close_pos % 2:
                # 2,4,6,8
                buy_price = close
            elif (close - down_price) / (up_price - down_price) > param.up_near_rate and close_pos % 2:
                # 3,5,7,9
                buy_price = close
            # elif cont_loss['low'] < down_price:
            #     buy_price = close
            # elif low < mark_price[close_pos] and (close - down_price) / (up_price - down_price) < param.low_near_rate and not close_pos % 2:
            #     buy_price = close
            else:
                buy_price = down_price * (1+param.buy_up_rate)

        # buy_price = buy_price * (1+param.buy_up_rate)
        low_price = close * (1 + param.low_rate)
        start_time = cont_loss['id']+level_ts
        _, buy_time = buy_detailed_back_trace(
            symbol, start_time, buy_price, low_price, param.max_buy_ts, interval=interval, level=level
        )
        # if buy_time:
        #     print(f'buy {symbol}, start at {datetime.ts2time(start_time)} with price {buy_price} and low mark {low_price}')
        #     print(f'finish buy as {datetime.ts2time(buy_time)}')
        # Global.sell_dict[key] = (buy_price, buy_time)
        return buy_price, buy_time


def get_buy_price_and_time_v3(cont_loss, param: Param, date, interval, level):
    symbol = cont_loss['symbol'].decode()

    if True:
        buy_price = cont_loss['boll_real']
        low_price = cont_loss['bollup_real']
        start_time = cont_loss['id']
        _, buy_time = buy_detailed_back_trace(
            symbol, start_time, buy_price, low_price, param.max_buy_ts, interval=interval, level=level
        )
        return buy_price, buy_time


def get_buy_price_and_time_v5(cont_loss, param: Param, date, interval, level):
    return cont_loss['close'], cont_loss['id2']


def get_sell_price_and_time(cont_loss, cont_loss_list, param: Param, date, interval, buy_price, buy_time, level, version=1):
    if version == 1:
        return get_sell_price_and_time_v1(cont_loss, cont_loss_list, param, date, interval, buy_price, buy_time, level)
    elif version == 2:
        return get_sell_price_and_time_v2(cont_loss, cont_loss_list, param, date, interval, buy_price, buy_time, level)
    elif version == 3:
        return get_sell_price_and_time_v3(cont_loss, cont_loss_list, param, date, interval, buy_price, buy_time, level)
    elif version == 4:
        return get_sell_price_and_time_v4(cont_loss, cont_loss_list, param, date, interval, buy_price, buy_time, level)
    elif version == 5:
        return get_sell_price_and_time_v5(cont_loss, cont_loss_list, param, date, interval, buy_price, buy_time, level)


def get_sell_price_and_time_v1(cont_loss, cont_loss_list, param: Param, date, interval, buy_price, buy_time, level):
    symbol = cont_loss['symbol'].decode()

    if True:
        if SELL_AS_BUY:
            close = buy_price
        else:
            close = cont_loss['close']
        # high_price = max(
        #     (cont_loss['open'] + cont_loss['close']) / 2,
        #     close * (1 + high_rate)
        # )
        high_price = close * (1+param.high_rate)
        high_back_price = close * (1+param.high_back_rate)
        # print(high_price, high_back_price, close)
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
        final_price = close * (1 + param.final_rate)
        clear_price = buy_price * (1 + param.clear_rate)
        stop_loss_price = buy_price * (1 + param.stop_loss_rate)

        # sell_price = cont_loss['close2']
        # sell_time = cont_loss['id2']+86340

        if cont_loss['high2'] < low_price and cont_loss['low2'] > 0:
            if cont_loss['close2'] >= clear_price:
                sell_price = cont_loss['close2']
                sell_time = cont_loss['id2']+86340
            else:
                klines = cont_loss_list.dict(symbol)
                i = np.where(klines['id'] == cont_loss['id'])[0][0]
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
                            sell_price = min(
                                stop_loss_price, day_kline['open'])
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
            sell_price, sell_time = sell_detailed_back_trace(
                symbol, start_time, buy_time,
                high_price, high_back_price,
                low_price, low_back_price, 0, interval=interval, level=level
            )

        # Global.sell_dict[key] = (sell_price, sell_time)
        return sell_price, sell_time


def get_sell_price_and_time_v2(cont_loss, cont_loss_list, param: Param, date, interval, buy_price, buy_time, level):
    symbol = cont_loss['symbol'].decode()
    _, level_ts = get_level(level)

    if True:
        if SELL_AS_BUY:
            close = buy_price
        else:
            close = cont_loss['close']

        high_price = close * (1+param.high_rate)
        high_back_price = close * (1 + param.high_back_rate)
        low_price = close * (1 + param.low_rate)
        low_back_price = close * (1 + param.low_back_rate)
        clear_price = buy_price * (1 + param.clear_rate)
        stop_loss_price = buy_price * (1 + param.stop_loss_rate)

        # print(cont_loss)
        # print(f'{symbol} buy at {datetime.ts2time(buy_time)} price {buy_price}')
        # print(f'high {high_price}, high back {high_back_price}')
        # print(f'low {low_price}, low back {low_back_price}')
        # print(f'clear {clear_price}, stop loss {stop_loss_price}')
        # print(f'tmr high {cont_loss["high2"]}, low {cont_loss["low2"]}, close {cont_loss["close2"]}')

        if cont_loss['high2'] < low_price and cont_loss['low2'] > stop_loss_price:
            if cont_loss['close2'] >= clear_price or param.max_hold_days == 1:
                sell_price = cont_loss['close2']
                sell_time = cont_loss['id2']+level_ts - 60
            else:
                klines = cont_loss_list.dict(symbol)
                i = np.where(klines['id'] == cont_loss['id'])[0][0]
                if i+2 >= len(klines):  # 买入当天就是最后一天 卖出
                    sell_price = klines[-1]['close']
                    sell_time = klines[-1]['id']+level_ts - 120
                else:
                    # for day_kline in klines[i+2:i+param.max_hold_days+1]:
                    for j in range(i+2, min(i+param.max_hold_days+1, klines.size)):
                        day_kline = klines[j]
                        yesterday_kline = klines[j-1]
                        open_price = day_kline['open']
                        boll = np.array([
                            yesterday_kline['bollup_tmr'],
                            yesterday_kline['bollmidup_tmr'],
                            yesterday_kline['boll_tmr'],
                            yesterday_kline['bollmiddown_tmr'],
                            yesterday_kline['bolldown_tmr'],
                        ])
                        pos = boll[boll > open_price].size
                        if pos == 0:
                            final_price = open_price
                        else:
                            final_price = boll[pos-1]
                            diff = boll[0]-boll[1]
                            if (final_price-open_price)/diff < param.final_rate:
                                final_price += diff * param.final_modify_rate

                        final_price = final_price * (1+param.sell_down_rate)

                        if day_kline['high'] > final_price and day_kline['low'] <= stop_loss_price:
                            # 分不清止损后止盈的先后
                            sell_price, sell_time = sell_detailed_next_back_trace(
                                symbol, day_kline['id'], final_price, stop_loss_price, interval=interval, level=level
                            )
                            break
                        elif day_kline['low'] <= stop_loss_price:
                            # 止损
                            sell_price = min(
                                stop_loss_price, day_kline['open'])
                            sell_time = day_kline['id']+level_ts - 180
                            # print('~', sell_price, datetime.ts2time(sell_time))
                            break
                        elif day_kline['high'] > final_price:
                            # 止盈
                            sell_price = final_price
                            sell_time = day_kline['id']+level_ts-240
                            break
                    else:
                        # 超时
                        sell_price = day_kline['close']
                        sell_time = day_kline['id']+level_ts-120
                    # print(sell_price)

        else:
            start_time = cont_loss['id']+level_ts
            # sell_price, sell_time = sell_detailed_back_trace(
            #     symbol, start_time, buy_time,
            #     high_price, param.high_back_rate, param.high_hold_time,
            #     low_price, low_back_price, 0, interval=interval
            # )
            sell_price, sell_time = sell_detailed_back_trace(
                symbol, start_time, buy_time,
                high_price, high_back_price,
                low_price, low_back_price, stop_loss_price, interval=interval, level=level
            )
        # print(f'sell at {datetime.ts2time(sell_time)}, price {sell_price}')
        # Global.sell_dict[key] = (sell_price, sell_time)
        return sell_price, sell_time


def get_sell_price_and_time_v3(cont_loss, cont_loss_list, param: Param, date, interval, buy_price, buy_time, level):
    symbol = cont_loss['symbol'].decode()

    if True:
        low_price = cont_loss['bollup_real']
        low_back_price = cont_loss['bollfake1_real']
        # stop_loss_price = buy_price * (1 + param.stop_loss_rate)
        stop_loss_price = (
            1.25*cont_loss['boll_real'] - 0.25*cont_loss['bollup_real']) * (1 + param.stop_loss_rate)
        # stop_loss_price = 0

        if cont_loss['high'] < low_price and cont_loss['low'] > stop_loss_price:
            sell_price = cont_loss['close']
            sell_time = cont_loss['id']+86340

        else:
            start_time = cont_loss['id']
            sell_price, sell_time = sell_detailed_back_trace(
                symbol, start_time, buy_time,
                buy_price * 10, buy_price * 9,
                low_price, low_back_price, stop_loss_price, interval=interval, level=level
            )

        return sell_price, sell_time


def get_sell_price_and_time_v4(cont_loss, cont_loss_list, param: Param, date, interval, buy_price, buy_time, level):
    symbol = cont_loss['symbol'].decode()

    if True:
        if SELL_AS_BUY:
            close = buy_price
        else:
            close = cont_loss['close']

        klines = cont_loss_list.dict(symbol)
        i = np.where(klines['id'] == cont_loss['id'])[0][0]
        if i+1 >= len(klines):
            sell_price = klines[-1]['close']
            sell_time = klines[-1]['id']+86340
        else:
            for day_kline in klines[i+1:i+param.max_hold_days+1]:
                if day_kline['close'] > day_kline['bolldown']:
                    sell_price = day_kline['close']
                    sell_time = day_kline['id']+86340
                    # print('~', sell_price, datetime.ts2time(sell_time))
                    break
                # elif day_kline['high'] > final_price:
                #     sell_price = final_price
                #     sell_time = day_kline['id']+86340
                #     break
            else:
                sell_price = day_kline['close']
                sell_time = day_kline['id']+86340

        return sell_price, sell_time


def get_sell_price_and_time_v5(cont_loss, cont_loss_list, param: Param, date, interval, buy_price, buy_time, level):
    symbol = cont_loss['symbol'].decode()
    _, level_ts = get_level(level)
    stop_loss_price = buy_price * (1 + param.stop_loss_rate)

    klines = cont_loss_list.dict(symbol)
    i = np.where(klines['id'] == cont_loss['id'])[0][0]
    max_num = len(klines)
    for j in range(i+1, max_num-1):
        day_kline = klines[j]
        if (j-i) % 2 == 1 and j != i+1 and day_kline['cont_loss_days'] < 2:
            sell_price = day_kline['close']
            sell_time = day_kline['id'] + level_ts - 120
            return sell_price, sell_time

    day_kline = klines[-1]
    sell_price = day_kline['close']
    sell_time = day_kline['id'] + level_ts - 60
    return sell_price, sell_time


def get_data(days=365, end=2, load=True, min_before=180, level='1day',
             klines_dict=None, cont_loss_list=None, filter_=True, force_update=False):
    _, level_ts = get_level(level)
    now = time.time()
    start_date = datetime.ts2date(now-(days+end)*86400)
    end_date = datetime.ts2date(now-end*86400)
    start_ts = int(datetime.date2ts(start_date))
    end_ts = int(datetime.date2ts(end_date))

    special_symbols = SPECIAL_SYMBOLS
    cont_loss_list_path = f'{ROOT}/back_trace/npy/cont_list_{BOLL_N}{"" if level == "1day" else "_"+level}.npy'
    klines_dict_path = f'{ROOT}/back_trace/npy/base_klines_dict{"" if level == "1day" else "_"+level}.npy'
    # cont_loss_csv_path = f'{ROOT}/test/csv/cont_loss_{start_date}_{end_date}.csv'
    market = Global.user.market
    if klines_dict:
        max_ts = klines_dict.data['id'].max()
        symbols = []
    elif load and os.path.exists(klines_dict_path):
        klines_dict = BaseKlineDict.load(klines_dict_path)
        dict_symbols = klines_dict.dict()
        all_symbols = market.all_symbol_info.keys()
        max_ts = klines_dict.data['id'].max()
        if force_update:
            symbols = [
                each for each in all_symbols
                if each.encode() not in special_symbols
            ]
            # symbols = [b'OGNUSDT']
        else:
            symbols = [
                each for each in all_symbols
                if each.encode() not in dict_symbols
                and each.encode() not in special_symbols
            ]

            for each in dict_symbols:
                max_ts = klines_dict.dict(each)['id'].max()
                # min_ts = klines_dict.dict(each)['id'].min()
                if end_ts > max_ts and each not in special_symbols:
                    symbols.append(each.decode())

    else:
        klines_dict = BaseKlineDict()
        max_ts = 0
        all_symbols = market.all_symbol_info.keys()
        symbols = [
            each for each in all_symbols
            and each.encode() not in special_symbols
        ]

    if len(symbols):
        temp_list = []

        def worker(symbol):
            try:
                data = klines_dict.dict(symbol)
                if data.size:
                    _start_ts = data['id'].max()
                else:
                    _start_ts = start_ts
                if force_update:
                    _start_ts -= 500 * level_ts

                klines = market.get_candlestick(
                    symbol, level,
                    start_ts=_start_ts,
                    end_ts=end_ts+86400
                )
                # klines_dict.load_from_raw(symbol, klines)
                klines = [(
                    symbol, kline.id, kline.open, kline.close,
                    kline.high, kline.low, kline.vol
                ) for kline in klines]
                temp_list.extend(klines)
            except Exception as e:
                print(e)

        run_thread_pool([(worker, (symbol,)) for symbol in symbols], True, 4)
        temp_data = np.array(temp_list, dtype=klines_dict.dtype)
        # print(temp_data[temp_data['id']==1649937600])
        # print(klines_dict.dict('OGNUSDT')[klines_dict.dict('OGNUSDT')['id']==1649937600])
        klines_dict.data = np.unique(np.concatenate((klines_dict.data, temp_data)))
        # klines_dict.data = np.concatenate((klines_dict.data, temp_data))
        # klines_dict.data = np.unique(klines_dict.data)
        # print(klines_dict.dict('OGNUSDT')[klines_dict.dict('OGNUSDT')['id']==1649937600])
        klines_dict.data.sort(order=['symbol', 'id', 'vol'])

        pos_list = np.array([], dtype=int)
        for symbol in klines_dict.dict():
            data = klines_dict.dict(symbol)
            for each in data[np.where(np.diff(data['id']) == 0)[0]]:
                ts = each['id']
                max_vol = data[data['id'] == ts]['vol'].max()
                pos = list(np.where(
                    (klines_dict.data['id'] == ts) &
                    (klines_dict.data['symbol'] == symbol) &
                    (klines_dict.data['vol'] < max_vol)
                )[0])
                # print(datetime.ts2time(ts, tz=8), pos, max_vol, klines_dict.data[pos])
                pos_list = np.concatenate((pos_list, pos))

        pos_list = np.unique(pos_list).astype(int)
        # print(pos_list)
        if pos_list.size:
            klines_dict.data = np.delete(klines_dict.data, pos_list)
        klines_dict.save(klines_dict_path)

    if cont_loss_list:
        max_ts = cont_loss_list.data['id'].max()
        min_ts = cont_loss_list.data['id'].min()
        symbols = []
    elif load and os.path.exists(cont_loss_list_path):
        cont_loss_list = ContLossList.load(cont_loss_list_path)
        if force_update:
            symbols = klines_dict.dict()
            # symbols = [b'OGNUSDT']
        else:
            symbols = []
            for symbol in klines_dict.dict():
                data = cont_loss_list.dict(symbol)
                if data.size:
                    max_ts = data['id'].max()
                    min_ts = data['id'].min()
                    if end_ts > max_ts and symbol not in special_symbols:
                        symbols.append(symbol)
                else:
                    symbols.append(symbol)
    else:
        cont_loss_list = ContLossList()
        max_ts = 0
        min_ts = now
        symbols = klines_dict.dict()

    if len(symbols):
        print('cont_loss_list', symbols)
        temp_list = []
        for symbol in symbols:
            data = klines_dict.dict(symbol)
            ori_data = cont_loss_list.dict(symbol)

            new_idx = np.where(
                np.isin(data['id'], ori_data[ori_data['id2'] == 0]['id']) +
                ~np.isin(data['id'], ori_data['id'])
            )[0]
            new_idx.sort()
            print(symbol, new_idx)
            rate_list = data['close']/data['open'] - 1
            min_ts = data['id'].min()
            # max_ts = ori_data['id'].max()
            cont_loss_days_dict = {}
            cont_loss_days = cont_loss_rate = 0
            for i in new_idx:
                ts = data[i]['id']
                last_ts = data[i-1]['id']
                item = data[i]
                date = datetime.ts2date(ts)

                if i-1 in cont_loss_days_dict:
                    cont_loss_days = cont_loss_days_dict[i-1]
                elif ori_data.size and ori_data[ori_data['id'] == last_ts].size:
                    last_ori_item = ori_data[ori_data['id'] == last_ts][0]
                    cont_loss_days = cont_loss_days_dict[i -
                                                         1] = last_ori_item['cont_loss_days']
                else:
                    cont_loss_days = cont_loss_days_dict[i-1] = 0

                rate = rate_list[i]

                if rate < 0:
                    # print(item, , cont_loss_days, i)
                    cont_loss_days += 1
                    cont_loss_rate = data[i]['close'] / \
                        data[i-cont_loss_days+1]['open']-1
                    is_max_loss = rate_list[i -
                                            cont_loss_days+1:i+1].min() == rate
                    is_min_loss = rate_list[i -
                                            cont_loss_days+1:i+1].max() == rate
                else:
                    cont_loss_days = cont_loss_rate = is_max_loss = is_min_loss = 0

                cont_loss_days_dict[i] = cont_loss_days

                try:
                    _, id2, open2, close2, high2, low2, vol2 = data[i+1]
                except IndexError:
                    id2 = open2 = close2 = high2 = low2 = vol2 = 0

                try:
                    vol_last = data[i-1]['vol']
                except IndexError:
                    vol_last = 0

                today_boll = get_boll(
                    data['close'][i-BOLL_N+1:i+1],
                    m=[2, 1.5, 1, 0.5, -0.5, -1, -1.5, -2]
                )
                tmr_boll = get_boll(
                    np.append(data['close'][i-BOLL_N+2:i+1], data['close'][i]),
                    m=[2, 1.5, 1, 0.5, -0.5, -1, -1.5, -2]
                )
                price_list = data['close'][i-BOLL_N+1:i]

                try:
                    boll_real = get_real_boll(price_list, 0)
                    bollup_real = get_real_boll(price_list, 2)
                    bollfake1_real = get_real_boll(price_list, 1.5)
                except Exception as e:
                    print(data[i])
                    raise e

                temp_list.append((
                    *data[i], id2, open2, close2, high2, low2, vol2, vol_last,
                    date, rate, cont_loss_days,
                    cont_loss_rate, is_max_loss, is_min_loss,
                    *today_boll, *tmr_boll,
                    boll_real, bollup_real, bollfake1_real, (
                        data[i]['id']-min_ts)//86400
                ))

        cont_loss_list.data = np.unique(np.concatenate([
            cont_loss_list.data,
            np.array(temp_list, dtype=ContLossList.dtype)
        ]))

        cont_loss_list.data.sort(order=['symbol', 'id'])

        pos_list = np.array([], dtype=int)
        for symbol in cont_loss_list.dict():
            data = cont_loss_list.dict(symbol)
            for each in data[np.where(np.diff(data['id']) == 0)[0]]:
                ts = each['id']
                max_vol = data[data['id'] == ts]['vol'].max()
                max_vol2 = data[data['id'] == ts]['vol2'].max()
                pos_list = np.concatenate((
                    pos_list,
                    np.where(
                        (cont_loss_list.data['id'] == ts) &
                        (cont_loss_list.data['symbol'] == symbol) &
                        ((cont_loss_list.data['vol'] < max_vol) | (
                            cont_loss_list.data['id2'] == 0))
                        # (cont_loss_list.data['vol']<max_vol)
                    )[0],
                    np.where(
                        (cont_loss_list.data['id'] == ts) &
                        (cont_loss_list.data['symbol'] == symbol) &
                        (cont_loss_list.data['vol'] == max_vol) &
                        (cont_loss_list.data['vol2'] == max_vol2)
                    )[0][:-1]
                ))

        pos_list = np.unique(pos_list).astype(int)
        if pos_list.size:
            cont_loss_list.data = np.delete(cont_loss_list.data, pos_list)
        cont_loss_list.save(cont_loss_list_path)

    if filter_:
        data = cont_loss_list.data
        cont_loss_list = ContLossList()
        cont_loss_list.data = data[
            (data['index'] > min_before) &
            (data['id'] >= start_ts) &
            (data['id'] < end_ts) &
            (np.isin(data['symbol'], SPECIAL_SYMBOLS, invert=True))
            # (data['rate']<0)
        ]

    return cont_loss_list, klines_dict


def create_random_cont_loss_list(cont_loss_list: ContLossList, random, num, boll_n=BOLL_N, level='1day'):
    np.random.seed(num)
    cont_loss_list_path = f'{ROOT}/back_trace/npy/cont_list_{boll_n}_{random}_{num}{"" if level == "1day" else "_"+level}.npy'
    symbols = cont_loss_list.dict()
    cont_loss_list.data['close'] *= (np.random.rand(
        cont_loss_list.data.size) * 2 * random + 1 - random)
    temp_list = []
    for symbol in symbols:
        data = cont_loss_list.dict(symbol)
        data['open'][1:] = data['close'][:-1]
        rate_list = data['close']/data['open'] - 1
        min_ts = data['id'].min()

        cont_loss_days = cont_loss_rate = 0
        for i, rate in enumerate(rate_list):
            if rate < 0:
                cont_loss_days += 1
                cont_loss_rate = data[i]['close'] / \
                    data[i-cont_loss_days+1]['open']-1
                is_max_loss = rate_list[i-cont_loss_days+1:i+1].min() == rate
                is_min_loss = rate_list[i-cont_loss_days+1:i+1].max() == rate
            else:
                cont_loss_days = cont_loss_rate = is_max_loss = is_min_loss = 0

            date = datetime.ts2date(data[i]['id'])
            today = data[i]
            today_info = [
                today['symbol'],
                today['id'],
                today['open'],
                today['close'],
                today['high'],
                today['low'],
                today['vol'],
            ]
            try:
                tmr = data[i+1]
                tmr_info = [
                    tmr['id'],
                    tmr['open'],
                    tmr['close'],
                    tmr['high'],
                    tmr['low'],
                    tmr['vol'],
                ]
            except IndexError:
                tmr_info = [0, 0, 0, 0, 0, 0]

            try:
                vol_last = data[i-1]['vol']
            except IndexError:
                vol_last = 0

            today_boll = get_boll(
                data['close'][i-boll_n+1:i+1],
                m=[2, 1.5, 1, 0.5, -0.5, -1, -1.5, -2]
            )
            tmr_boll = get_boll(
                np.append(data['close'][i-boll_n+2:i+1], data['close'][i]),
                m=[2, 1.5, 1, 0.5, -0.5, -1, -1.5, -2]
            )
            price_list = data['close'][i-boll_n+1:i]
            boll_real = get_real_boll(price_list, 0)
            bollup_real = get_real_boll(price_list, 2)
            bollfake1_real = get_real_boll(price_list, 1.5)
            temp_list.append((
                *today_info, *tmr_info, vol_last,
                date, rate, cont_loss_days, cont_loss_rate,
                is_max_loss, is_min_loss, *today_boll, *tmr_boll,
                boll_real, bollup_real, bollfake1_real, (
                    data[i]['id']-min_ts)//86400
            ))
    cont_loss_list.data = np.array(temp_list, dtype=ContLossList.dtype)

    cont_loss_list.data.sort(order=['symbol', 'id'])
    cont_loss_list.save(cont_loss_list_path)


def get_random_cont_loss_list(random, num, boll_n=BOLL_N, level='1day'):
    return [
        ContLossList.load(
            f'{ROOT}/back_trace/npy/cont_list_{boll_n}_{random}_{i}{"" if level == "1day" else "_"+level}.npy')
        for i in range(num)
    ]


if __name__ == '__main__':
    from user.binance import BinanceUser

    [u] = BinanceUser.init_users()
    Global.user = u

    # for file in sorted(os.listdir(f'{ROOT}/back_trace/npy/detail')):
    #     try:
    #         symbol, t, interval = file[:-4].split('_')
    #     except:
    #         print(file)
    #         continue

    #     get_detailed_klines(symbol, interval, int(t), '')
    get_data(100, 0, True, force_update=True, level='4hour')
