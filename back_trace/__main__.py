import time
import argparse
from back_trace.model import Global, Param
from back_trace.func import back_trace, get_data, ROOT
from user.binance import BinanceUser
from utils.parallel import run_process_pool, run_thread_pool, run_process
from itertools import product
import numpy as np
# from utils.profile import do_cprofile

def str2list(string, t=float, sep=','):
    return [t(each) for each in string.split(sep)]

def str2range(string, t=float, sep=','):
    if ':' in string:
        l = str2list(string, t, ':')
        return np.arange(*l)
    else:
        return str2list(string, t, sep)

if __name__ == '__main__':
    # @do_cprofile('back_trace/result.prof')
    def sub_back_trace(
        param: Param,
        write=True,
        sub_write=False,
        load=True,
        show=False
        ):
        result = []
        Global.add_num()
        if not param.check():
            return
        def sub_worker(end, cont_loss_list, base_klines_dict):
            total_money, profit_rate, max_back_rate = back_trace(
                cont_loss_list, base_klines_dict, param,
                min_vol=u.min_usdt_amount,
                fee_rate=u.fee_rate, 
                days=args.days, 
                end=end,
                write=sub_write,
                interval=interval,
            )
            result.append([end, total_money, profit_rate, max_back_rate])

        # run_process([[sub_worker, [end,],] for end in range(2,200,20)], is_lock=True, limit_num=2)
        for end in end_list:
            loss_list, _ = get_data(
                args.days, end, load,
                min_before=args.min_before,
                klines_dict=klines_dict,
            )
            sub_worker(end, loss_list, klines_dict)

        times = len(result)
        mean_total_money = sum([end_result[1] for end_result in result]) / times
        mean_profit_rate = sum([end_result[2] for end_result in result]) / times
        mean_back_rate = sum([end_result[3] for end_result in result]) / times

        if show:
            print(f'mean result of {times} tries: total_money {mean_total_money}, rate {mean_profit_rate} back {mean_back_rate}')

        if write:
            with open(best_params_path, 'a+') as f:
                f.write(f'{param.to_csv()},{mean_total_money},{mean_profit_rate},{mean_back_rate}\n')

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--days', default=365, type=int)
    parser.add_argument('-b', '--min_before', default=180, type=int)
    parser.add_argument('-l', '--load', action='store_true', default=False)
    parser.add_argument('-s', '--search', action='store_true', default=False)
    parser.add_argument('-e', '--end', default='1', type=str)

    parser.add_argument('--min_price_list', default='0')
    parser.add_argument('--max_price_list', default='1')
    parser.add_argument('--max_hold_days_list', default='2')
    parser.add_argument('--min_buy_vol_list', default='5000000')
    parser.add_argument('--max_buy_vol_list', default='1e11')
    parser.add_argument('--min_num_list', default='3')
    parser.add_argument('--max_num_list', default='10')
    parser.add_argument('--max_buy_ts_list', default='3600')
    parser.add_argument('--buy_rate_list', default='-0.01')
    parser.add_argument('--high_rate_list', default='0.10:0.35:0.05')
    parser.add_argument('--high_back_rate_list', default='0.5')
    parser.add_argument('--low_rate_list', default='0.02:0.09:0.01')
    parser.add_argument('--low_back_rate_list', default='0.01:0.07:0.01')
    parser.add_argument('--clear_rate_list', default='-0.01')
    parser.add_argument('--final_rate_list', default='0:0.1:0.02')
    parser.add_argument('--stop_loss_rate_list', default='-1')
    parser.add_argument('--min_cont_rate_list', default='-0.05:-0.3:-0.05')
    parser.add_argument('--break_cont_rate_list', default='-0.2:-0.4:-0.05')
    parser.add_argument('--up_cont_rate_list', default='-0.05:-0.4:-0.05')


    parser.add_argument('--min_price', default=0, type=float)
    parser.add_argument('--max_price', default=1, type=float)
    parser.add_argument('--max_hold_days', default=2, type=float)
    parser.add_argument('--min_buy_vol', default=5000000, type=float)
    parser.add_argument('--max_buy_vol', default=1e11, type=float)
    parser.add_argument('--min_num', default=3, type=float)
    parser.add_argument('--max_num', default=10, type=float)
    parser.add_argument('--max_buy_ts', default=60*60*12, type=float)
    parser.add_argument('--buy_rate', default=-0.01, type=float)
    parser.add_argument('--high_rate', default=0.25, type=float)
    parser.add_argument('--high_back_rate', default=0.6, type=float)
    parser.add_argument('--low_rate', default=0.06, type=float)
    parser.add_argument('--low_back_rate', default=0.02, type=float)
    parser.add_argument('--clear_rate', default=-0.01, type=float)
    parser.add_argument('--final_rate', default=0.08, type=float)
    parser.add_argument('--stop_loss_rate', default=-1, type=float)
    parser.add_argument('--min_cont_rate', default=-0.15, type=float)
    parser.add_argument('--break_cont_rate', default=-0.3, type=float)
    parser.add_argument('--up_cont_rate', default=-0.1, type=float)

    args = parser.parse_args()

    # end_list = range(5, 200, 20)
    interval = '1min'
    end_list = str2list(args.end, int)

    [u] = BinanceUser.init_users()
    Global.user = u
    best_params_path = f'{ROOT}/back_trace/csv/params_new2.csv'
    cont_loss_list, klines_dict = get_data(days=args.days, end=1, min_before=args.min_before, filter_=False)

    if args.search:
        

        params_list = [
            str2range(args.min_price_list),
            str2range(args.max_price_list),
            str2range(args.max_hold_days_list),
            str2range(args.min_buy_vol_list),
            str2range(args.max_buy_vol_list),
            str2range(args.min_num_list),
            str2range(args.max_num_list),
            str2range(args.max_buy_ts_list),
            str2range(args.buy_rate_list),
            str2range(args.high_rate_list),
            str2range(args.high_back_rate_list),
            str2range(args.low_rate_list),
            str2range(args.low_back_rate_list),
            str2range(args.clear_rate_list),
            str2range(args.final_rate_list),
            str2range(args.stop_loss_rate_list),
            str2range(args.min_cont_rate_list),
            str2range(args.break_cont_rate_list),
            str2range(args.up_cont_rate_list)
        ]

        with open(best_params_path, 'w') as f:
            params_title = ','.join(Param.orders)
            f.write(f'{params_title},final_money,profit_rate,max_back_rate\n')

        tasks = ((sub_back_trace, [Param(*each)],) for each in product(*params_list))
        tasks_num = np.prod([len(each) for each in params_list])
        # print(tasks_num, np.prod(tasks_num))
        run_process_pool(tasks, is_lock=False, limit_num=40)

        while Global.num.value < tasks_num:
            Global.show(tasks_num)
            time.sleep(10)

    else:
        param = Param(**args.__dict__)
        sub_back_trace(
            param,
            write=False,
            sub_write=True,
            show=True,
            load=args.load
        )