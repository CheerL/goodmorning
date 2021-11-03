import time
import argparse
from back_trace.model import Global, Param
from back_trace.func import back_trace, get_data, ROOT
from user.binance import BinanceUser
from utils import logger
from utils.parallel import run_process_pool, run_thread, run_thread_pool, run_process
from itertools import product
import numpy as np
import optuna
import logging
import sys
# from utils.profile import do_cprofile

def str2list(string, t=float, sep=','):
    return [t(each) for each in string.split(sep)]

def str2range(string, t=float, sep=',', nprange=False):
    if ':' in string:
        l = str2list(string, t, ':')
        
    else:
        l = str2list(string, t, sep)
        
    if len(l) == 1:
        l = l * 2

    if nprange:
        return np.arange(*l)
    else:
        return l

if __name__ == '__main__':
    # @do_cprofile('back_trace/result.prof')
    def sub_back_trace(
        param: Param,
        write=True,
        sub_write=False,
        load=True,
        show=False
        ):
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

        result = []
        Global.add_num()
        if not param.check():
            return 0, 0, 0

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
        return mean_total_money, mean_profit_rate, mean_back_rate

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--days', default=365, type=int)
    parser.add_argument('-b', '--min_before', default=180, type=int)
    parser.add_argument('-l', '--load', action='store_true', default=False)
    parser.add_argument('-s', '--search', action='store_true', default=False)
    parser.add_argument('-e', '--end', default='10', type=str)

    parser.add_argument('--search_num', default=10, type=int)
    parser.add_argument('--new_search', default=False, action='store_true')
    parser.add_argument('--node_trials', default=1000, type=int)

    parser.add_argument('--min_price_list', default='0')
    parser.add_argument('--max_price_list', default='1')
    parser.add_argument('--max_hold_days_list', default='2')
    parser.add_argument('--min_buy_vol_list', default='5000000')
    parser.add_argument('--max_buy_vol_list', default='1e11')
    parser.add_argument('--min_num_list', default='3')
    parser.add_argument('--max_num_list', default='10')
    parser.add_argument('--max_buy_ts_list', default='86300')
    parser.add_argument('--buy_rate_list', default='-0.05:0')
    parser.add_argument('--high_rate_list', default='0.05:0.5')
    parser.add_argument('--high_back_rate_list', default='0.3:0.8')
    parser.add_argument('--low_rate_list', default='0.02:0.08')
    parser.add_argument('--low_back_rate_list', default='0:0.05')
    parser.add_argument('--clear_rate_list', default='-0.01')
    parser.add_argument('--final_rate_list', default='0:0.1')
    parser.add_argument('--stop_loss_rate_list', default='-1')
    parser.add_argument('--min_cont_rate_list', default='-0.25:-0.1')
    parser.add_argument('--break_cont_rate_list', default='-0.35:-0.15')
    parser.add_argument('--up_cont_rate_list', default='-0.25:-0.05')


    parser.add_argument('--min_price', default=0, type=float)
    parser.add_argument('--max_price', default=1, type=float)
    parser.add_argument('--max_hold_days', default=2, type=int)
    parser.add_argument('--min_buy_vol', default=5000000, type=float)
    parser.add_argument('--max_buy_vol', default=1e11, type=float)
    parser.add_argument('--min_num', default=3, type=float)
    parser.add_argument('--max_num', default=10, type=float)
    parser.add_argument('--max_buy_ts', default=86300, type=float)
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
    cont_loss_list, klines_dict = get_data(days=args.days, end=min(end_list), min_before=args.min_before, filter_=False)

    if args.search:
        def objective(trial: optuna.Trial):
            param = Param(
                min_price = trial.suggest_float('min_price', *str2range(args.min_price_list)),
                max_price = trial.suggest_float('max_price', *str2range(args.max_price_list)),
                max_hold_days = trial.suggest_int('max_hold_days', *str2range(args.max_hold_days_list, int)),
                min_buy_vol = trial.suggest_float('min_buy_vol', *str2range(args.min_buy_vol_list)),
                max_buy_vol = trial.suggest_float('max_buy_vol', *str2range(args.max_buy_vol_list)),
                min_num = trial.suggest_int('min_num', *str2range(args.min_num_list, int)),
                max_num = trial.suggest_int('max_num', *str2range(args.max_num_list, int)),
                max_buy_ts = trial.suggest_float('max_buy_ts', *str2range(args.max_buy_ts_list)),
                buy_rate = trial.suggest_float('buy_rate', *str2range(args.buy_rate_list)),
                high_rate = trial.suggest_float('high_rate', *str2range(args.high_rate_list)),
                high_back_rate = trial.suggest_float('high_back_rate', *str2range(args.high_back_rate_list)),
                low_rate = trial.suggest_float('low_rate', *str2range(args.low_rate_list)),
                low_back_rate = trial.suggest_float('low_back_rate', *str2range(args.low_back_rate_list)),
                clear_rate = trial.suggest_float('clear_rate', *str2range(args.clear_rate_list)),
                final_rate = trial.suggest_float('final_rate', *str2range(args.final_rate_list)),
                stop_loss_rate = trial.suggest_float('stop_loss_rate', *str2range(args.stop_loss_rate_list)),
                min_cont_rate = trial.suggest_float('min_cont_rate', *str2range(args.min_cont_rate_list)),
                break_cont_rate = trial.suggest_float('break_cont_rate', *str2range(args.break_cont_rate_list)),
                up_cont_rate = trial.suggest_float('up_cont_rate', *str2range(args.up_cont_rate_list))
            )
            mean_total_money, mean_profit_rate, mean_back_rate = sub_back_trace(param)
            return mean_total_money

        if args.new_search:
            with open(best_params_path, 'w') as f:
                params_title = ','.join(Param.orders)
                f.write(f'{params_title},final_money,profit_rate,max_back_rate\n')
        
        opt_logger = optuna.logging._get_library_root_logger()
        for handler in logger.handlers:
            opt_logger.handlers.append(handler)

        study = optuna.create_study(
            storage='sqlite:///back_trace/param.db',
            sampler=optuna.samplers.TPESampler(),
            direction='maximize',
            pruner=optuna.pruners.HyperbandPruner(),
            study_name='best_param',
            load_if_exists=True
            )

        run_process_pool([(study.optimize, (objective, args.node_trials)) for _ in range(args.search_num)], True, args.search_num)
        print('Number of finished trials:', len(study.trials))
        print("------------------------------------------------")
        print('Best trial:', study.best_trial.params)
        print("------------------------------------------------")
        print(study.trials_dataframe())
        print("------------------------------------------------")

        # params_list = [
        #     str2range(args.min_price_list),
        #     str2range(args.max_price_list),
        #     str2range(args.max_hold_days_list, int),
        #     str2range(args.min_buy_vol_list),
        #     str2range(args.max_buy_vol_list),
        #     str2range(args.min_num_list),
        #     str2range(args.max_num_list),
        #     str2range(args.max_buy_ts_list),
        #     str2range(args.buy_rate_list),
        #     str2range(args.high_rate_list),
        #     str2range(args.high_back_rate_list),
        #     str2range(args.low_rate_list),
        #     str2range(args.low_back_rate_list),
        #     str2range(args.clear_rate_list),
        #     str2range(args.final_rate_list),
        #     str2range(args.stop_loss_rate_list),
        #     str2range(args.min_cont_rate_list),
        #     str2range(args.break_cont_rate_list),
        #     str2range(args.up_cont_rate_list)
        # ]
        # def report():
        #     while Global.num.value < tasks_num:
        #         Global.show(tasks_num)
        #         time.sleep(10)

        # with open(best_params_path, 'w') as f:
        #     params_title = ','.join(Param.orders)
        #     f.write(f'{params_title},final_money,profit_rate,max_back_rate\n')

        # tasks = ((sub_back_trace, [Param(*each)],) for each in product(*params_list))
        # tasks_num = np.prod([len(each) for each in params_list])
        # print(f'Start with {tasks_num} task')

        # run_thread([(report, ())], False)
        # # print(tasks_num, np.prod(tasks_num))
        # run_process_pool(tasks, is_lock=True, limit_num=args.search_num)

        

    else:
        param = Param(**args.__dict__)
        sub_back_trace(
            param,
            write=False,
            sub_write=True,
            show=True,
            load=args.load
        )