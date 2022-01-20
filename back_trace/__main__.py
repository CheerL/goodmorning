import argparse
from back_trace.model import Global, Param
from back_trace.func import back_trace, get_data, ROOT, get_random_cont_loss_list
from user.binance import BinanceUser
from utils import logger
from utils.parallel import run_process_pool
import numpy as np
import optuna
import random
import time

RANDOM_ALL_NUM = 10
BACK_COFF_1 = -0.1
BACK_COFF_2 = -0.15


# np.random.seed(time.time())
# random.seed(time.time())

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
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--days', default='365', type=str)
    parser.add_argument('-b', '--min_before', default=180, type=int)
    parser.add_argument('-l', '--load', action='store_true', default=False)
    parser.add_argument('-s', '--search', action='store_true', default=False)
    parser.add_argument('-e', '--end', default='10', type=str)
    parser.add_argument('--weight', default='1', type=str)

    parser.add_argument('--random', default=0, type=float)
    parser.add_argument('--random_repeat', default=5, type=int)
    parser.add_argument('--search_random', default=0.005, type=float)

    parser.add_argument('--param_csv', default='param_csv', type=str)
    parser.add_argument('--search_num', default=50, type=int)
    parser.add_argument('--search_name', default='best_param')
    parser.add_argument('--new_search', default=False, action='store_true')
    parser.add_argument('--search_show', default=False, action='store_true')
    parser.add_argument('--node_trials', default=1000, type=int)
    parser.add_argument('--search_algo', default='tpe')
    parser.add_argument('--search_storage', default='postgresql://chenran:lcr0717@ai.math.cuhk.edu.hk:54321/params')

    parser.add_argument('--min_price_list', default='0')
    parser.add_argument('--max_price_list', default='2')
    parser.add_argument('--max_hold_days_list', default='2')
    parser.add_argument('--min_buy_vol_list', default='1000000:10000000:1000000')
    # parser.add_argument('--min_buy_vol_list', default='3000000')
    parser.add_argument('--max_buy_vol_list', default='1e11')
    parser.add_argument('--min_num_list', default='3')
    parser.add_argument('--max_num_list', default='10')
    parser.add_argument('--max_buy_ts_list', default='86300')
    parser.add_argument('--buy_rate_list', default='0')
    parser.add_argument('--high_rate_list', default='0.091:0.4')
    parser.add_argument('--high_back_rate_list', default='0.1:0.8')
    parser.add_argument('--high_hold_time_list', default='86400')
    # parser.add_argument('--high_hold_time_list', default='3600:86400:1800')
    parser.add_argument('--low_rate_list', default='0.005:0.09')
    parser.add_argument('--low_back_rate_list', default='0:0.089')
    parser.add_argument('--clear_rate_list', default='-0.03:0.0')
    parser.add_argument('--final_rate_list', default='0:0.10')
    parser.add_argument('--stop_loss_rate_list', default='-1')
    parser.add_argument('--min_cont_rate_list', default='-0.25:-0.05')
    parser.add_argument('--break_cont_rate_list', default='-0.4:-0.15')
    parser.add_argument('--up_cont_rate_list', default='-0.2:-0.05')
    parser.add_argument('--min_close_rate_list', default='0')
    parser.add_argument('--up_near_rate_list', default='0.6:1')
    parser.add_argument('--low_near_rate_list', default='0')
    parser.add_argument('--up_small_cont_rate_list', default='-0.25:-0.08')
    parser.add_argument('--up_small_loss_rate_list', default='-0.05:0')
    parser.add_argument('--up_break_cont_rate_list', default='-0.4:-0.08')

    parser.add_argument('--min_price', default=0, type=float)
    parser.add_argument('--max_price', default=2, type=float)
    parser.add_argument('--max_hold_days', default=2, type=int)
    parser.add_argument('--min_buy_vol', default=4000000, type=float)
    parser.add_argument('--max_buy_vol', default=1e11, type=float)
    parser.add_argument('--min_num', default=3, type=float)
    parser.add_argument('--max_num', default=10, type=float)
    parser.add_argument('--max_buy_ts', default=86300, type=float)
    parser.add_argument('--buy_rate', default=0, type=float)
    parser.add_argument('--high_rate', default=0.29, type=float)
    parser.add_argument('--high_back_rate', default=0.59, type=float)
    parser.add_argument('--high_hold_time', default=14400, type=int)
    parser.add_argument('--low_rate', default=0.079, type=float)
    parser.add_argument('--low_back_rate', default=0.05, type=float)
    parser.add_argument('--clear_rate', default=-0.013, type=float)
    parser.add_argument('--final_rate', default=0.065, type=float)
    parser.add_argument('--stop_loss_rate', default=-1, type=float)
    parser.add_argument('--min_cont_rate', default=-0.136, type=float)
    parser.add_argument('--break_cont_rate', default=-0.18, type=float)
    parser.add_argument('--up_cont_rate', default=-0.1, type=float)
    parser.add_argument('--min_close_rate', default=0, type=float)
    parser.add_argument('--up_near_rate', default=0.91, type=float)
    parser.add_argument('--low_near_rate', default=0.28, type=float)
    parser.add_argument('--up_small_cont_rate', default=-0.15, type=float)
    parser.add_argument('--up_small_loss_rate', default=-0.03, type=float)
    parser.add_argument('--up_break_cont_rate', default=-0.2, type=float)




    args = parser.parse_args()

    # end_list = range(5, 200, 20)
    interval = '1min'
    end_list = str2list(args.end, int)
    days_list = str2list(args.days, int)
    weight_list = str2list(args.weight, float)
    assert len(end_list) == len(days_list) == len(weight_list), 'Not same'

    [u] = BinanceUser.init_users()
    Global.user = u
    best_params_path = f'{ROOT}/back_trace/csv/{args.param_csv}.csv'
    cont_loss_list, klines_dict = get_data(days=max(days_list)+max(end_list)+args.min_before, end=min(end_list), min_before=args.min_before, filter_=False)
    
    if args.search:
        args.random = args.search_random
        
    random_lists = get_random_cont_loss_list(args.random, RANDOM_ALL_NUM) if args.random  else []
    
    def sub_back_trace(
        param: Param,
        write=True,
        sub_write=False,
        load=True,
        show=False
        ):
        def sub_worker(end, days, weight, cont_loss_list):
            total_money, profit_rate, max_back_rate = back_trace(
                cont_loss_list, param,
                min_vol=u.min_usdt_amount,
                fee_rate=u.fee_rate,
                days=days,
                end=end,
                write=sub_write,
                interval=interval,
            )
            if args.search:
                if max_back_rate < BACK_COFF_2:
                    coff = weight * ((max_back_rate - BACK_COFF_2) * 6 + 1 - BACK_COFF_1 + BACK_COFF_2)
                else:
                    coff = weight * (max_back_rate + 1 - BACK_COFF_1)
            else:
                coff = 1
            result.append([end, total_money * coff, profit_rate, max_back_rate])

        result = []
        Global.add_num()
        if not param.check():
            return 0, 0, 0

        for end, days, weight in zip(end_list, days_list, weight_list):
            loss_list, _ = get_data(
                days, end, load,
                min_before=args.min_before,
                klines_dict=klines_dict,
                cont_loss_list=cont_loss_list
            )
            sub_worker(end, days, weight, loss_list)

            if args.random > 0 and args.random_repeat > 0:
                # for i in range(args.random_repeat):
                for i in random.sample(range(RANDOM_ALL_NUM), args.random_repeat):
                # for i in np.random.choice(RANDOM_ALL_NUM, args.random_repeat, False):
                    loss_list, _ = get_data(
                        days, end, load,
                        min_before=args.min_before,
                        klines_dict=klines_dict,
                        cont_loss_list=random_lists[i]
                    )
                    sub_worker(end, days, weight, loss_list)

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

    if args.search:
        def objective(trial: optuna.Trial):
            param = Param(
                min_price = trial.suggest_float('min_price', *str2range(args.min_price_list)),
                max_price = trial.suggest_float('max_price', *str2range(args.max_price_list)),
                max_hold_days = trial.suggest_int('max_hold_days', *str2range(args.max_hold_days_list, int)),
                min_buy_vol = trial.suggest_int('min_buy_vol', *str2range(args.min_buy_vol_list, int)),
                max_buy_vol = trial.suggest_float('max_buy_vol', *str2range(args.max_buy_vol_list)),
                min_num = trial.suggest_int('min_num', *str2range(args.min_num_list, int)),
                max_num = trial.suggest_int('max_num', *str2range(args.max_num_list, int)),
                max_buy_ts = trial.suggest_float('max_buy_ts', *str2range(args.max_buy_ts_list)),
                buy_rate = trial.suggest_float('buy_rate', *str2range(args.buy_rate_list)),
                high_rate = trial.suggest_float('high_rate', *str2range(args.high_rate_list)),
                high_back_rate = trial.suggest_float('high_back_rate', *str2range(args.high_back_rate_list)),
                high_hold_time = trial.suggest_int('high_hold_time', *str2range(args.high_hold_time_list, int)),
                low_rate = trial.suggest_float('low_rate', *str2range(args.low_rate_list)),
                low_back_rate = trial.suggest_float('low_back_rate', *str2range(args.low_back_rate_list)),
                clear_rate = trial.suggest_float('clear_rate', *str2range(args.clear_rate_list)),
                final_rate = trial.suggest_float('final_rate', *str2range(args.final_rate_list)),
                stop_loss_rate = trial.suggest_float('stop_loss_rate', *str2range(args.stop_loss_rate_list)),
                min_cont_rate = trial.suggest_float('min_cont_rate', *str2range(args.min_cont_rate_list)),
                break_cont_rate = trial.suggest_float('break_cont_rate', *str2range(args.break_cont_rate_list)),
                up_cont_rate = trial.suggest_float('up_cont_rate', *str2range(args.up_cont_rate_list)),
                min_close_rate = trial.suggest_float('min_close_rate', *str2range(args.min_close_rate_list)),
                up_near_rate = trial.suggest_float('up_near_rate', *str2range(args.up_near_rate_list)),
                low_near_rate = trial.suggest_float('low_near_rate', *str2range(args.low_near_rate_list)),
                up_small_cont_rate = trial.suggest_float('up_small_cont_rate', *str2range(args.up_small_cont_rate_list)),
                up_small_loss_rate = trial.suggest_float('up_small_loss_rate', *str2range(args.up_small_loss_rate_list)),
                up_break_cont_rate = trial.suggest_float('up_break_cont_rate', *str2range(args.up_break_cont_rate_list)),
            )
            mean_total_money, mean_profit_rate, mean_back_rate = sub_back_trace(param)
            return mean_total_money

        if args.search_algo == 'tpe':
            sampler=optuna.samplers.TPESampler()
            pruner=optuna.pruners.HyperbandPruner()
        elif args.search_algo == 'cma':
            sampler=optuna.samplers.CmaEsSampler(warn_independent_sampling=False)
            pruner=optuna.pruners.MedianPruner()
        else:
            sampler=optuna.samplers.RandomSampler()
            pruner=optuna.pruners.MedianPruner()

        study = optuna.create_study(
            # storage='sqlite:///back_trace/param.db',
            storage=args.search_storage,
            sampler=sampler,
            direction='maximize',
            pruner=pruner,
            study_name=args.search_name,
            load_if_exists=True
            )

        if args.search_show:
            print('Best trial:', study.best_trial.params)
            print("------------------------------------------------")
            print(study.trials_dataframe())
            fig = optuna.visualization.plot_parallel_coordinate(study)
            fig.show()

        else:
            if args.new_search:
                with open(best_params_path, 'w') as f:
                    params_title = ','.join(Param.orders)
                    f.write(f'{params_title},final_money,profit_rate,max_back_rate\n')
            
            opt_logger = optuna.logging._get_library_root_logger()
            for handler in logger.handlers:
                opt_logger.handlers.append(handler)

            

            run_process_pool([(study.optimize, (objective, args.node_trials,)) for _ in range(args.search_num)], True, args.search_num)
            print('Number of finished trials:', len(study.trials))
            print("------------------------------------------------")
            print('Best trial:', study.best_trial.params)
            print("------------------------------------------------")
            print(study.trials_dataframe())

    else:
        param = Param(**args.__dict__)
        sub_back_trace(
            param,
            write=False,
            sub_write=True,
            show=True,
            load=args.load
        )
