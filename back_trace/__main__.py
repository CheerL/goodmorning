import argparse
from back_trace.model import ContLossList, Global, Param
from back_trace.func import back_trace, get_data, ROOT, get_random_cont_loss_list, create_random_cont_loss_list
from user.binance import BinanceUser
from utils import logger, datetime
from utils import parallel
from utils.parallel import run_process_pool
import numpy as np
import optuna
import random

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
        if len(l) == 2:
            d = {'low': l[0], 'high': l[1]}
            # print(d)
            return d
        elif len(l) == 3:
            d = {'low': l[0], 'high': l[1], 'step': l[2]}
            # print(d)
            return d

if __name__ == '__main__':
    # @do_cprofile('back_trace/result.prof')
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--days', default='365', type=str)
    parser.add_argument('-b', '--min_before', default=180, type=int)
    parser.add_argument('-l', '--load', action='store_true', default=False)
    parser.add_argument('-s', '--search', action='store_true', default=False)
    parser.add_argument('-e', '--end', default='10', type=str)
    parser.add_argument('--weight', default='1', type=str)

    parser.add_argument('--level', default='4hour', type=str)
    parser.add_argument('--buy_algo_version', default=2, type=int)
    parser.add_argument('--sell_algo_version', default=2, type=int)

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
    parser.add_argument('--search_storage', default='postgresql://linchenran:lcr0717@cai.math.cuhk.edu.hk:54321/params')

    parser.add_argument('--min_price_list', default='0')
    parser.add_argument('--max_price_list', default='1')
    parser.add_argument('--max_hold_days_list', default='2:24:1')
    parser.add_argument('--min_buy_vol_list', default='500000:5000000:500000')
    # parser.add_argument('--min_buy_vol_list', default='3000000')
    parser.add_argument('--max_buy_vol_list', default='1e12')
    parser.add_argument('--min_up_small_buy_vol_list', default='500000:5000000:500000')
    parser.add_argument('--min_num_list', default='3')
    parser.add_argument('--max_num_list', default='10')
    parser.add_argument('--max_buy_ts_list', default='0')
    parser.add_argument('--buy_rate_list', default='0')
    parser.add_argument('--high_hold_time_list', default='86400')
    parser.add_argument('--high_rate_list', default='0.05:0.2:0.001')
    parser.add_argument('--high_back_rate_list', default='0.05:0.95:0.05')
    parser.add_argument('--low_rate_list', default='0.05:0.95:0.05')
    parser.add_argument('--low_back_rate_list', default='0.05:0.95:0.05')
    parser.add_argument('--clear_rate_list', default='-0.03:0.03:0.001')
    parser.add_argument('--final_rate_list', default='0:0.4:0.01')
    parser.add_argument('--stop_loss_rate_list', default='-1')
    parser.add_argument('--break_cont_rate_list', default='-0.6:-0.01:0.001')
    parser.add_argument('--min_cont_rate_list', default='0.05:0.95:0.02')
    
    parser.add_argument('--min_close_rate_list', default='0')
    parser.add_argument('--up_near_rate_list', default='0.5:1:0.05')
    parser.add_argument('--up_near_rate_fake_list', default='0.5:1:0.05')
    parser.add_argument('--low_near_rate_list', default='0')

    parser.add_argument('--up_break_cont_rate_list', default='-0.3:-0.001:0.001')
    parser.add_argument('--up_cont_rate_list', default='0.05:0.95:0.025')
    parser.add_argument('--up_small_cont_rate_list', default='0.05:0.95:0.025')
    parser.add_argument('--up_small_loss_rate_list', default='0.01:0.32:0.01')
    # parser.add_argument('--buy_up_rate_list', default='0:0.02:0.001')
    # parser.add_argument('--sell_down_rate_list', default='-0.02:0:0.001')
    parser.add_argument('--buy_up_rate_list', default='0')
    parser.add_argument('--sell_down_rate_list', default='0')
    parser.add_argument('--final_modify_rate_list', default='0:1:0.05')
    
    parser.add_argument('--min_num', default=3, type=int)
    parser.add_argument('--max_num', default=10, type=int)
    parser.add_argument('--max_buy_ts', default=0, type=float)
    parser.add_argument('--buy_rate', default=-0.01, type=float)
    parser.add_argument('--high_hold_time', default=86400, type=int)
    parser.add_argument('--low_near_rate', default=0, type=float)
    parser.add_argument('--stop_loss_rate', default=-1, type=float)
    parser.add_argument('--min_close_rate', default=0, type=float)

    ############################################# NOW

    parser.add_argument('--min_price', default=0, type=float)
    parser.add_argument('--max_price', default=1, type=float)
    parser.add_argument('--max_hold_days', default=5, type=int)
    parser.add_argument('--min_buy_vol', default=4500000, type=float)
    parser.add_argument('--max_buy_vol', default=1e11, type=float)
    parser.add_argument('--min_up_small_buy_vol', default=500000, type=float)
    
    parser.add_argument('--break_cont_rate', default=-0.393, type=float)
    parser.add_argument('--buy_up_rate', default=0, type=float)
    parser.add_argument('--clear_rate', default=0.028, type=float)
    parser.add_argument('--final_modify_rate', default=0.95, type=float)
    parser.add_argument('--final_rate', default=0.11, type=float)
    parser.add_argument('--high_back_rate', default=0.9, type=float)
    parser.add_argument('--high_rate', default=0.193, type=float)
    parser.add_argument('--low_back_rate', default=0.25, type=float)
    parser.add_argument('--low_rate', default=0.95, type=float)

    parser.add_argument('--min_cont_rate', default=0.53, type=float)
    parser.add_argument('--sell_down_rate', default=0, type=float)
    parser.add_argument('--up_break_cont_rate', default=-0.086, type=float)
    parser.add_argument('--up_cont_rate', default=0.875, type=float)
    parser.add_argument('--up_near_rate', default=0.7, type=float)
    parser.add_argument('--up_near_rate_fake', default=0.9, type=float)
    parser.add_argument('--up_small_cont_rate', default=0.90, type=float)
    parser.add_argument('--up_small_loss_rate', default=0.02, type=float)
    
    ###################################### NEW
    
    # parser.add_argument('--min_price', default=0, type=float)
    # parser.add_argument('--max_price', default=1, type=float)
    # parser.add_argument('--max_hold_days', default=10, type=int)
    # parser.add_argument('--min_buy_vol', default=2000000, type=float)
    # parser.add_argument('--max_buy_vol', default=1e11, type=float)
    # parser.add_argument('--min_up_small_buy_vol', default=4000000, type=float)
    
    
    # parser.add_argument('--break_cont_rate', default=-0.34, type=float)
    # parser.add_argument('--buy_up_rate', default=0, type=float)
    # parser.add_argument('--clear_rate', default=0.024, type=float)
    # parser.add_argument('--final_modify_rate', default=0.1, type=float)
    # parser.add_argument('--final_rate', default=0.3, type=float)
    # parser.add_argument('--high_back_rate', default=0.1, type=float)
    # parser.add_argument('--high_rate', default=0.16, type=float)
    # parser.add_argument('--low_back_rate', default=0.066, type=float)
    # parser.add_argument('--low_rate', default=0.07, type=float)
    
    # parser.add_argument('--min_cont_rate', default=-0.244, type=float)
    # parser.add_argument('--sell_down_rate', default=-0, type=float)
    # parser.add_argument('--up_break_cont_rate', default=-0.29, type=float)
    # parser.add_argument('--up_cont_rate', default=-0.178, type=float)
    # parser.add_argument('--up_near_rate', default=0.75, type=float)
    # parser.add_argument('--up_near_rate_fake', default=1, type=float)
    # parser.add_argument('--up_small_cont_rate', default=-0.118, type=float)
    # parser.add_argument('--up_small_loss_rate', default=-0.033, type=float)


    parser.add_argument('--generate', action='store_true', default=False)

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
    cont_loss_list, klines_dict = get_data(days=max(days_list)+max(end_list)+args.min_before, end=min(end_list), min_before=args.min_before, filter_=False, level=args.level)
    
    if args.search:
        args.random = args.search_random
        
    if args.generate:
        boll_n = 20
        # cont_loss_list = ContLossList.load(f'{ROOT}/back_trace/npy/cont_list_{boll_n}{"" if args.level == "1day" else "_"+args.level}.npy')
        def sub_generater(i):
            print(i)
            create_random_cont_loss_list(cont_loss_list, args.random, i, boll_n, args.level)
        
        parallel.run_process_pool([(sub_generater, (i,)) for i in range(10)], True, 10)

    if args.random:
        random_lists = get_random_cont_loss_list(args.random, RANDOM_ALL_NUM, level=args.level) if args.random  else []
    
    def sub_back_trace(
        param: Param,
        write=True,
        sub_write=False,
        load=True,
        show=False,
        level='1day'
        ):
        def sub_worker(end, days, weight, cont_loss_list, level):
            total_money, profit_rate, max_back_rate = back_trace(
                cont_loss_list, param,
                min_vol=u.min_usdt_amount,
                fee_rate=u.fee_rate,
                days=days,
                end=end,
                write=sub_write,
                interval=interval,
                level=level
            )
            if max_back_rate < BACK_COFF_2:
                coff = weight * ((max_back_rate - BACK_COFF_2) * 6 + 1 - BACK_COFF_1 + BACK_COFF_2)
            else:
                coff = weight * (max_back_rate + 1 - BACK_COFF_1)

            result.append([end, total_money, total_money * coff, profit_rate, max_back_rate])

        result = []
        Global.add_num()
        if not param.check():
            return 0, 0, 0

        for end, days, weight in zip(end_list, days_list, weight_list):
            loss_list, _ = get_data(
                days, end, load,
                min_before=args.min_before,
                klines_dict=klines_dict,
                cont_loss_list=cont_loss_list,
                level=level
            )
            sub_worker(end, days, weight, loss_list, level)

            if args.random > 0 and args.random_repeat > 0:
                # for i in range(args.random_repeat):
                for i in random.sample(range(RANDOM_ALL_NUM), args.random_repeat):
                # for i in np.random.choice(RANDOM_ALL_NUM, args.random_repeat, False):
                    loss_list, _ = get_data(
                        days, end, load,
                        min_before=args.min_before,
                        klines_dict=klines_dict,
                        cont_loss_list=random_lists[i],
                        level=level
                    )
                    sub_worker(end, days, weight, loss_list, level)

        times = len(result)
        mean_total_money = sum([end_result[1] for end_result in result]) / times
        mean_weighted_money = sum([end_result[2] for end_result in result]) / times
        mean_profit_rate = sum([end_result[3] for end_result in result]) / times
        mean_back_rate = sum([end_result[4] for end_result in result]) / times

        if show:
            print(f'mean result of {times} tries: total_money {mean_total_money}, weighted money {mean_weighted_money}, rate {mean_profit_rate} back {mean_back_rate}')

        if write:
            with open(best_params_path, 'a+') as f:
                f.write(f'{param.to_csv()},{mean_total_money},{mean_profit_rate},{mean_back_rate}\n')
        return mean_weighted_money if args.search else mean_total_money, mean_profit_rate, mean_back_rate

    datetime.Tz.tz_num = 8
    if args.search:
        def objective(trial: optuna.Trial):
            # high_rate = trial.suggest_float('high_rate', **str2range(args.high_rate_list))
            # high_back_rate = high_rate * trial.suggest_float('high_back_rate', **str2range(args.high_back_rate_list))
            # low_rate = high_back_rate * trial.suggest_float('low_rate', **str2range(args.low_rate_list))
            # low_back_rate = low_rate * trial.suggest_float('low_back_rate', **str2range(args.low_back_rate_list))
            
            # break_cont_rate = trial.suggest_float('break_cont_rate', **str2range(args.break_cont_rate_list))
            # min_cont_rate = break_cont_rate * trial.suggest_float('min_cont_rate', **str2range(args.min_cont_rate_list))
            
            # up_break_cont_rate = trial.suggest_float('up_break_cont_rate', **str2range(args.up_break_cont_rate_list))
            # up_cont_rate = up_break_cont_rate * trial.suggest_float('up_cont_rate', **str2range(args.up_cont_rate_list))
            # up_small_cont_rate = up_break_cont_rate * trial.suggest_float('up_small_cont_rate', **str2range(args.up_small_cont_rate_list))
            # up_small_loss_rate = up_small_cont_rate * trial.suggest_float('up_small_loss_rate', **str2range(args.up_small_loss_rate_list))

            param = Param(
                buy_algo_version = args.buy_algo_version,
                sell_algo_version = args.sell_algo_version,
                # min_price = trial.suggest_float('min_price', **str2range(args.min_price_list)),
                max_price = trial.suggest_int('max_price', **str2range(args.max_price_list, int)),
                max_hold_days = trial.suggest_int('max_hold_days', **str2range(args.max_hold_days_list, int)),
                min_buy_vol = trial.suggest_int('min_buy_vol', **str2range(args.min_buy_vol_list, int)),
                min_up_small_buy_vol = trial.suggest_int('min_up_small_buy_vol', **str2range(args.min_up_small_buy_vol_list, int)),
                # max_buy_vol = trial.suggest_float('max_buy_vol', **str2range(args.max_buy_vol_list)),
                # min_num = trial.suggest_int('min_num', **str2range(args.min_num_list, int)),
                # max_num = trial.suggest_int('max_num', **str2range(args.max_num_list, int)),
                # max_buy_ts = trial.suggest_float('max_buy_ts', **str2range(args.max_buy_ts_list)),
                # buy_rate = trial.suggest_float('buy_rate', **str2range(args.buy_rate_list)),
                # min_close_rate = trial.suggest_float('min_close_rate', **str2range(args.min_close_rate_list)),
                clear_rate = trial.suggest_float('clear_rate', **str2range(args.clear_rate_list)),
                final_rate = trial.suggest_float('final_rate', **str2range(args.final_rate_list)),
                stop_loss_rate = trial.suggest_float('stop_loss_rate', **str2range(args.stop_loss_rate_list)),# high_hold_time = trial.suggest_int('high_hold_time', **str2range(args.high_hold_time_list, int)),
                up_near_rate = trial.suggest_float('up_near_rate', **str2range(args.up_near_rate_list)),
                up_near_rate_fake = trial.suggest_float('up_near_rate_fake', **str2range(args.up_near_rate_fake_list)),
                # low_near_rate = trial.suggest_float('low_near_rate', **str2range(args.low_near_rate_list)),
                buy_up_rate = trial.suggest_float('buy_up_rate', **str2range(args.buy_up_rate_list)),
                sell_down_rate = trial.suggest_float('sell_down_rate', **str2range(args.sell_down_rate_list)),
                final_modify_rate = trial.suggest_float('final_modify_rate', **str2range(args.final_modify_rate_list)),
                ################################################################
                high_rate = trial.suggest_float('high_rate', **str2range(args.high_rate_list)),
                high_back_rate = trial.suggest_float('high_back_rate', **str2range(args.high_back_rate_list)),
                low_rate = trial.suggest_float('low_rate', **str2range(args.low_rate_list)),
                low_back_rate = trial.suggest_float('low_back_rate', **str2range(args.low_back_rate_list)),
                break_cont_rate = trial.suggest_float('break_cont_rate', **str2range(args.break_cont_rate_list)),
                min_cont_rate = trial.suggest_float('min_cont_rate', **str2range(args.min_cont_rate_list)),
                up_break_cont_rate = trial.suggest_float('up_break_cont_rate', **str2range(args.up_break_cont_rate_list)),
                up_cont_rate = trial.suggest_float('up_cont_rate', **str2range(args.up_cont_rate_list)),
                up_small_cont_rate = trial.suggest_float('up_small_cont_rate', **str2range(args.up_small_cont_rate_list)),
                up_small_loss_rate = trial.suggest_float('up_small_loss_rate', **str2range(args.up_small_loss_rate_list)),
                
                ################################################################
                # high_rate = high_rate,
                # high_back_rate = high_back_rate,
                # low_rate = low_rate,
                # low_back_rate = low_back_rate,
                # break_cont_rate = break_cont_rate,
                # min_cont_rate = min_cont_rate,
                # up_break_cont_rate = up_break_cont_rate,
                # up_cont_rate = up_cont_rate,
                # up_small_cont_rate = up_small_cont_rate,
                # up_small_loss_rate = up_small_loss_rate
                ################################################################
            )
            # print(param.show())
            param.high_back_rate = param.high_rate * param.high_back_rate
            param.low_rate = param.high_back_rate * param.low_rate
            param.low_back_rate = param.low_rate * param.low_back_rate
            param.min_cont_rate = param.break_cont_rate * param.min_cont_rate
            param.up_cont_rate = param.up_break_cont_rate * param.up_cont_rate
            param.up_small_cont_rate = param.up_break_cont_rate * param.up_small_cont_rate
            param.up_small_loss_rate = param.up_small_cont_rate * param.up_small_loss_rate
            mean_total_money, mean_profit_rate, mean_back_rate = sub_back_trace(param, level=args.level)
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
        param.high_back_rate = param.high_rate * param.high_back_rate
        param.low_rate = param.high_back_rate * param.low_rate
        param.low_back_rate = param.low_rate * param.low_back_rate
        param.min_cont_rate = param.break_cont_rate * param.min_cont_rate
        param.up_cont_rate = param.up_break_cont_rate * param.up_cont_rate
        param.up_small_cont_rate = param.up_break_cont_rate * param.up_small_cont_rate
        param.up_small_loss_rate = param.up_small_cont_rate * param.up_small_loss_rate

        print(param.show())
        sub_back_trace(
            param,
            write=False,
            sub_write=True,
            show=True,
            load=args.load,
            level=args.level
        )
