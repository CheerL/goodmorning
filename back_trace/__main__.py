import time
from back_trace.model import Global, Param
from back_trace.func import back_trace, get_data, ROOT
from user.binance import BinanceUser
from utils.parallel import run_process_pool, run_thread_pool, run_process
from itertools import product
import numpy as np
# from utils.profile import do_cprofile

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
                days=days, 
                end=end,
                write=sub_write,
                interval=interval,
            )
            result.append([end, total_money, profit_rate, max_back_rate])

        # run_process([[sub_worker, [end,],] for end in range(2,200,20)], is_lock=True, limit_num=2)
        for end in end_list:
            loss_list, _ = get_data(
                days, end, load,
                klines_dict=klines_dict, cont_loss_list=cont_loss_list
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

    days = 365
    load = True
    param_search = True
    detailed_check = True
    interval = '1min'
    # end_list = range(5, 200, 20)
    end_list = [6]

    [u] = BinanceUser.init_users()
    Global.user = u
    best_params_path = f'{ROOT}/back_trace/csv/params_new2.csv'
    cont_loss_list, klines_dict = get_data(days=days, end=1, filter_=False)

    if param_search:
        min_price_list = [10]
        max_price_list = [1000]
        max_hold_days_list = np.arange(2, 14, 2)
        min_buy_vol_list = [5000000]
        max_buy_vol_list = [1e10]
        min_num_list = [3]
        max_num_list = [10]
        high_rate_list = np.arange(0.10, 0.35, 0.05)
        high_back_rate_list = [0.5]
        low_rate_list = np.arange(0.02, 0.09, 0.01)
        low_back_rate_list = np.arange(0.01, 0.07, 0.01)
        clear_rate_list = [-0.01]
        final_rate_list = np.arange(0, 0.1, 0.02)
        stop_loss_rate_list = [-1]
        min_cont_rate_list = np.arange(-0.05, -0.3, -0.05)
        break_cont_rate_list = np.arange(-0.2, -0.4, -0.05)
        up_cont_rate_list = np.arange(-0.05, -0.4, -0.05)
        # min_price_list = [0]
        # max_price_list = [1]
        # max_hold_days_list = [2]
        # min_buy_vol_list = [5000000]
        # max_buy_vol_list = [1e10]
        # min_num_list = [2]
        # max_num_list = [10]
        # high_rate_list = [0.1]
        # high_back_rate_list = np.arange(0, 1, 0.01)
        # low_rate_list = [0.07]
        # low_back_rate_list = [0.02]
        # clear_rate_list = [-0.01]
        # final_rate_list = [0.08]
        # stop_loss_rate_list = [-1]
        # min_cont_rate_list = [-0.15]
        # break_cont_rate_list = [-0.3]
        # up_cont_rate_list = [-0.11]

        params_list = [
            min_price_list,
            max_price_list,
            max_hold_days_list,
            min_buy_vol_list,
            max_buy_vol_list,
            min_num_list,
            max_num_list,
            high_rate_list,
            high_back_rate_list,
            low_rate_list,
            low_back_rate_list,
            clear_rate_list,
            final_rate_list,
            stop_loss_rate_list,
            min_cont_rate_list,
            break_cont_rate_list,
            up_cont_rate_list
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
        param = Param(
            min_price=10,
            max_price=1000,
            max_hold_days=2,
            min_buy_vol=5000000,
            max_buy_vol=1e10,
            min_num=3,
            max_num=30,
            high_rate=0.1,
            high_back_rate=0.6,
            low_rate=0.07,
            low_back_rate=0.02,
            clear_rate=-0.01,
            final_rate=0.08,
            stop_loss_rate=-1,
            min_cont_rate=-0.15,
            break_cont_rate=-0.3,
            up_cont_rate=-0.11,
        )
        sub_back_trace(
            param,
            write=False,
            sub_write=True,
            show=True,
            load=load
        )