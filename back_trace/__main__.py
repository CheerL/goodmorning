import time
from back_trace.model import Global
from back_trace.func import back_trace, get_data, ROOT
from user.binance import BinanceUser
from utils.parallel import run_process_pool, run_thread_pool, run_process
from itertools import product
import numpy as np
# from utils.profile import do_cprofile

if __name__ == '__main__':
    # @do_cprofile('back_trace/result.prof')
    def sub_back_trace(
        price_range,
        max_hold_days,
        min_buy_vol,
        max_buy_vol,
        min_num,
        max_num,
        high_rate,
        low_rate,
        low_back_rate,
        clear_rate,
        final_rate,
        stop_loss_rate,
        min_cont_rate,
        break_cont_rate,
        up_cont_rate,
        write=True,
        sub_write=False,
        load=True,
        show=False
        ):
        result = []
        min_price, max_price = price_range
        Global.add_num()
        if (
            low_back_rate >= low_rate
            or clear_rate >= low_rate
            or stop_loss_rate >= clear_rate
            or break_cont_rate >= min_cont_rate
            or low_rate >= high_rate
            or min_price >= max_price
            or min_buy_vol >= max_buy_vol
            or min_num >= max_num
        ):
            return

        def sub_worker(end, cont_loss_list, base_klines_dict):
            total_money, profit_rate, max_back_rate = back_trace(
                cont_loss_list, base_klines_dict,u.min_usdt_amount, u.fee_rate, days, 
                end=end,
                max_hold_days=max_hold_days,
                min_num=min_num,
                max_num=max_num,
                high_rate=high_rate,
                low_rate=low_rate,
                low_back_rate=low_back_rate,
                clear_rate=clear_rate,
                final_rate=final_rate,
                stop_loss_rate=stop_loss_rate,
                min_cont_rate=min_cont_rate,
                break_cont_rate=break_cont_rate,
                up_cont_rate=up_cont_rate,
                min_buy_vol=min_buy_vol,
                max_buy_vol=max_buy_vol,
                min_price=min_price,
                max_price=max_price,
                min_cont_days=1,
                write=sub_write,
                detailed_interval=interval,
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
                f.write(','.join([str(e) for e in [
                    min_price,
                    max_price,
                    min_buy_vol,
                    max_buy_vol,
                    max_hold_days,
                    min_num,
                    max_num,
                    high_rate,
                    low_rate,
                    low_back_rate,
                    clear_rate,
                    final_rate,
                    stop_loss_rate,
                    min_cont_rate,
                    break_cont_rate,
                    up_cont_rate,
                    mean_total_money,
                    mean_profit_rate,
                    mean_back_rate
                ]])+'\n')

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
        # price_range_list = [(0, 1)]
        # max_hold_days_list = [2, 3, 4]
        # min_buy_vol_list = [5000000]
        # max_buy_vol_list = [1e10]
        # min_num_list = [2, 3, 4]
        # max_num_list = [10, 20, 30]
        # high_rate_list = np.arange(0.15, 0.35, 0.025)
        # low_rate_list = np.arange(0.03, 0.09, 0.01)
        # low_back_rate_list = np.arange(0.005, 0.05, 0.005)
        # clear_rate_list = [-0.01]
        # final_rate_list = np.arange(0.02, 0.1, 0.02)
        # stop_loss_rate_list = [-1]
        # min_cont_rate_list = np.arange(-0.1, -0.3, -0.05)
        # break_cont_rate_list = np.arange(-0.15, -0.4, -0.05)
        # up_cont_rate_list = np.arange(-0.1, -0.4, -0.05)
        price_range_list = [(0, 1)]
        max_hold_days_list = [2]
        min_buy_vol_list = [5000000]
        max_buy_vol_list = [1e10]
        min_num_list = [2]
        max_num_list = [10]
        high_rate_list = [0.25]
        low_rate_list = [0.07]
        low_back_rate_list = [0.02]
        clear_rate_list = [-0.01]
        final_rate_list = [0.08]
        stop_loss_rate_list = [-1]
        min_cont_rate_list = [-0.15]
        break_cont_rate_list = [-0.3]
        up_cont_rate_list = np.arange(-0.05, -0.4, -0.01)
        with open(best_params_path, 'w') as f:
            f.write('最低买入价,最高买入价,最低买入交易量,最高买入交易量,最大持有天数,最少仓数,最多仓数,高标记,低标记,回撤单,清仓单,回本单,止损单,连续跌幅,突破连续跌幅,首日跌幅,最终资产,收益率,最大回撤\n')

        tasks = ((sub_back_trace, each,) for each in product(
            price_range_list,
            max_hold_days_list,
            min_buy_vol_list,
            max_buy_vol_list,
            min_num_list,
            max_num_list,
            high_rate_list,
            low_rate_list,
            low_back_rate_list,
            clear_rate_list,
            final_rate_list,
            stop_loss_rate_list,
            min_cont_rate_list,
            break_cont_rate_list,
            up_cont_rate_list,
        ))
        tasks_num = np.prod([len(each) for each in [
            price_range_list,
            max_hold_days_list,
            min_buy_vol_list,
            max_buy_vol_list,
            min_num_list,
            max_num_list,
            high_rate_list,
            low_rate_list,
            low_back_rate_list,
            clear_rate_list,
            final_rate_list,
            stop_loss_rate_list,
            min_cont_rate_list,
            break_cont_rate_list,
            up_cont_rate_list
        ]])
        # print(tasks_num, np.prod(tasks_num))
        run_process_pool(tasks, is_lock=False, limit_num=4)

        while Global.num.value < tasks_num:
            Global.show(tasks_num)
            time.sleep(10)

    else:
        sub_back_trace(
            price_range=(0, 1),
            max_hold_days=2,
            min_buy_vol=5000000,
            max_buy_vol=10000000000,
            min_num=2,
            max_num=10,
            high_rate=0.25,
            low_rate=0.07,
            low_back_rate=0.01,
            clear_rate=-0.01,
            final_rate=0.1,
            stop_loss_rate=-1,
            min_cont_rate=-0.15,
            break_cont_rate=-0.3,
            up_cont_rate=-0.2,
            write=False,
            sub_write=True,
            show=True,
            load=load
        )