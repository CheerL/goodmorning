import time
from back_trace.model import Global
from back_trace.func import back_trace, get_data, ROOT
from user.binance import BinanceUser
from utils.parallel import run_process_pool, run_thread_pool, run_process
from itertools import product
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
        mark_rate,
        back_rate,
        final_rate,
        min_cont_rate,
        break_cont_rate,
        write=True,
        sub_write=False,
        load=True,
        show=False
        ):
        result = []
        min_price, max_price = price_range
        Global.add_num()
        if (
            back_rate >= mark_rate
            # or 0.3 * mark_rate >= back_rate
            or break_cont_rate >= min_cont_rate
            or mark_rate >= high_rate
            or min_price >= max_price
        ):
            return

        def sub_worker(end, cont_loss_list, base_klines_dict):
            total_money, profit_rate, max_back_rate = back_trace(
                cont_loss_list, base_klines_dict, u.min_usdt_amount, u.fee_rate, days, 
                end=end,
                max_hold_days=max_hold_days,
                min_num=min_num,
                max_num=max_num,
                high_rate=high_rate,
                mark_rate=mark_rate,
                back_rate=back_rate,
                final_rate=final_rate,
                min_cont_rate=min_cont_rate,
                break_cont_rate=break_cont_rate,
                min_buy_vol=min_buy_vol,
                max_buy_vol=max_buy_vol,
                min_price=min_price,
                max_price=max_price,
                min_cont_days=1,
                write=sub_write,
                detailed_check=detailed_check,
                detailed_interval=interval
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
                    mark_rate,
                    back_rate,
                    final_rate,
                    min_cont_rate,
                    break_cont_rate,
                    mean_total_money,
                    mean_profit_rate,
                    mean_back_rate
                ]])+'\n')

    days = 365
    load = True
    param_search = False
    detailed_check = True
    interval = '1min'
    # end_list = range(5, 200, 20)
    end_list = [5]

    [u] = BinanceUser.init_users()
    Global.user = u
    best_params_path = f'{ROOT}/back_trace/csv/params_new.csv'
    cont_loss_list, klines_dict = get_data(days=days, end=1, filter_=False)

    if param_search:
        price_range_list = [(0, 1)]
        max_hold_days_list = [2, 3, 4]
        min_buy_vol_list = [3000000, 4000000, 5000000, 6000000, 7000000, 8000000]
        max_buy_vol_list = [10000000000]
        min_num_list = [2, 3, 4, 5]
        max_num_list = [10, 20, 30]
        high_rate_list = [0.1, 0.15, 0.2, 0.25, 0.3]
        mark_rate_list = [0.04, 0.06, 0.08]
        back_rate_list = [0.01, 0.03, 0.05, 0.07]
        final_rate_list = [0.02, 0.04, 0.06, 0.08, 0.1]
        min_cont_rate_list = [-0.1, -0.15, -0.2, 0.25]
        break_cont_rate_list = [-0.2, -0.25, -0.3, -0.35, -0.4]
        # price_range_list = [(0, 1)]
        # max_hold_days_list = [2]
        # min_buy_vol_list = [3000000, 4000000, 5000000, 6000000, 7000000, 8000000]
        # max_buy_vol_list = [10000000000]
        # min_num_list = [2]
        # max_num_list = [10]
        # high_rate_list = [0.1]
        # mark_rate_list = [0.04]
        # back_rate_list = [0.01]
        # final_rate_list = [0.02]
        # min_cont_rate_list = [-0.1]
        # break_cont_rate_list = [-0.2]
        with open(best_params_path, 'w') as f:
            f.write('最低买入价,最高买入价,最低买入交易量,最高买入交易量,最大持有天数,最少仓数,最多仓数,高标记,低标记,回撤单,回本单,连续跌幅,突破连续跌幅,最终资产,收益率,最大回撤\n')

        tasks = [(sub_back_trace, each,) for each in product(
            price_range_list,
            max_hold_days_list,
            min_buy_vol_list,
            max_buy_vol_list,
            min_num_list,
            max_num_list,
            high_rate_list,
            mark_rate_list,
            back_rate_list,
            final_rate_list,
            min_cont_rate_list,
            break_cont_rate_list
        )]
        tasks_num = len(tasks)
        run_process_pool(tasks, is_lock=False, limit_num=20)

        while True:
            Global.show(tasks_num)
            time.sleep(10)

    else:
        sub_back_trace(
            price_range=(0, 1),
            max_hold_days=2,
            min_buy_vol=5000000,
            max_buy_vol=1000000000,
            min_num=2,
            max_num=10,
            high_rate=0.2,
            mark_rate=0.06,
            back_rate=0.05,
            final_rate=0.08,
            min_cont_rate=-0.15,
            break_cont_rate=-0.3,
            write=False,
            sub_write=True,
            show=True,
            load=load
        )