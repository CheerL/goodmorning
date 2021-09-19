
import argparse

from dealer import trade_update_callback, error_callback, init_users, init_dealer
from utils import config, kill_all_threads, logger, user_config, get_rate
from utils.parallel import run_thread_pool, run_process, run_thread
from market import MarketClient
from user import User

MIN_LOSS_RATE = config.getfloat('loss', 'MIN_LOSS_RATE')
BREAK_LOSS_RATE = config.getfloat('loss', 'BREAK_LOSS_RATE')

def check_buy(m: MarketClient, symbol, min_loss_rate, break_loss_rate, end=0, min_before=180):
    try:
        klines = m.get_candlestick(symbol, '1day', min_before+end+1)[end:]
    except Exception:
        print(symbol)
        return 0

    if len(klines) <= min_before:
        return 0

    rate = get_rate(klines[0].close, klines[0].open)
    if rate >= 0:
        return 0

    cont_loss_list = [rate]

    for kline in klines[1:]:
        if kline.close < kline.open:
            cont_loss_list.append(get_rate(kline.close, kline.open))
        else:
            break
    
    cont_loss = sum(cont_loss_list)
    if (rate == min(cont_loss_list) and cont_loss <= min_loss_rate) or cont_loss <= break_loss_rate:
        return klines[0].close

    return 0

def find_targets(
        m: MarketClient, min_loss_rate=MIN_LOSS_RATE,
        break_loss_rate=BREAK_LOSS_RATE, end=0, min_before=180
    ):
    symbols = m.get_all_symbols_info().keys()
    targets: 'dict[str, float]' = {}

    def worker(symbol):
        price = check_buy(m, symbol, min_loss_rate, break_loss_rate, end, min_before)
        if price:
            targets[symbol] = price
    
    run_thread_pool([(worker, (symbol,)) for symbol in symbols], True, 8)
    return targets

def main(user: User):
    client = init_dealer(user)
    user.start(trade_update_callback(client), error_callback('order'))
    targets = find_targets(client.market_client, end=1)

    print(targets)
    kill_all_threads()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num', default=-1, type=int)
    args = parser.parse_args()

    logger.info('Dealer')
    users = init_users(num=args.num)
    run_process([(main, (user,), user.username) for user in users], is_lock=True)
