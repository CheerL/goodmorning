import time

from parallel import run_thread
from rpc_generator import get_dealer_server
from utils import config, kill_all_threads, logger

from goodmorning import initial

SELL_AFTER = config.getfloat('setting', 'SellAfter')
MAX_BUY = config.getint('setting', 'MaxBuy')
users, market_client, target_time = initial()


def buy_and_sell(user, targets):
    user.buy(targets, [user.buy_amount for _ in targets])
    user.check_balance(targets)
    sell_amounts = [user.balance[target.base_currency] for target in targets]
    user.sell_limit(targets, sell_amounts)

class DealerHandler:
    def __init__(self):
        self.targets = []

    def buy_signal(self, symbol, price, init_price):
        print(symbol, price, 'buy')
        if len(self.targets) >= MAX_BUY:
            return

        target = market_client.symbols_info[symbol]
        target.init_price = init_price
        target.buy_price = price
        # run_thread([
        #     (buy_and_sell, (user, [target], ))
        #     for user in users
        # ], is_lock=False)
        self.targets.append(target)

    def sell_signal(self, symbol, price, init_price):
        print(symbol, price, 'sell')
        target = market_client.symbols_info[symbol]
        # run_thread([(user.cancel_and_sell, ([target], )) for user in users], is_lock=False)

    def alive(self):
        return 'alive'


def main():
    handler = DealerHandler()
    server = get_dealer_server(handler)
    run_thread([(server.serve, ())], is_lock=False)

    sell_time = target_time + SELL_AFTER
    time.sleep(max(sell_time - time.time() - 5, 1))

    while time.time() < sell_time:
        pass

    server.close()
    server.trans.close()
    logger.info('Time to cancel')
    run_thread([(user.cancel_and_sell, (handler.targets, )) for user in users], is_lock=True)
    run_thread([(user.report, ()) for user in users], is_lock=True)
    kill_all_threads()

if __name__ == '__main__':
    main()
