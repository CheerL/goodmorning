from utils import logger, config, initial
import time

SELL_INTERVAL = config.getfloat('setting', 'SellInterval')
SELL_AFTER = config.getfloat('setting', 'SellAfter')
MIDNIGHT = config.getboolean('setting', 'Midnight')
MIDNIGHT_INTERVAL = config.getfloat('setting', 'MidnightInterval')

def cancel_after(users, t):
    while time.time() < t:
        open_orders = []
        for user in users:
            open_orders.extend(user.algo_client.get_open_orders())
        
        if open_orders:
            time.sleep(1)
        else:
            break

    for user in users:
        user.cancel_algo_and_sell()

def main():
    users, market_client, target_time = initial()
    base_price, base_price_time = market_client.get_base_price(target_time)

    if MIDNIGHT or target_time % (24*60*60) == 16*60*60:
        logger.info('Midnight model')
        targets_1 = market_client.get_target(
            target_time, base_price, change_base=False, interval=MIDNIGHT_INTERVAL, unstop=True
        )
        if targets_1:
            for user in users:
                user.buy(targets_1, [user.buy_amount for _ in targets_1])
            for user in users:
                user.check_balance(targets_1)
            for user in users:
                sell_amounts = [user.balance[target.base_currency] for target in targets_1]
                user.sell_algo(targets_1, sell_amounts)


        targets_2 = market_client.get_target(
            target_time, base_price, change_base=False, interval=MIDNIGHT_INTERVAL
        )
        if targets_2:
            for user in users:
                user.buy(targets_2, [user.buy_amount for _ in targets_2])
            for user in users:
                user.check_balance(targets_2)
            for user in users:
                sell_amounts = [user.balance[target.base_currency] for target in targets_2]
                user.sell_algo(targets_2, sell_amounts)


        buy_time = time.time()
        targets = list(set(targets_1+targets_2))
        if not targets:
            logger.warning('No targets in 2 tries, exit')
            return

        # cancel_after(users, buy_time + SELL_AFTER)
    else:
        logger.info('General model')
        targets = market_client.get_target(target_time, base_price, base_price_time)
        if not targets:
            logger.debug('Exit')
            return

        for user in users:
            user.buy(targets, [user.buy_amount for _ in targets])

        buy_time = time.time()

        for user in users:
            user.check_balance(targets)

        for user in users:
            sell_amounts = [user.balance[target.base_currency] for target in targets]
            user.sell_algo(targets, sell_amounts)

    cancel_after(users, buy_time + SELL_AFTER)

    for user in users:
        user.report()


if __name__ == '__main__':
    main()

