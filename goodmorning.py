from utils import logger, config, initial
import time

SELL_INTERVAL = config.getfloat('setting', 'SellInterval')
SELL_AFTER = config.getfloat('setting', 'SellAfter')
SELL_RATE = config.getfloat('setting', 'SellRate')
MIDNIGHT = config.getboolean('setting', 'Midnight')
MIDNIGHT_INTERVAL = config.getfloat('setting', 'MidnightInterval')

def sell_half_after(users, targets, buy_time, t):
    while time.time() < buy_time + t:
        pass
    else:
        logger.info(f'Sell half after {t}s')
        for user in users:
            sell_amounts = [user.balance[target.base_currency] / 2 for target in targets]
            user.sell(targets, sell_amounts)

def sell_algo_left_market(users, targets, buy_time, market_client):
    for user in users:
        sell_amounts = [user.balance[target.base_currency] for target in targets]
        user.sell_algo(targets, sell_amounts, market_client.price_record, SELL_RATE)

    while time.time() < buy_time + SELL_AFTER:
        pass
    else:
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
                user.sell_algo(targets_1, sell_amounts, market_client.price_record, SELL_RATE)


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
                user.sell_algo(targets_2, sell_amounts, market_client.price_record, SELL_RATE)


        buy_time = time.time()
        targets = list(set(targets_1+targets_2))
        if not targets:
            logger.warning('No targets in 2 tries, exit')
            return

        while time.time() < buy_time + SELL_AFTER:
            pass
        else:
            for user in users:
                user.cancel_algo_and_sell()
    else:
        logger.info('General model')
        targets = market_client.get_target(target_time, base_price, base_price_time)
        if not targets:
            return

        for user in users:
            user.buy(targets, [user.buy_amount for _ in targets])

        buy_time = time.time()

        for user in users:
            user.check_balance()

        for user in users:
            sell_amounts = [user.balance[target.base_currency] for target in targets]
            user.sell_algo(targets, sell_amounts, market_client.price_record, SELL_RATE)

        while time.time() < buy_time + SELL_AFTER:
            pass
        else:
            for user in users:
                user.cancel_algo_and_sell()


    for user in users:
        user.report()

    logger.debug('Exit')

if __name__ == '__main__':
    main()

