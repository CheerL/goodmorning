from utils import logger, config, initial
import time

SELL_INTERVAL = config.getfloat('setting', 'SellInterval')
SELL_AFTER = config.getfloat('setting', 'SellAfter')
SELL_RATE = config.getfloat('setting', 'SellRate')
MIDNIGHT = config.getboolean('setting', 'Midnight')

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
        logger.info(f'Sell left after {SELL_AFTER}s')
        for user in users:
            user.cancel_algo()
            user.get_balance(targets)
            sell_amounts = [user.balance[target.base_currency] for target in targets]
            user.sell(targets, sell_amounts)

def main():
    users, market_client, target_time = initial()
    base_price, base_price_time = market_client.get_base_price(target_time)

    if MIDNIGHT or target_time % (24*60*60) == 16*60*60:
        logger.info('Midnight model')
        targets_1, base_price = market_client.get_target_midnight(target_time, base_price, unstop=True)
        if targets_1:
            for user in users:
                user.buy(targets_1, [user.buy_amount for _ in targets_1])
        buy_time_1 = time.time()

        targets_2, base_price = market_client.get_target_midnight(buy_time_1, base_price)
        
        if targets_2:
            for user in users:
                user.buy(targets_2, [user.buy_amount for _ in targets_2])
        buy_time_2 = time.time()
        targets = list(set(targets_1+targets_2))

        targets_3, _ = market_client.get_target_midnight(buy_time_2, base_price)
        targets_3 = [target for target in targets_3 if target not in targets]
        if targets_3:
            for user in users:
                user.buy(targets_3, [user.buy_amount for _ in targets_3])

        targets = targets + targets_3
        buy_time = buy_time_2
        if not targets:
            logger.warning('No targets in 3 tries, exit')
            return
    else:
        logger.info('General model')
        targets = market_client.get_target(target_time, base_price, base_price_time)
        if not targets:
            return

        for user in users:
            user.buy(targets, [user.buy_amount for _ in targets])

        buy_time = time.time()

    time.sleep(0.5)

    ## check balance
    for user in users:
        user.check_balance(targets)

    ## sell
    sell_algo_left_market(users, targets, buy_time, market_client)
    # sell_half_after(users, targets, buy_time, SELL_INTERVAL)
    # sell_half_after(users, targets, buy_time, 2*SELL_INTERVAL)

    logger.debug('Exit')

if __name__ == '__main__':
    main()

