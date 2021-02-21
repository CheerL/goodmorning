from utils import logger, config, initial
import time

SELL_INTERVAL = config.getfloat('setting', 'SellInterval')
MODEL = config.get('setting', 'model')

def sell_half_after(users, targets, buy_time, t):
    while time.time() < buy_time + t:
        pass
    else:
        logger.info(f'Sell half after {t}s')
        for user in users:
            sell_amounts = [user.balance[target.base_currency] / 2 for target in targets]
            user.sell(targets, sell_amounts)

def main():
    users, market_client, target_time = initial()
    base_price, base_price_time = market_client.get_base_price(target_time)

    if target_time % (24*60*60) == 16*60*60 or MODEL == 'midnight':
        logger.info('Midnight model')
        targets_1, base_price = market_client.get_target_by_batch(target_time, base_price)
        if targets_1:
            for user in users:
                user.buy(targets_1, [user.buy_amount for _ in targets_1])
        buy_time = time.time()

        targets_2, base_price = market_client.get_target_by_batch(buy_time, base_price)
        if targets_2:
            for user in users:
                user.buy(targets_2, [user.buy_amount for _ in targets_2])
        buy_time_2 = time.time()

        targets_3, base_price = market_client.get_target_by_batch(buy_time_2, base_price)
        if targets_3:
            for user in users:
                user.buy(targets_3, [user.buy_amount for _ in targets_3])

        targets = list(set(targets_1+targets_2+targets_3))
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
    sell_half_after(users, targets, buy_time, SELL_INTERVAL)
    sell_half_after(users, targets, buy_time, 2*SELL_INTERVAL)

    logger.debug('Exit')

if __name__ == '__main__':
    main()

