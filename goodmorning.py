from utils import logger, config, strftime, initial
import time


SELL_INTERVAL = config.getfloat('setting', 'SellInterval')


def main():
    targets = []
    market_client, users = initial()

    # target_time = get_target_time()
    target_time = time.time() + 5
    logger.debug(f'Target time is {strftime(target_time)}')

    base_price, base_price_time = market_client.get_base_price(target_time)

    targets = market_client.get_target(target_time, base_price, base_price_time)
    if not targets:
        return

    for user in users:
        user.buy(targets)

    buy_time = time.time()
    time.sleep(0.5)

    ## check balance
    for user in users:
        user.check_balance(targets)

    ## sell
    while time.time() < buy_time + SELL_INTERVAL:
        pass
    else:
        logger.info(f'Sell half after {SELL_INTERVAL}s')
        for user in users:
            user.sell(targets)

    while time.time() < buy_time + 2 * SELL_INTERVAL:
        pass
    else:
        logger.info(f'Sell half after {2 * SELL_INTERVAL}s')
        for user in users:
            user.sell(targets)

    logger.debug('Exit')

if __name__ == '__main__':
    main()

