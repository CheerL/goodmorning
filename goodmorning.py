from utils import logger, config, strftime, get_target_time, get_info, check_amount

import time

from huobi.constant import *
from huobi.utils import *

BOOT_PRECENT = config.getfloat('setting', 'BootPrecent')
BUY_AMOUNT = config.getfloat('setting', 'BuyAmount')
SELL_INTERVAL = config.getfloat('setting', 'SellInterval')
BEFORE = config.getint('setting', 'Before')
AFTER = config.getint('setting', 'After')
MAX_AFTER = config.getint('setting', 'MaxAfter')

def get_price(market_client):
    market_data = market_client.get_market_tickers()
    price = {
        pair.symbol: pair.close
        for pair in market_data
        if pair.symbol.endswith('usdt')
    }
    return price

def get_increase(market_client, initial_price):
    price = get_price(market_client)
    increase = [
        (symbol, close, (close - initial_price[symbol]) / initial_price[symbol])
        for symbol, close in price.items()
        if symbol in initial_price and symbol.endswith('usdt')
    ]
    increase = sorted(increase, key=lambda pair: pair[2], reverse=True)
    return increase, price

def get_balance(user, targets):
    target_currencies = [target.base_currency for target in targets]
    balance = {
        info.currency: float(info.balance)
        for info in user.account_client.get_balance(user.account_id)
        if info.currency in target_currencies and info.type == 'trade'
    }
    return balance

def main():
    symbols_info, market_client, users = get_info()
    targets = []

    # target_time = get_target_time()
    target_time = time.time() + 5
    logger.debug(f'Target time is {strftime(target_time)}')

    while True:
        now = time.time()
        if now < target_time - 310:
            logger.info('Wait 5mins')
            time.sleep(300)
        else:
            base_price = get_price(market_client)
            if now > target_time - BEFORE:
                base_price_time = now
                logger.debug(f'Get base price successfully/')
                break
            else:
                time.sleep(1)

    while True:
        try:
            now = time.time()
            increase, price = get_increase(market_client, base_price)
            big_increase = [item for item in increase if item[2] > BOOT_PRECENT]
            if big_increase:
                for symbol, now_price, target_increase in big_increase:
                    targets.append(symbols_info[symbol])
                    logger.debug(f'Find target: {symbol.upper()}, initial price {base_price[symbol]}, now price {now_price} , increase {round(target_increase * 100, 4)}%')
                break
            elif now > target_time + MAX_AFTER:
                logger.warning(f'Fail to find target in {MAX_AFTER}s, exit')
                return
            else:
                logger.info('\t'.join([f'{index+1}. {data[0].upper()} {round(data[2]*100, 4)}%' for index, data in enumerate(increase[:3])]))
                if now - base_price_time > AFTER:
                    base_price_time = now
                    base_price = price
                    logger.info('User now base price')
                time.sleep(0.1)
        except:
            pass

    for target in targets:
        buy_amount = check_amount(max(
            BUY_AMOUNT,
            target.min_order_value
        ), target)

        for user in users:
            user.trade_client.create_spot_order(
                symbol=target.symbol, account_id=user.account_id,
                order_type=OrderType.BUY_MARKET,
                amount=buy_amount, price=1
            )
            logger.debug(f'Speed {buy_amount} USDT to buy {target.base_currency}')

    buy_time = time.time()
    time.sleep(0.5)

    ## check balance
    for user in users:
        user.balance = get_balance(user, targets)
        for target in targets:
            target_balance = user.balance[target.base_currency]
            if target_balance > 10 ** -target.amount_precision:
                logger.debug(f'Get {target_balance} {target.base_currency.upper()} with average price {buy_amount / target_balance}')

    ## sell
    while time.time() < buy_time + SELL_INTERVAL:
        pass
    else:
        for user in users:
            for target in targets:
                sell_amount = check_amount(max(
                    user.balance[target.base_currency] / 2,
                    target.min_order_amt,
                    target.sell_market_min_order_amt
                ), target)
                user.trade_client.create_spot_order(
                    symbol=target.symbol, account_id=user.account_id,
                    order_type=OrderType.SELL_MARKET,
                    amount=sell_amount, price=1
                )
                logger.debug(f'Sell {sell_amount} {target.base_currency.upper()} with market price after 5s')

    while time.time() < buy_time + 2 * SELL_INTERVAL:
        pass
    else:
        for user in users:
            for target in targets:
                sell_amount = check_amount(max(
                    user.balance[target.base_currency] / 2,
                    target.min_order_amt,
                    target.sell_market_min_order_amt
                ), target)
                user.trade_client.create_spot_order(
                    symbol=target.symbol, account_id=user.account_id,
                    order_type=OrderType.SELL_MARKET,
                    amount=sell_amount, price=1
                )
                logger.debug(f'Sell {sell_amount} {target.base_currency.upper()} with market price after 10s')

    logger.debug('Exit')

if __name__ == '__main__':
    main()

