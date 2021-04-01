from math import log
import sys
import time

from huobi.constant import OrderSide, OrderSource, OrderType, OrderState
from retry import retry

from goodmorning import init_users
from parallel import run_process
from report import wx_push
from user import User
from utils import logger, user_config

MAX_ORDER_RETRY = 5

class FailBuyException(Exception):
    pass

@retry(tries=-1, delay=0.05, logger=logger)
def buy_at(user: 'User', symbol: str, amount: float):
    order_id = user.trade_client.create_order(
        symbol, user.account_id, OrderType.BUY_MARKET,
        amount, 0, OrderSource.API
    )
    user.buy_id.append(order_id)

def buy_task(user: User, symbol: str):
    @retry(FailBuyException, tries=MAX_ORDER_RETRY, delay=0.5, logger=logger)
    def _buy():
        buy_at(user, symbol, user.buy_amount)

        while user.trade_client.get_open_orders(symbol, user.account_id, OrderSide.BUY):
            pass

        order = user.trade_client.get_order(user.buy_id[0])
        if order.state in [OrderState.CANCELED, OrderState.CANCELLING, OrderState.FAILED]:
            user.buy_id.clear()
            raise FailBuyException(f'order failed: {order.state}')

    logger.info(f'User {user.username} start to run, use {user.buy_amount} USDT')
    try:
        _buy()
    except:
        logger.error(f'Fail to buy after {MAX_ORDER_RETRY} tries')
        wx_push(f'尝试购买{symbol.upper()}失败累计{MAX_ORDER_RETRY}次, 放弃此次购买', [user.wxuid])
        return
    
    currency = symbol[:-4]
    user.balance = user.get_currency_balance([currency])
    num = user.balance[currency]
    price = user.buy_amount / num

    logger.info(f'User {user.username} speed {user.buy_amount} USDT get {num} {currency.upper()} with price {price}')
    msg = f'用户{user.username}花费{user.buy_amount} USDT购入{num} {currency.upper()}, 单价{price}, 请注意自行卖出'
    wx_push(msg, [user.wxuid])

def main():
    FMT  = '%Y-%m-%d %H:%M:%S'
    SYMBOL = sys.argv[1]
    TARGET_TIME_STR = sys.argv[2]
    TARGET_TIME = time.mktime(time.strptime(TARGET_TIME_STR, FMT))
    SPECIAL_AMOUNT = user_config.get('setting', 'SpecialAmount').split(',')

    logger.info(f'Strat to run, try to buy {SYMBOL} at {TARGET_TIME_STR}')

    users = init_users()
    for user, amount in zip(users, SPECIAL_AMOUNT):
        user.buy_amount = float(amount) if amount else 0

    while time.time() < TARGET_TIME - 30:
        time.sleep(1)

    logger.info('Generate sub process for each user')
    run_process([(buy_task, (user, SYMBOL,), user.username) for user in users if user.buy_amount], is_lock=True)

if __name__ == '__main__':
    main()
