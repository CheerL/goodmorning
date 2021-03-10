import math
import time

from huobi.client.account import AccountClient
from huobi.client.algo import AlgoClient
from huobi.client.trade import TradeClient
from huobi.constant import *
from huobi.utils import *

from utils import config, logger, wxpush, strftime, timeout_handle


SELL_RATE = config.getfloat('setting', 'SellRate')
SELL_MIN_RATE = config.getfloat('setting', 'SellMinRate')

class User:
    def __init__(self, access_key, secret_key, buy_amount, wxuid):
        self.access_key = access_key
        self.sercet_key = secret_key
        self.account_client = AccountClient(api_key=access_key, secret_key=secret_key)
        self.trade_client = TradeClient(api_key=access_key, secret_key=secret_key)
        self.algo_client = AlgoClient(api_key=access_key, secret_key=secret_key)
        self.account_id = next(filter(
            lambda account: account.type=='spot' and account.state =='working',
            self.account_client.get_accounts()
        )).id
        
        if buy_amount.startswith('/'):
            usdt_balance = self.get_currency_balance(['usdt'])['usdt']
            self.buy_amount =  max(math.floor(usdt_balance / float(buy_amount[1:])), 5)
        else:
            self.buy_amount = float(buy_amount)
        self.wxuid = wxuid

        self.balance = {}
        self.buy_order_list = []
        self.sell_order_list = []
        self.buy_id = []
        self.sell_id = []
        self.sell_algo_id = []

    @staticmethod
    def _check_amount(amount, symbol_info):
        precision_num = 10 ** symbol_info.amount_precision
        return math.floor(amount * precision_num) / precision_num

    @staticmethod
    def _check_price(price, symbol_info):
        precision_num = 10 ** symbol_info.price_precision
        return math.floor(price * precision_num) / precision_num

    def buy(self, targets, amounts):
        buy_order_list = [{
            "symbol": target.symbol,
            "account_id": self.account_id,
            "order_type": OrderType.BUY_MARKET,
            "source": OrderSource.SPOT_API,
            "price": 1,
            "amount": self._check_amount(max(
                amount,
                target.min_order_value
            ), target)}
            for target, amount in zip(targets, amounts)
            if amount > 0
        ]
        if buy_order_list:
            self.buy_id.extend(self.trade_client.batch_create_order(buy_order_list))
            self.buy_order_list.extend(buy_order_list)
            logger.debug(f'User {self.account_id} buy report')
            for order in buy_order_list:
                logger.debug(f'Speed {order["amount"]} USDT to buy {order["symbol"][:-4].upper()}')

        

    def sell(self, targets, amounts):
        sell_order_list = [{
            "symbol": target.symbol,
            "account_id": self.account_id,
            "order_type": OrderType.SELL_MARKET,
            "source": OrderSource.SPOT_API,
            "price": 1,
            "amount": self._check_amount(amount, target)}
            for target, amount in zip(targets, amounts)
        ]
        sell_order_list = [
            order for order, target in zip(sell_order_list, targets)
            if order['amount'] >= target.sell_market_min_order_amt
        ]
        
        if sell_order_list:
            self.sell_id.extend(self.trade_client.batch_create_order(sell_order_list))
            self.sell_order_list.extend(sell_order_list)
            logger.debug(f'User {self.account_id} sell report')
            for order in sell_order_list:
                logger.debug(f'Sell {order["amount"]} {order["symbol"][:-4].upper()} with market price')


    def sell_limit(self, targets, amounts, rate=SELL_RATE, min_rate=SELL_MIN_RATE):
        sell_order_list = [{
            "symbol": target.symbol,
            "account_id": self.account_id,
            "order_type": OrderType.SELL_LIMIT,
            "source": OrderSource.SPOT_API,
            "price": self._check_price(max(
                rate * target.init_price,
                min_rate * target.buy_price
            ), target),
            "amount": self._check_amount(amount, target)}
            for target, amount in zip(targets, amounts)
        ]
        sell_order_list = [
            order for order, target in zip(sell_order_list, targets)
            if order['amount'] >= target.limit_order_min_order_amt
        ]

        if sell_order_list:
            self.sell_id.extend(self.trade_client.batch_create_order(sell_order_list))
            self.sell_order_list.extend(sell_order_list)
            logger.debug(f'User {self.account_id} sell report')
            for order in sell_order_list:
                logger.debug(f'Sell {order["amount"]} {order["symbol"][:-4].upper()} with price {order["price"]}')

    def sell_algo(self, targets, amounts, rate=SELL_RATE, min_rate=SELL_MIN_RATE):
        for target, amount in zip(targets, amounts):
            if amount <= 0:
                continue

            symbol = target.symbol
            stop_price = str(self._check_price(max(
                rate * target.init_price,
                min_rate * target.buy_price
            ), target))
            amount = str(self._check_amount(max(
                amount,
                target.min_order_amt,
                target.sell_market_min_order_amt
            ), target))
            client_id = (symbol + stop_price + str(time.time())).replace('.', '_')
            sell_order_id = self.algo_client.create_order(
                account_id=self.account_id, symbol=symbol, order_side=OrderSide.SELL,
                order_type='market', stop_price=stop_price, order_size=amount,
                client_order_id=client_id
            )
            order = {
                "symbol": symbol,
                "price": stop_price,
                "amount": amount,
                "id": sell_order_id
            }
            self.sell_algo_id.append(client_id)
            self.sell_order_list.append(order)
            logger.debug(f'Sell {order["amount"]} {order["symbol"][:-4].upper()} with market price')

    @timeout_handle([])
    def get_open_orders(self, targets, side=OrderSide.SELL):
        symbols = ','.join([target.symbol for target in targets])
        open_orders = self.trade_client.get_open_orders(symbols, self.account_id, side)
        return open_orders

    def cancel_and_sell(self, targets):
        open_orders = self.get_open_orders(targets)
        if open_orders:
            symbols = ','.join([target.symbol for target in targets])
            self.trade_client.cancel_orders(symbols, [order.id for order in open_orders])
            logger.info(f'User {self.account_id} cancel all open sell orders')
            time.sleep(0.2)
            sell_amount = [float(order.amount) for order in open_orders]
            target_dict = {
                target.symbol:target
                for target in targets
            }
            sell_targets = [target_dict[order.symbol] for order in open_orders]
            self.sell(sell_targets, sell_amount)

    def cancel_algo_and_sell(self, targets):
        open_orders = self.algo_client.get_open_orders() or []
        if open_orders:
            open_ids = [order.clientOrderId for order in open_orders]
            self.algo_client.cancel_orders(open_ids)
            logger.info(f'User {self.account_id} cancel all open algo orders')

            sell_amount = [float(order.amount) for order in open_orders]
            target_dict = {
                target.symbol:target
                for target in targets
            }
            sell_targets = [target_dict[order.symbol] for order in open_orders]
            self.sell(sell_targets, sell_amount)
            self.sell_algo_id = list(set(self.sell_algo_id)-set(open_ids))

    def get_currency_balance(self, currencies):
        return {
            currency.currency: float(currency.balance)
            for currency in self.account_client.get_balance(self.account_id)
            if currency.currency in currencies and currency.type == 'trade'
        }

    def get_balance(self, targets):
        while True:
            target_currencies = [target.base_currency for target in targets]
            self.balance = self.get_currency_balance(target_currencies)
            if not list(set(target_currencies)-set(self.balance.keys())):
                break

    def check_balance(self, targets):
        self.get_balance(targets)

        logger.debug(f'User {self.account_id} balance report')
        for target, order in zip(targets, self.buy_order_list):
            target_balance = self.balance[target.base_currency]
            if target_balance > 10 ** -target.amount_precision:
                buy_price = order["amount"] / target_balance * 0.998
                target.buy_price = max(buy_price, target.buy_price)
                logger.debug(f'Get {target_balance} {target.base_currency.upper()} with average price {buy_price}')
            else:
                logger.debug(f'Get 0 {target.base_currency.upper()}')

    def report(self):
        orders = [
            self.trade_client.get_order(order.order_id)
            for order in self.buy_id + self.sell_id
            if order.order_id
        ]

        order_info = [{
            'symbol': order.symbol,
            'time': strftime(order.finished_at / 1000, fmt='%Y-%m-%d %H:%M:%S.%f'),
            'price': round(float(order.filled_cash_amount) / float(order.filled_amount), 6),
            'amount': round(float(order.filled_amount), 6),
            'fee': round(float(order.filled_fees), 6),
            'currency': order.symbol[:-4].upper(),
            'vol': float(order.filled_cash_amount),
            'direct': order.type.split('-')[0]}
            for order in orders
            if order.state == 'filled'
        ]
        buy_info = list(filter(lambda x: x['direct']=='buy', order_info))
        sell_info = list(filter(lambda x: x['direct']=='sell', order_info))

        pay = round(sum([each['vol'] for each in buy_info]), 4)
        if pay <= 0:
            logger.warning(f'NO REPORT for User {self.account_id}')
            return

        income = round(sum([each['vol'] for each in sell_info]), 4)
        profit = round(income - pay, 4)
        percent = round(profit / pay * 100, 4)

        logger.info(f'REPORT for user {self.account_id}')
        logger.info('Buy')
        for each in buy_info:
            currency = each['currency']
            symbol_name = '/'.join([currency, 'USDT'])
            vol = each['vol']
            amount = each['amount']
            price = each['price']
            fee = round(each['fee'] * price, 6)
            each['fee'] = fee
            logger.info(f'{symbol_name}: use {vol} USDT, get {amount} {currency}, price {price}, fee {fee} {currency}, at {each["time"]}')

        logger.info('Sell')
        for each in sell_info:
            currency = each['currency']
            symbol_name = '/'.join([currency, 'USDT'])
            vol = each['vol']
            amount = each['amount']
            price = each['price']
            fee = each['fee']
            logger.info(f'{symbol_name}: use {amount} {currency}, get {vol} USDT, price {price}, fee {fee} USDT, at {each["time"]}')

        logger.info(f'Totally pay {pay} USDT, get {income} USDT, profit {profit} USDT, {percent}%')

        if self.wxuid:
            summary = f'{strftime(time.time())} 本次交易支出 {pay}, 收入 {income}, 利润 {profit}, 收益率 {percent}%'
            msg = '''
### 买入记录

| 币种 | 时间 |价格 | 成交量 | 成交额 | 手续费 |
| ---- | ---- | ---- | ---- | ---- | ---- |
''' + \
'\n'.join([
    f'| {each["currency"]} | {each["time"]} | {each["price"]} | {each["amount"]} | {each["vol"]} | {each["fee"]} |'
    for each in buy_info
]) + '''
### 卖出记录

| 币种 | 时间 | 价格 | 成交量 | 成交额 | 手续费 |
| ---- | ---- | ---- | ---- | ---- | ---- |
''' + \
'\n'.join([
    f'| {each["currency"]} | {each["time"]} | {each["price"]} | {each["amount"]} | {each["vol"]} | {each["fee"]} |'
    for each in sell_info
]) + f'''
### 总结
            
- 支出: **{pay} USDT**

- 收入: **{income} USDT**

- 利润: **{profit} USDT**

- 收益率: **{percent} %**
'''
            wxpush(content=msg, uids=[self.wxuid], content_type=3, summary=summary)
