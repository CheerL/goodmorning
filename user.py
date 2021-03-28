import math
import time

import huobi
from huobi.client.account import AccountClient
from huobi.client.trade import TradeClient
from huobi.constant import OrderSource, OrderSide, OrderType

from utils import config, logger, strftime, timeout_handle
from report import wx_report, add_profit, get_profit, wx_name


SELL_RATE = config.getfloat('setting', 'SellRate')
SELL_MIN_RATE = config.getfloat('setting', 'SellMinRate')

class User:
    def __init__(self, access_key, secret_key, buy_amount, wxuid):
        self.access_key = access_key
        self.sercet_key = secret_key
        self.account_client = AccountClient(api_key=access_key, secret_key=secret_key)
        self.trade_client = TradeClient(api_key=access_key, secret_key=secret_key)
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
        self.username = wx_name(wxuid)

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

    @timeout_handle([])
    def get_open_orders(self, targets, side=OrderSide.SELL) -> 'list[huobi.model.trade.order.Order]':
        open_orders = []
        all_symbols = [target.symbol for target in targets]
        for symbols in [all_symbols[i:i+10] for i in range(0, len(all_symbols), 10)]:
            open_orders.extend(self.trade_client.get_open_orders(','.join(symbols), self.account_id, side))
        return open_orders

    def cancel_and_sell(self, targets):
        open_orders = self.get_open_orders(targets)
        if open_orders:
            all_symbols = [target.symbol for target in targets]
            for symbols in [all_symbols[i:i+10] for i in range(0, len(all_symbols), 10)]:
                self.trade_client.cancel_orders(','.join(symbols), [order.id for order in open_orders if order.symbol in symbols])
            logger.info(f'User {self.account_id} cancel all open sell orders')

        self.get_balance(targets)
        amounts = [self.balance[target.base_currency] for target in targets]
        self.sell(targets, amounts)

        target_currencies = [target.base_currency for target in targets]
        while True:
            frozen_balance = self.get_currency_balance(target_currencies, 'frozen')
            if not any(frozen_balance.values()):
                break
            else:
                time.sleep(0.1)
        
        self.get_balance(targets)
        amounts = [self.balance[target.base_currency] for target in targets]
        self.sell(targets, amounts)

    def buy_and_sell(self, targets):
        self.buy(targets, [self.buy_amount for _ in targets])
        self.check_balance(targets)
        sell_amounts = [self.balance[target.base_currency] for target in targets]
        self.sell_limit(targets, sell_amounts)

    def get_currency_balance(self, currencies, balance_type='trade'):
        return {
            currency.currency: float(currency.balance)
            for currency in self.account_client.get_balance(self.account_id)
            if currency.currency in currencies and currency.type == balance_type
        }

    def get_balance(self, targets):
        while self.get_open_orders(targets, side=None):
            pass

        target_currencies = [target.base_currency for target in targets]
        self.balance = self.get_currency_balance(target_currencies)


    def check_balance(self, targets):
        self.get_balance(targets)

        logger.debug(f'User {self.account_id} balance report')
        for target, order in zip(targets, self.buy_order_list):
            target_balance = self.balance[target.base_currency]
            if target_balance > 10 ** -target.amount_precision:
                buy_price = order["amount"] / target_balance * 0.998
                target.buy_price = buy_price
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

        income = round(sum([each['vol'] - each['fee'] for each in sell_info]), 4)
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
        add_profit(self.account_id, pay, income, profit, percent)
        total_profit, month_profit = get_profit(self.account_id)
        wx_report(self.wxuid, self.username, pay, income, profit, percent, buy_info, sell_info, total_profit, month_profit)
