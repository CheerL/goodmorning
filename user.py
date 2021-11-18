import math
import time

import huobi
from huobi.client.account import AccountClient
from huobi.client.trade import TradeClient
from huobi.constant import OrderSource, OrderSide, OrderType, AccountBalanceMode
from huobi.model.account.account_update_event import AccountUpdateEvent, AccountUpdate
from retry.api import retry
from threading import Timer

from utils import config, logger, strftime, timeout_handle
from report import wx_report, add_profit, get_profit, wx_name
from target import Target
from order import OrderSummary, OrderSummaryStatus

FINAL_STOP_PROFIT_TIME = int(config.getfloat('time', 'FINAL_STOP_PROFIT_TIME'))
CLEAR_TIME = int(config.getint('time', 'CLEAR_TIME'))
STOP_PROFIT_RATE_HIGH = config.getfloat('sell', 'STOP_PROFIT_RATE_HIGH')
STOP_PROFIT_RATE_LOW = config.getfloat('sell', 'STOP_PROFIT_RATE_LOW')
ALL_STOP_PROFIT_RATE = config.getfloat('sell', 'ALL_STOP_PROFIT_RATE')
IOC_RATE = config.getfloat('sell', 'IOC_RATE')
IOC_BATCH_NUM = config.getint('sell', 'IOC_BATCH_NUM')
HIGH_STOP_PROFIT_HOLD_TIME = config.getfloat('time', 'HIGH_STOP_PROFIT_HOLD_TIME')
AccountBalanceMode.TOTAL = '2'

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

        self.balance: dict[str, float] = {}
        self.balance_update_time: dict[str, float] = {}
        self.buy_amount = buy_amount
        self.orders: dict[str, dict[str, list[OrderSummary]]] = {'buy': {}, 'sell': {}}

        self.wxuid = wxuid.split(';')

        self.buy_order_list = []
        self.sell_order_list = []
        self.buy_id = []
        self.sell_id = []
        self.username = wx_name(self.wxuid[0])

    
    def get_asset(self):
        asset = float(self.account_client.get_account_asset_valuation('spot', 'USD').balance)
        return asset

    @retry(tries=3, delay=0.01)
    def set_start_asset(self):
        self.start_asset = self.get_asset()
        self.all_stop_profit_asset = self.start_asset + self.usdt_balance * ALL_STOP_PROFIT_RATE / 100

    def start(self, callback, error_callback):
        self.account_client.sub_account_update(AccountBalanceMode.TOTAL, self.balance_callback, error_callback('account'))
        self.trade_client.sub_order_update('*', callback, error_callback('trade'))

        while 'usdt' not in self.balance:
            time.sleep(0.1)
        
        self.usdt_balance = self.balance['usdt']
        if isinstance(self.buy_amount, str) and self.buy_amount.startswith('/'):
            self.buy_amount = float(max(math.floor(self.usdt_balance / float(self.buy_amount[1:])), 5))
        else:
            self.buy_amount = float(self.buy_amount)
        

    def balance_callback(self, event: AccountUpdateEvent):
        update: AccountUpdate = event.data
        if float(update.balance) - float(update.available) > 1e-8:
            return

        if not update.changeTime:
            update.changeTime = 0

        if (update.currency not in self.balance_update_time or
            update.changeTime == 0 or
            update.changeTime > self.balance_update_time[update.currency]
        ):
            self.balance[update.currency] = float(update.balance)
            self.balance_update_time[update.currency] = int(update.changeTime) / 1000

    def buy(self, target: Target, amount, price=None, limit=False):
        symbol = target.symbol
        order_summary = OrderSummary(symbol, 'buy')
        self.orders['buy'].setdefault(symbol, []).append(order_summary)
        
        if limit:
            price = target.check_price(price or target.get_buy_price())
            order_type = OrderType.BUY_LIMIT
            amount = target.check_amount(max(
                amount / price,
                target.limit_order_min_order_amt
            ))
            order_summary.created_amount = amount
            order_summary.created_price = price
            logger.debug(f'Buy {amount} USDT {symbol[:-4]} with price {price}')
        else:
            price = 1
            amount = target.check_amount(max(
                amount,
                target.min_order_value
            ))
            order_type = OrderType.BUY_MARKET
            order_summary.created_vol = amount
            logger.debug(f'Buy {amount} USDT {symbol[:-4]} with market price')
        
        order = dict(
            symbol=symbol,
            account_id=self.account_id,
            order_type=order_type,
            amount=amount,
            price=price,
            source=OrderSource.SPOT_API
        )
        order_summary.remain_amount = amount

        try:
            order_id = self.trade_client.create_order(**order)
            self.buy_id.append(order_id)
            self.buy_order_list.append(order)
            order_summary.order_id = order_id
            return order_summary
        except Exception as e:
            order_summary.error(e)
            logger.error(e)
            self.orders['buy'][symbol].remove(order_summary)
            raise Exception(e)

    def sell(self, target: Target, amount, price=None, limit=False, ioc=False):
        symbol = target.symbol
        order_summary = OrderSummary(symbol, 'sell')
        self.orders['sell'].setdefault(symbol, []).append(order_summary)
        amount = target.check_amount(amount)
        
        if limit:
            price = target.check_price(price or target.stop_profit_price)
            assert amount >= target.limit_order_min_order_amt, f'amount too less, {amount}/{target.limit_order_min_order_amt}'
            assert price * amount >= target.min_order_value, f'vol too less, {price * amount}/{target.min_order_value}'
            order_type=OrderType.SELL_LIMIT if not ioc else OrderType.SELL_IOC
            order_summary.created_price = price
            logger.debug(f'Sell {amount} {symbol[:-4]} with price {price}')
        else:
            price = 1
            assert amount >= target.sell_market_min_order_amt, 'amount too less'
            order_type=OrderType.SELL_MARKET
            logger.debug(f'Sell {amount} {symbol[:-4]} with market price')

        order = dict(
            symbol=symbol,
            account_id=self.account_id,
            amount=amount,
            order_type=order_type,
            price=price,
            source=OrderSource.SPOT_API
        )
        order_summary.created_amount = amount
        order_summary.remain_amount = amount

        try:
            order_id = self.trade_client.create_order(**order)
            self.sell_id.append(order_id)
            self.sell_order_list.append(order)
            order_summary.order_id = order_id
            return order_summary
        except Exception as e:
            order_summary.error(e)
            logger.error(e)
            self.orders['sell'][symbol].remove(order_summary)
            raise Exception(e)

    @timeout_handle([])
    def get_open_orders(self, targets, side=OrderSide.SELL) -> 'list[huobi.model.trade.order.Order]':
        open_orders = []
        all_symbols = [target.symbol for target in targets]
        for symbols in [all_symbols[i:i+10] for i in range(0, len(all_symbols), 10)]:
            open_orders.extend(self.trade_client.get_open_orders(','.join(symbols), self.account_id, side))
        return open_orders

    def check_and_sell(self, targets: 'list[Target]', limit=True):
        @retry(tries=5, delay=0.05)
        def _sell(target: Target, amount, limit=True):
            if limit and amount > target.limit_order_min_order_amt:
                self.sell(target, amount, limit=True)
            elif not limit and amount > target.sell_market_min_order_amt:
                self.sell(target, amount)

        for target in list(targets):
            buy_amount = sum([summary.amount for summary in self.orders['buy'][target.symbol] if summary.order_id])
            sell_amount = sum([
                (summary.created_amount
                if summary.status in [OrderSummaryStatus.CREATED, OrderSummaryStatus.PARTIAL_FILLED]
                else summary.amount)
                for summary in self.orders['sell'][target.symbol]
                if summary.order_id
                ])
            remain_amount = 0.998 * buy_amount - sell_amount
            logger.info(f'{target.symbol} buy {buy_amount} sell {sell_amount} left {remain_amount}')
            try:
                if (remain_amount / buy_amount) > 0.01:
                    _sell(target, remain_amount, limit)
            except:
                pass

    @retry(tries=5, delay=0.05, logger=logger)
    def get_amount(self, currency):
        return self.balance[currency]

    @retry(tries=5, delay=0.05)
    def get_target_amount(self, target, summary=None, buy=False):
        if summary:
            if buy:
                amount = min(self.get_amount(target.base_currency), summary.amount * 0.998)
                assert amount - 0.9 * summary.amount > 0, "Not yet arrived"
            else:
                amount = min(self.get_amount(target.base_currency), summary.remain_amount)
                assert amount - 0.9 * summary.remain_amount > 0, "Not yet arrived"
        else:
            amount = self.get_amount(target.base_currency)
        return amount

    def cancel_and_sell_in_buy_price(self, target: 'Target'):
        def callback(summary=None):
            amount = self.get_target_amount(target, summary)
            self.sell(target, amount, price=target.buy_price, limit=True)

        self.cancel_and_sell(target, callback, market=False)

    def cancel_and_sell_ioc(self, target: Target, price: float, count: int):
        def callback(summary=None):
            amount = self.get_target_amount(target, summary)
            sell_price = price * (1 - IOC_RATE / 100)
            sell_rate = 1 / (IOC_BATCH_NUM - count) if count > 0 else 1 / (IOC_BATCH_NUM - 1)
            sell_amount = amount * sell_rate
            logger.info(f'Try to ioc sell {sell_amount} {target.symbol} with price {sell_price}')
            try:
                self.sell(target, sell_amount, sell_price, limit=True ,ioc=True)
            except Exception as e:
                logger.error(e)

        if count < IOC_BATCH_NUM - 1:
            self.cancel_and_sell(target, callback, False)
        else:
            self.cancel_and_sell(target, market=True)



    def cancel_and_sell(self, target: Target, callback=None, market=True):
        def _callback(summary=None):
            amount = self.get_target_amount(target, summary)
            self.sell(target, amount, limit=(not market))

        symbol = target.symbol
        callback = callback or _callback
        is_canceled = False
        
        if symbol in self.orders['sell']:
            for summary in self.orders['sell'][symbol]:
                if summary.status not in [OrderSummaryStatus.FILLED, OrderSummaryStatus.CANCELED]:
                    if not summary.order_id:
                        continue
                    try:
                        self.trade_client.cancel_order(summary.symbol, summary.order_id)
                        summary.add_cancel_callback(callback, [summary])
                        logger.info(f'Cancel open sell order for {symbol}')
                        is_canceled = True
                    except Exception as e:
                        logger.error(f'{summary.order_id} {summary.status} {summary.symbol} {e}')
                    # break

        if not is_canceled:
            try:
                callback()
            except Exception as e:
                logger.error(e)

    def high_cancel_and_sell(self, targets: 'list[Target]', symbol, price):
        def _callback(summary=None):
            amount = self.get_target_amount(target, summary)
            self.sell(target, amount, (price + 2 * target.buy_price) / 3, limit=True)

        for target in targets:
            if target.symbol == symbol:
                self.cancel_and_sell(target, _callback, market=False)
            else:
                self.turn_low_cancel_and_sell(target, None)

        logger.info(f'Stop profit {symbol}')

    def turn_low_cancel_and_sell(self, target: 'Target', callback=None):
        if target.high_stop_profit:
            target.set_high_stop_profit(False)
            self.cancel_and_sell(target, callback, False)
            logger.info(f'Turn {target.symbol} to low profit')

    def buy_and_sell(self, target: Target, client, limit=False):
        @retry(tries=5, delay=0.05)
        def callback(summary):
            # print('callback')
            if target.symbol not in client.targets:
                client.targets[target.symbol] = target

            if summary.aver_price <=0:
                client.after_buy(target.symbol, 0)
                logger.error(f'Fail to buy {target.symbol}')
                return

            client.after_buy(target.symbol, summary.aver_price)
            amount = self.get_target_amount(target, summary, True)
            now = time.time()
            turn_low_time = summary.ts + HIGH_STOP_PROFIT_HOLD_TIME
            buy_price_sell_time = client.target_time + FINAL_STOP_PROFIT_TIME
            clear_time = client.target_time + CLEAR_TIME
            stop_time = clear_time + 10

            if now > stop_time:
                # print('clear')
                self.sell(target, amount)
            elif now >= clear_time:
                # print('ioc')
                pass
            elif now < turn_low_time:
                # print('high')
                self.sell(target, amount, limit=True)
                if turn_low_time < clear_time:
                    Timer(
                        turn_low_time - now, 
                        self.turn_low_cancel_and_sell, [target, None]
                    ).start()
            elif now < buy_price_sell_time:
                # print('low')
                self.turn_low_cancel_and_sell(target, None)
            elif now >= buy_price_sell_time:
                # print('same')
                self.cancel_and_sell_in_buy_price(target)


        def buy_callback():
            try:
                summary = retry(tries=4, delay=0.05)(self.buy)(target, self.buy_amount, limit=limit)
            except Exception as e:
                logger.error(e)
                client.after_buy(target.symbol, 0)
                return

            summary.check_cancel(client)
            summary.add_filled_callback(callback, [summary])
            summary.add_cancel_callback(callback, [summary])

        Timer(0, buy_callback).start()

    @retry(tries=5, delay=0.1)
    def report(self):
        orders = []
        for order_id in set(self.buy_id + self.sell_id):
            try:
                order = self.trade_client.get_order(order_id)
                orders.append(order)
            except Exception as e:
                logger.error(e)

        order_info = [{
            'symbol': order.symbol,
            'time': strftime(order.finished_at / 1000, fmt='%Y-%m-%d %H:%M:%S.%f'),
            'price': round(float(order.filled_cash_amount) / float(order.filled_amount), 6),
            # 'price': round(float(order.price), 6),
            'amount': round(float(order.filled_amount), 6),
            'fee': round(float(order.filled_fees), 6),
            'currency': order.symbol[:-4].upper(),
            'vol': float(order.filled_cash_amount),
            'direct': order.type.split('-')[0]}
            for order in orders
            if order.state in ['filled', 'partial-filled', 'partial-canceled']
        ]
        buy_info = list(filter(lambda x: x['direct']=='buy', order_info))
        sell_info = list(filter(lambda x: x['direct']=='sell', order_info))

        pay = round(sum([each['vol'] for each in buy_info]), 4)
        if pay <= 0:
            logger.warning(f'NO REPORT for User {self.account_id}')
            return

        income = round(sum([each['vol'] - each['fee'] for each in sell_info]), 4)
        profit = round(income - pay, 4)
        percent = round(profit / self.usdt_balance * 100, 4)

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
        wx_report(self.account_id, self.wxuid, self.username, pay, income, profit, percent, buy_info, sell_info, total_profit, month_profit)