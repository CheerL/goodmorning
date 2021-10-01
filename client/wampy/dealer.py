import time

from threading import Timer
from retry import retry
from market import MarketClient
from target import MorningTarget as Target
from utils import config, logger, user_config
from utils.datetime import ts2time
from wampy.roles.subscriber import subscribe
from client.wampy import ControlledClient, Topic, State, WS_URL
from client import BaseDealerClient
from user import BaseUser as User
from order import OrderSummaryStatus
from report import wx_report, add_profit, get_profit


STOP_PROFIT_SLEEP = config.getfloat('time', 'STOP_PROFIT_SLEEP')
REPORT_PRICE = user_config.getboolean('setting', 'REPORT_PRICE')
STOP_PROFIT_RATE_HIGH = config.getfloat('sell', 'STOP_PROFIT_RATE_HIGH')
STOP_PROFIT_RATE_LOW = config.getfloat('sell', 'STOP_PROFIT_RATE_LOW')
ALL_STOP_PROFIT_RATE = config.getfloat('sell', 'ALL_STOP_PROFIT_RATE')
IOC_RATE = config.getfloat('sell', 'IOC_RATE')
IOC_BATCH_NUM = config.getint('sell', 'IOC_BATCH_NUM')
HIGH_STOP_PROFIT_HOLD_TIME = config.getfloat('time', 'HIGH_STOP_PROFIT_HOLD_TIME')

class MorningDealerClient(ControlledClient, BaseDealerClient):
    def __init__(self, user: User, url=WS_URL):
        super().__init__(url=url)
        self.market_client = user.market_client
        self.targets = {}
        self.user = user
        self.client_type = 'dealer'
        self.high_stop_profit = True
        self.not_buy = False

    @classmethod
    @retry(tries=5, delay=1, logger=logger)
    def init_dealer(cls, user):
        client = super().init_dealer(user)
        client.start()
        return client

    def buy_and_sell(self, target: Target, client):
        @retry(tries=5, delay=0.05)
        def callback(summary):
            client.after_buy(target.symbol, summary.aver_price)
            if summary.aver_price <=0:
                logger.error(f'Fail to buy {target.symbol}')
                return

            amount = min(self.user.get_amount(target.base_currency), summary.amount * 0.998)
            assert amount - 0.9 * summary.amount > 0, "Not yet arrived"
            self.user.sell_limit(target, amount, target.stop_profit_price)
            Timer(HIGH_STOP_PROFIT_HOLD_TIME, self.turn_low_cancel_and_sell, [target, None]).start()

        def buy_callback():
            summary = self.user.buy(target, self.user.buy_amount)
            if summary != None:
                summary.check_after_buy(client)
                summary.add_filled_callback(callback, [summary])
                summary.add_cancel_callback(callback, [summary])
            else:
                client.after_buy(target.symbol, 0)

        Timer(0, buy_callback).start()

    def buy_limit_and_sell(self, target: Target, client):
        @retry(tries=5, delay=0.05)
        def callback(summary):
            client.after_buy(target.symbol, summary.aver_price)
            if summary.aver_price <=0:
                logger.error(f'Fail to buy {target.symbol}')
                return

            amount = min(self.user.get_amount(target.base_currency), summary.amount * 0.998)
            assert amount - 0.9 * summary.amount > 0, "Not yet arrived"
            self.user.sell_limit(target, amount, target.stop_profit_price)
            Timer(HIGH_STOP_PROFIT_HOLD_TIME, self.turn_low_cancel_and_sell, [target, None]).start()

        def buy_callback():
            buy_price = target.get_target_buy_price()
            summary = self.user.buy_limit(target, float(self.user.buy_amount), buy_price)
            if summary != None:
                summary.check_after_buy(client)
                summary.add_filled_callback(callback, [summary])
                summary.add_cancel_callback(callback, [summary])
            else:
                client.after_buy(target.symbol, 0)

        Timer(0, buy_callback).start()

    def after_buy(self, symbol, price):
        if self.targets[symbol].buy_price:
            return

        if REPORT_PRICE:
            self.publish(topic=Topic.AFTER_BUY, symbol=symbol, price=price)

        if price == 0:
            del self.targets[symbol]
        else:
            self.targets[symbol].set_buy_price(price)

    def cancel_and_sell(self, target: Target, callback=None, market=True):
        @retry(tries=5, delay=0.05)
        def _callback(summary=None):
            if summary:
                amount = min(self.user.get_amount(target.base_currency), summary.remain)
                assert amount - 0.9 * summary.remain > 0, "Not yet arrived"
            else:
                amount = self.user.get_amount(target.base_currency)

            if market:
                self.user.sell(target, amount)
            else:
                self.user.sell_limit(target, amount, target.stop_profit_price)

        symbol = target.symbol
        callback = callback if callback else _callback
        is_canceled = False
        
        order_id_list = list(self.user.orders.keys())
        for order_id in order_id_list:
            summary = self.user.orders[order_id]
            if (summary.symbol == target.symbol and summary.order_id in self.user.sell_id
                and summary.status in [OrderSummaryStatus.PARTIAL_FILLED, OrderSummaryStatus.CREATED]
            ):
                try:
                    self.user.cancel_order(summary.order_id)
                    summary.add_cancel_callback(callback, [summary])
                    logger.info(f'Cancel open sell order for {symbol}')
                    is_canceled = True
                except Exception as e:
                    logger.error(f'{summary.order_id} {summary.status} {summary.symbol} {e}')

        if not is_canceled:
            try:
                callback()
            except Exception as e:
                logger.error(e)

    def check_and_sell(self, limit=True):
        @retry(tries=5, delay=0.05)
        def sell(target: Target, amount, limit=True):
            if limit and amount > target.limit_order_min_order_amt:
                self.user.sell_limit(target, amount, target.stop_profit_price)
            elif not limit and amount > target.sell_market_min_order_amt:
                self.user.sell(target, amount)

        for target in list(self.targets.values()):
            buy_amount = sum([
                summary.amount for summary
                in self.user.orders.values()
                if summary.order_id in self.user.buy_id
                and summary.symbol == target.symbol
            ])
            sell_amount = sum([
                summary.amount for summary
                in self.user.orders.values()
                if summary.order_id in self.user.sell_id
                and summary.symbol == target.symbol
            ])
            remain = 0.998 * buy_amount - sell_amount
            logger.info(f'{target.symbol} buy {buy_amount} sell {sell_amount} left {remain}')
            try:
                if (remain / buy_amount) > 0.01:
                    sell(target, remain, limit)
            except:
                pass

    def sell_in_buy_price(self):
        def callback_generator(target):
            @retry(tries=5, delay=0.05)
            def callback(summary=None):
                if summary:
                    amount = min(self.user.get_amount(target.base_currency), summary.remain)
                    assert amount - 0.9 * summary.remain > 0, "Not yet arrived"
                else:
                    amount = self.user.get_amount(target.base_currency)

                self.sell_limit(target, amount, price=target.buy_price)
            return callback

        for target in list(self.targets.values()):
            self.cancel_and_sell(target, callback_generator(target), market=False)

    def cancel_and_sell_ioc(self, target: Target, price: float, count: int):
        @retry(tries=5, delay=0.05)
        def callback(summary=None):
            if summary:
                amount = min(self.user.get_amount(target.base_currency), summary.remain)
                assert amount - 0.9 * summary.remain > 0, "Not yet arrived"
            else:
                amount = self.user.get_amount(target.base_currency)

            sell_price = price * (1 - IOC_RATE / 100)
            sell_rate = 1 / (IOC_BATCH_NUM - count) if count > 0 else 1 / (IOC_BATCH_NUM - 1)
            sell_amount = amount * sell_rate
            logger.info(f'Try to ioc sell {sell_amount} {target.symbol} with price {sell_price}')
            self.user.sell_limit(target, sell_amount, sell_price, ioc=True)

        if count < IOC_BATCH_NUM - 1:
            self.cancel_and_sell(target, callback, False)
        else:
            self.cancel_and_sell(target, market=True)

    def high_cancel_and_sell(self, symbol, price):
        @retry(tries=5, delay=0.05)
        def _callback(summary=None):
            if summary:
                amount = min(self.user.get_amount(target.base_currency), summary.remain)
                assert amount - 0.9 * summary.remain > 0, "Not yet arrived"
            else:
                amount = self.user.get_amount(target.base_currency)

            self.user.sell_limit(target, amount, (price + 2 * target.buy_price) / 3)

        for target in list(self.targets.values()):
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

    def check_all_stop_profit(self):
        while self.state == State.RUNNING:
            time.sleep(0.1)
            try:
                asset = self.user.get_asset()
                if asset > self.user.all_stop_profit_asset:
                    self.state_handler(State.STARTED)
                    logger.info(f'Now asset {asset}U, start asset {self.user.start_asset}U, stop profit')
                    break
            except:
                pass

    # handler
    @subscribe(topic=Topic.BUY_SIGNAL)
    def buy_signal_handler(self, symbol, price, init_price, now, *args, **kwargs):
        if self.state != State.RUNNING or symbol in self.targets:
            return

        if self.not_buy:
            logger.info(f'Fail to buy {symbol}, already stop buy')
            return

        receive_time = time.time()
        self.market_client.symbols_info[symbol].init_price = init_price
        target = Target(symbol, price, now, self.high_stop_profit)
        target.set_info(self.market_client.symbols_info[symbol])
        self.targets[symbol] = target

        self.user.buy_limit_and_sell(target, self)
        logger.info(f'Buy. {symbol}, recieved at {receive_time}, sent at {now}, price {price}')

    @subscribe(topic=Topic.STOP_LOSS)
    def stop_loss_handler(self, symbol, price, *args, **kwargs):
        if self.state != State.RUNNING or symbol not in self.targets:
            return

        target = self.targets[symbol]
        self.user.cancel_and_sell(target)
        logger.info(f'Stop loss. {symbol}: {price}USDT')

    @subscribe(topic=Topic.STOP_PROFIT)
    def stop_profit_handler(self, symbol, price, *args, **kwargs):
        if self.state != State.RUNNING or not self.high_stop_profit:
            return

        def high_cancel_and_sell():
            self.user.high_cancel_and_sell(list(self.targets.values()), symbol, price)

        self.high_stop_profit = False
        if symbol:
            self.not_buy = True

        Timer(STOP_PROFIT_SLEEP, high_cancel_and_sell).start()
        logger.info(f'Stop profit. {symbol}: {price}USDT')

    @subscribe(topic=Topic.CLEAR)
    def clear_handler(self, data, count, *arg, **kwargs):
        if self.state != State.RUNNING:
            return

        logger.info(f'Start ioc clear for round {count+1}')
        for symbol, price in data:
            if symbol not in self.targets:
                continue
            
            target = self.targets[symbol]
            self.user.cancel_and_sell_ioc(target, price, count)

    @retry(tries=5, delay=0.1)
    def report(self):
        orders = []
        for order_id in set(self.user.buy_id + self.user.sell_id):
            try:
                order = self.user.get_order(order_id)
                orders.append(order)
            except Exception as e:
                logger.error(e)

        order_info = [{
            'symbol': order.symbol,
            'time': ts2time(order.finished_at / 1000, '%Y-%m-%d %H:%M:%S.%f'),
            'price': round(float(order.filled_cash_amount) / float(order.filled_amount), 6),
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