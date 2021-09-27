import math
import time

import huobi
from huobi.client.account import AccountClient
from huobi.client.trade import TradeClient
from huobi.constant import OrderSource, OrderSide, OrderType, AccountBalanceMode
from huobi.model.account.account_update_event import AccountUpdateEvent, AccountUpdate
from huobi.model.trade.order_update_event import OrderUpdateEvent, OrderUpdate
from huobi.model.market.candlestick import Candlestick
from retry.api import retry
from threading import Timer

from utils import config, logger, strftime, timeout_handle, user_config
from utils.datetime import ts2date
from report import wx_report, add_profit, get_profit, wx_name
from target import Target, LossTarget
from order import OrderSummary, OrderSummaryStatus
from dataset.pgsql import Order as OrderSQL, LossTarget as LossTargetSQL

STOP_PROFIT_RATE_HIGH = config.getfloat('sell', 'STOP_PROFIT_RATE_HIGH')
STOP_PROFIT_RATE_LOW = config.getfloat('sell', 'STOP_PROFIT_RATE_LOW')
ALL_STOP_PROFIT_RATE = config.getfloat('sell', 'ALL_STOP_PROFIT_RATE')
IOC_RATE = config.getfloat('sell', 'IOC_RATE')
IOC_BATCH_NUM = config.getint('sell', 'IOC_BATCH_NUM')
HIGH_STOP_PROFIT_HOLD_TIME = config.getfloat('time', 'HIGH_STOP_PROFIT_HOLD_TIME')
MIN_NUM = config.getint('loss', 'MIN_NUM')
MAX_NUM = config.getint('loss', 'MAX_NUM')
MAX_DAY = config.getint('loss', 'MAX_DAY')
TEST = user_config.getboolean('setting', 'TEST')

AccountBalanceMode.TOTAL = '2'

class BaseUser:
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
        self.available_balance: dict[str, float] = {}
        self.balance_update_time: dict[str, float] = {}
        self.orders: dict[int, OrderSummary] = {}
        self.wxuid = wxuid.split(';')

        self.buy_id = []
        self.sell_id = []
        self.username = wx_name(self.wxuid[0])
        self.buy_amount = buy_amount

    @property
    def asset(self):
        asset = float(self.account_client.get_account_asset_valuation('spot', 'USD').balance)
        return asset

    @classmethod
    @retry(tries=5, delay=1, logger=logger)
    def init_users(cls, num=-1) -> 'list[BaseUser]':
        ACCESSKEY = user_config.get('setting', 'AccessKey')
        SECRETKEY = user_config.get('setting', 'SecretKey')
        WXUIDS = user_config.get('setting', 'WxUid')
        BUY_AMOUNT = user_config.get('setting', 'BuyAmount')
        TEST = user_config.getboolean('setting', 'Test')

        access_keys = [key.strip() for key in ACCESSKEY.split(',')]
        secret_keys = [key.strip() for key in SECRETKEY.split(',')]
        wxuids = [uid.strip() for uid in WXUIDS.split(',')]
        buy_amounts = [amount.strip() for amount in BUY_AMOUNT.split(',')]

        if num == -1:
            users = [cls(*user_data) for user_data in zip(access_keys, secret_keys, buy_amounts, wxuids)]
        else:
            users = [cls(access_keys[num], secret_keys[num], buy_amounts[num], wxuids[num])]
                                                    
        if TEST:
            users = users[:1]
        return users

    def start(self, watch_dog=None):
        self.account_client.sub_account_update(
            AccountBalanceMode.TOTAL,
            self.balance_callback,
            self.error_callback('balance')
        )
        if watch_dog:
            watch_dog.after_connection_created('balance')

        self.trade_client.sub_order_update(
            '*', 
            self.trade_callback(),
            self.error_callback('order')
        )
        if watch_dog:
            watch_dog.after_connection_created('order')

        while 'usdt' not in self.balance:
            time.sleep(0.1)

        self.usdt_balance = self.balance['usdt']
        if isinstance(self.buy_amount, str) and self.buy_amount.startswith('/'):
            self.buy_amount = max(math.floor(self.usdt_balance / float(self.buy_amount[1:])), 5)
        else:
            self.buy_amount = float(self.buy_amount)

    def balance_callback(self, event: AccountUpdateEvent):
        update: AccountUpdate = event.data
        # if float(update.balance) - float(update.available) > 1e-8:
        #     return

        if not update.changeTime:
            update.changeTime = 0

        if (update.currency not in self.balance_update_time
            or update.changeTime > self.balance_update_time[update.currency]
        ):
            self.balance[update.currency] = float(update.balance)
            self.available_balance[update.currency] = float(update.available)
            self.balance_update_time[update.currency] = int(update.changeTime) / 1000

    def trade_callback(self):
        def warpper(event: OrderUpdateEvent):
            @retry(tries=3, delay=0.01)
            def _warpper(event: OrderUpdateEvent):
                update: OrderUpdate = event.data
                symbol = update.symbol
                direction = 'buy' if 'buy' in update.type else 'sell'
                etype = update.eventType
                order_id = update.orderId

                if order_id in self.orders:
                    summary = self.orders[order_id]
                else:
                    summary = OrderSummary(order_id, symbol, direction)
                    self.orders[order_id] = summary

                try:
                    if etype == 'creation':
                        summary.create(update)

                    elif etype == 'trade':
                        summary.update(update)
                        if update.orderStatus == 'filled' and summary.filled_callback:
                            summary.filled_callback(*summary.filled_callback_args)

                    elif etype == 'cancellation':
                        summary.cancel_update(update)
                        if summary.cancel_callback:
                            summary.cancel_callback(*summary.cancel_callback_args)

                except Exception as e:
                    if not isinstance(e, KeyError):
                        logger.error(f"{direction} {etype} | Error: {type(e)} {e}")
                    raise e

            try:
                _warpper(event)
            except Exception as e:
                if not isinstance(e, KeyError):
                    logger.error(f"max tries | {type(e)} {e}")

        return warpper

    def error_callback(self, prefix):
        def warpper(error):
            logger.error(f'[{prefix}] {error}')
        return warpper

    def buy(self, target: Target, vol):
        symbol = target.symbol
        vol = target.check_amount(max(
            vol,
            target.min_order_value
        ))
        order = dict(
            symbol=symbol,
            account_id=self.account_id,
            order_type=OrderType.BUY_MARKET,
            amount=vol,
            price=1,
            source=OrderSource.SPOT_API
        )
        # order_id = -1

        try:
            order_id = self.trade_client.create_order(**order)
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'buy')
                self.orders[order_id] = order_summary

            order_summary.created_vol = vol
            order_summary.remain = vol
            self.buy_id.append(order_id)
            
            logger.debug(f'Speed {vol} USDT to buy {target.symbol[:-4]}')
            return order_summary
        except Exception as e:
            logger.error(e)
            # if order_id in self.orders:
            #     del self.orders[order_id]
            return None

    def buy_limit(self, target: Target, vol, price=None):
        if not price:
            price = target.get_target_buy_price()
        else:
            price = target.check_price(price)
        symbol = target.symbol
        amount = target.check_amount(max(
            vol / price,
            target.limit_order_min_order_amt
        ))
        order = dict(
            symbol=symbol,
            account_id=self.account_id,
            order_type=OrderType.BUY_LIMIT,
            amount=amount,
            price=price,
            source=OrderSource.API
        )
        # order_id = -1

        try:
            order_id = self.trade_client.create_order(**order)
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'buy')
                self.orders[order_id] = order_summary

            order_summary.created_amount = amount
            order_summary.created_price = price
            order_summary.remain = amount
            self.buy_id.append(order_id)
            
            logger.debug(f'Buy {vol} {symbol[:-4]}')
            return order_summary
        except Exception as e:
            logger.error(e)
            # if order_id in self.orders:
            #     del self.orders[order_id]
            return None

    def sell(self, target: Target, amount):
        symbol = target.symbol
        amount = target.check_amount(max(
            amount,
            target.sell_market_min_order_amt
        ))
        order = dict(
            symbol=symbol,
            account_id=self.account_id,
            order_type=OrderType.SELL_MARKET,
            amount=amount,
            price=1,
            source=OrderSource.SPOT_API
        )

        try:
            order_id = self.trade_client.create_order(**order)
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'sell')
                self.orders[order_id] = order_summary

            order_summary.created_amount = amount
            order_summary.remain = amount
            self.sell_id.append(order_id)
            
            logger.debug(f'Sell {amount} {symbol[:-4]} with market price')
            return order_summary
        except Exception as e:
            # order_summary.error(e)
            logger.error(e)
            raise Exception(e)

    def sell_limit(self, target: Target, amount, price, ioc=False):
        price = target.check_price(price)
        symbol = target.symbol
        amount = target.check_amount(max(
            amount,
            target.limit_order_min_order_amt
        ))
        order = dict(
            symbol=symbol,
            account_id=self.account_id,
            order_type=OrderType.SELL_LIMIT if not ioc else OrderType.SELL_IOC,
            amount=amount,
            price=price,
            source=OrderSource.SPOT_API
        )

        try:
            order_id = self.trade_client.create_order(**order)
            if order_id in self.orders:
                order_summary = self.orders[order_id]
            else:
                order_summary = OrderSummary(order_id, symbol, 'sell')
                self.orders[order_id] = order_summary

            order_summary.created_amount = amount
            order_summary.created_price = price
            order_summary.remain = amount
            self.sell_id.append(order_id)
            logger.debug(f'Sell {amount} {symbol[:-4]} with price {price}')
            return order_summary
        except Exception as e:
            # order_summary.error(e)
            logger.error(e)
            raise Exception(e)

    @timeout_handle([])
    def get_open_orders(self, targets, side=OrderSide.SELL) -> 'list[huobi.model.trade.order.Order]':
        open_orders = []
        all_symbols = [target.symbol for target in targets]
        for symbols in [all_symbols[i:i+10] for i in range(0, len(all_symbols), 10)]:
            open_orders.extend(self.trade_client.get_open_orders(','.join(symbols), self.account_id, side))
        return open_orders

    @retry(tries=5, delay=0.05, logger=logger)
    def get_amount(self, currency, available=False, check=True):
        if available:
            return self.available_balance[currency]
        if check:
            assert self.balance[currency] - self.available_balance[currency] < 1e-8, 'unavailable'
        return self.balance[currency]

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

class User(BaseUser):
    def check_and_sell(self, targets: 'list[Target]', limit=True):
        @retry(tries=5, delay=0.05)
        def _sell(target: Target, amount, limit=True):
            if limit and amount > target.limit_order_min_order_amt:
                self.sell_limit(target, amount, target.stop_profit_price)
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
            remain = 0.998 * buy_amount - sell_amount
            logger.info(f'{target.symbol} buy {buy_amount} sell {sell_amount} left {remain}')
            try:
                if (remain / buy_amount) > 0.01:
                    _sell(target, remain, limit)
            except:
                pass

    def cancel_and_sell_in_buy_price(self, targets: 'list[Target]'):
        def callback_generator(target):
            @retry(tries=5, delay=0.05)
            def callback(summary=None):
                if summary:
                    amount = min(self.get_amount(target.base_currency), summary.remain)
                    assert amount - 0.9 * summary.remain > 0, "Not yet arrived"
                else:
                    amount = self.get_amount(target.base_currency)

                self.sell_limit(target, amount, price=target.buy_price)
            return callback

        for target in list(targets):
            self.cancel_and_sell(target, callback_generator(target), market=False)

    def cancel_and_sell_ioc(self, target: Target, price: float, count: int):
        @retry(tries=5, delay=0.05)
        def callback(summary=None):
            if summary:
                amount = min(self.get_amount(target.base_currency), summary.remain)
                assert amount - 0.9 * summary.remain > 0, "Not yet arrived"
            else:
                amount = self.get_amount(target.base_currency)

            sell_price = price * (1 - IOC_RATE / 100)
            sell_rate = 1 / (IOC_BATCH_NUM - count) if count > 0 else 1 / (IOC_BATCH_NUM - 1)
            sell_amount = amount * sell_rate
            logger.info(f'Try to ioc sell {sell_amount} {target.symbol} with price {sell_price}')
            self.sell_limit(target, sell_amount, sell_price, ioc=True)

        if count < IOC_BATCH_NUM - 1:
            self.cancel_and_sell(target, callback, False)
        else:
            self.cancel_and_sell(target, market=True)

    def cancel_and_sell(self, target: Target, callback=None, market=True):
        @retry(tries=5, delay=0.05)
        def _callback(summary=None):
            if summary:
                amount = min(self.get_amount(target.base_currency), summary.remain)
                assert amount - 0.9 * summary.remain > 0, "Not yet arrived"
            else:
                amount = self.get_amount(target.base_currency)

            if market:
                self.sell(target, amount)
            else:
                self.sell_limit(target, amount, target.stop_profit_price)

        symbol = target.symbol
        callback = callback if callback else _callback
        is_canceled = False
        
        if symbol in self.orders['sell']:
            for summary in self.orders['sell'][symbol]:
                if summary.status in [OrderSummaryStatus.PARTIAL_FILLED, OrderSummaryStatus.CREATED]:
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
        @retry(tries=5, delay=0.05)
        def _callback(summary=None):
            if summary:
                amount = min(self.get_amount(target.base_currency), summary.remain)
                assert amount - 0.9 * summary.remain > 0, "Not yet arrived"
            else:
                amount = self.get_amount(target.base_currency)

            self.sell_limit(target, amount, (price + 2 * target.buy_price) / 3)

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

    def buy_and_sell(self, target: Target, client):
        @retry(tries=5, delay=0.05)
        def callback(summary):
            client.after_buy(target.symbol, summary.aver_price)
            if summary.aver_price <=0:
                logger.error(f'Fail to buy {target.symbol}')
                return

            amount = min(self.get_amount(target.base_currency), summary.amount * 0.998)
            assert amount - 0.9 * summary.amount > 0, "Not yet arrived"
            self.sell_limit(target, amount, target.stop_profit_price)
            Timer(HIGH_STOP_PROFIT_HOLD_TIME, self.turn_low_cancel_and_sell, [target, None]).start()

        def buy_callback():
            summary = self.buy(target, self.buy_amount)
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

            amount = min(self.get_amount(target.base_currency), summary.amount * 0.998)
            assert amount - 0.9 * summary.amount > 0, "Not yet arrived"
            self.sell_limit(target, amount, target.stop_profit_price)
            Timer(HIGH_STOP_PROFIT_HOLD_TIME, self.turn_low_cancel_and_sell, [target, None]).start()

        def buy_callback():
            buy_price = target.get_target_buy_price()
            summary = self.buy_limit(target, float(self.buy_amount), buy_price)
            if summary != None:
                summary.check_after_buy(client)
                summary.add_filled_callback(callback, [summary])
                summary.add_cancel_callback(callback, [summary])
            else:
                client.after_buy(target.symbol, 0)

        Timer(0, buy_callback).start()


class LossUser(BaseUser):
    def filter_targets(self, targets):
        targets_num = len(targets)
        usdt_amount = self.get_amount('usdt', available=True, check=False)
        buy_num = max(min(targets_num, MAX_NUM), MIN_NUM)
        buy_amount = usdt_amount // buy_num
        if buy_amount < 6:
            buy_amount = 6
            buy_num = int(usdt_amount // buy_amount)

        if not TEST:
            self.buy_amount = buy_amount
        targets = {
            target.symbol: target for target in
            sorted(targets.values(), key=lambda x: -x.vol)[:buy_num]
        }
        return targets
