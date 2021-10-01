import time
import math
import re

from utils import logger, timeout_handle
from huobi.client.account import AccountClient
from huobi.client.trade import TradeClient
from huobi.constant import OrderSource, OrderType, AccountBalanceMode
from huobi.model.account.account_update_event import AccountUpdateEvent, AccountUpdate
from huobi.model.trade.order_update_event import OrderUpdateEvent, OrderUpdate
from huobi.client.generic import GenericClient
from huobi.client.market import MarketClient 
from huobi.model.generic.symbol import Symbol
from retry import retry
from target import BaseTarget as Target
from order import OrderSummary
from user import BaseUser, BaseMarketClient
from websocket_handler import replace_watch_dog

AccountBalanceMode.TOTAL = '2'


class HuobiMarketClient(MarketClient, BaseMarketClient):
    exclude_list = [
        'htusdt', 'btcusdt', 'bsvusdt', 'bchusdt', 'etcusdt',
        'ethusdt', 'botusdt','mcousdt','lendusdt','venusdt',
        'yamv2usdt', 'bttusdt', 'dogeusdt', 'shibusdt',
        'filusdt', 'xrpusdt', 'trxusdt', 'nftusdt',
        'thetausdt', 'dotusdt', 'eosusdt', 'maticusdt',
        'linkusdt', 'adausdt', 'jstusdt', 'vetusdt', 'xmxusdt',
        'newusdt', 'uipusdt', 'smtusdt'
    ]
    min_usdt_amount = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.generic_client = GenericClient()
        self.all_symbol_info: 'dict[str, Symbol]' = {}
        self.symbols_info: 'dict[str, Symbol]' = {}
        self.mark_price: 'dict[str, float]' = {}
        self.update_symbols_info()

    def get_all_symbols_info(self):
        return {
            info.symbol: info
            for info in self.generic_client.get_exchange_symbols()
            if info.symbol.endswith('usdt')
            and not re.search('\d', info.symbol)
            and info.symbol not in [
                'bchausdt', 'mcousdt', 'borusdt',
                'venusdt', 'botusdt', 'lendusdt'
            ]
        }

    def get_candlestick(self, symbol, period, size=200):
        if period == '1hour':
            period = '60min'
        return super().get_candlestick(symbol, period, size=size)

    def get_market_tickers(self, **kwargs) -> list:
        return super().get_market_tickers()



class HuobiUser(BaseUser):
    user_type = 'Huobi'
    MarketClient = HuobiMarketClient

    def __init__(self, access_key, secret_key, buy_amount, wxuid):
        self.watch_dog = replace_watch_dog()
        self.account_client = AccountClient(api_key=access_key, secret_key=secret_key)
        self.trade_client = TradeClient(api_key=access_key, secret_key=secret_key)
        super().__init__(access_key, secret_key, buy_amount, wxuid)
        self.scheduler = self.watch_dog.scheduler

    def start(self, **kwargs):
        self.account_client.sub_account_update(
            AccountBalanceMode.TOTAL,
            self.balance_callback,
            self.error_callback('balance')
        )
        self.watch_dog.after_connection_created('balance')

        self.trade_client.sub_order_update(
            '*', 
            self.trade_callback,
            self.error_callback('order')
        )
        self.watch_dog.after_connection_created('order')

        while 'usdt' not in self.balance:
            time.sleep(0.1)

        usdt = self.balance['usdt']
        if isinstance(self.buy_amount, str) and self.buy_amount.startswith('/'):
            self.buy_amount = max(math.floor(usdt / float(self.buy_amount[1:])), 5)
        else:
            self.buy_amount = float(self.buy_amount)

    def get_account_id(self) -> int:
        return next(filter(
            lambda account: account.type=='spot' and account.state =='working',
            self.account_client.get_accounts()
        )).id

    def get_order(self, order_id):
        detail = self.trade_client.get_order(order_id)
        detail.filled_amount = float(detail.filled_amount)
        detail.filled_cash_amount = float(detail.filled_cash_amount)
        detail.price = float(detail.price)
        detail.amount = float(detail.amount)
        return detail

    def cancel_order(self, order_id):
        symbol = self.orders[order_id].symbol
        self.trade_client.cancel_order(symbol, order_id)

    def balance_callback(self, event: AccountUpdateEvent):
        update: AccountUpdate = event.data

        if not update.changeTime:
            update.changeTime = 0

        if (update.currency not in self.balance_update_time
            or update.changeTime > self.balance_update_time[update.currency]
        ):
            self.balance[update.currency] = float(update.balance)
            self.available_balance[update.currency] = float(update.available)
            self.balance_update_time[update.currency] = int(update.changeTime) / 1000

    def trade_callback(self, event: OrderUpdateEvent):
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

                elif etype == 'cancellation':
                    summary.cancel_update(update)

            except Exception as e:
                if not isinstance(e, KeyError):
                    logger.error(f"{direction} {etype} | Error: {type(e)} {e}")
                raise e

        try:
            _warpper(event)
        except Exception as e:
            if not isinstance(e, KeyError):
                logger.error(f"max tries | {type(e)} {e}")

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
            logger.error(e)
            raise Exception(e)
