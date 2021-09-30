# from huobi.client.account import AccountClient
# from huobi.client.trade import TradeClient
# from huobi.constant import OrderSource, OrderType, AccountBalanceMode
# from huobi.model.account.account_update_event import AccountUpdateEvent, AccountUpdate
# from huobi.model.trade.order_update_event import OrderUpdateEvent, OrderUpdate
from binance.spot import Spot
from binance.websocket.spot.websocket_client import SpotWebsocketClient
from retry import retry
from utils import logger
from target import BaseTarget as Target
from order import OrderSummary
from user import BaseUser

import time
# AccountBalanceMode.TOTAL = '2'

class ListenKey:
    def __init__(self, api: Spot):
        self.api = api
        self.key = self.api.new_listen_key()['listenKey']
        self.update_time = time.time()
        self.create_time = self.update_time

    def update(self):
        self.key = self.api.renew_listen_key(self.key)
        self.update_time = time.time()

    def recreate(self):
        self.api.close_listen_key(self.key)
        self.key = self.api.new_listen_key()['listenKey']
        self.update_time = time.time()
        self.create_time = self.update_time

    def check(self):
        now = time.time()
        if now > self.create_time + 12 * 60 * 60:
            self.recreate()
        elif now > self.update_time + 30 * 60:
            self.update()

class OrderUpdate:
    def __init__(self, update):
        self.orderId = 0
        self.tradePrice = ""
        self.tradeVolume = ""
        self.tradeId = 0
        self.tradeTime = 0
        self.aggressor = False
        self.remainAmt = ""
        self.orderStatus = 0
        self.clientOrderId = ""
        self.eventType = ""
        self.symbol = ""
        self.type = 0
        self.accountId = 0

class BinanceUser(BaseUser):
    user_type = 'Binance'

    def __init__(self, access_key, secret_key, buy_amount, wxuid):
        self.api = Spot(key=access_key, secret=secret_key)
        super().__init__(access_key, secret_key, buy_amount, wxuid)
        self.listen_key = ListenKey(self.api)
        self.websocket = SpotWebsocketClient()

    def get_account_id(self) -> int:
        return 0

    def get_order(self, order_id):
        symbol = self.orders[order_id].symbol
        return self.api.get_order(symbol, orderId=order_id)

    def cancel_order(self, order_id):
        symbol = self.orders[order_id].symbol
        self.api.cancel_order(symbol, orderId=order_id)

    def sub_balance_update(self, **kwargs):
        self.listen_key.check()
        self.websocket.user_data(self.listen_key.key, 1, self.user_data_callback)


    def user_data_callback(self, update):
        print(update)
        if update['e'] == 'outboundAccountPosition':
            self.balance_callback(update)
        elif update['e'] == 'executionReport':
            self.trade_callback(update)

    def balance_callback(self, update):
        change_time = update['E']

        for sub_update in update['B']:
            currency = sub_update['a']
            if change_time > self.balance_update_time[currency]:
                self.available_balance[currency] = float(sub_update['f'])
                self.balance[currency] = float(sub_update['f'])+float(sub_update['l'])
                self.balance_update_time[update.currency] = change_time

    def trade_callback(self, update):
        @retry(tries=3, delay=0.01)
        def _warpper(update):
            symbol = update['s']
            direction = update['S']
            etype = update['x']
            now_etype = update['X']
            order_id = update['i']

            if order_id in self.orders:
                summary = self.orders[order_id]
            else:
                summary = OrderSummary(order_id, symbol, direction)
                self.orders[order_id] = summary

            try:
                if etype == 'New':
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
            update['from'] = 'binance'
            _warpper(update)
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
