import time

from threading import Timer
from retry import retry
from market import MarketClient
from target import Target, LossTarget
from utils import config, logger, user_config, get_rate
from utils.parallel import run_thread_pool
from utils.datetime import date2ts, ts2date
from order import OrderSummary, OrderSummaryStatus

from wampy.roles.subscriber import subscribe
from client import ControlledClient, Topic, State, WS_URL
from user import User, LossUser
from dataset.pgsql import Order as OrderSQL, LossTarget as LossTargetSQL

STOP_PROFIT_SLEEP = config.getfloat('time', 'STOP_PROFIT_SLEEP')
REPORT_PRICE = user_config.getboolean('setting', 'REPORT_PRICE')
MIN_LOSS_RATE = config.getfloat('loss', 'MIN_LOSS_RATE')
BREAK_LOSS_RATE = config.getfloat('loss', 'BREAK_LOSS_RATE')
AVER_LENGTH = config.getfloat('loss', 'AVER_LENGTH')
BUY_UP_RATE = config.getfloat('loss', 'BUY_UP_RATE')
SELL_UP_RATE = config.getfloat('loss', 'SELL_UP_RATE')
MAX_DAY = config.getint('loss', 'MAX_DAY')


class BaseDealerClient(ControlledClient):
    def __init__(self, market_client: MarketClient, user: User, url=WS_URL):
        super().__init__(url=url)
        self.market_client : MarketClient = market_client
        self.targets = {}
        self.user = user
        self.client_type = 'base_dealer'

    @classmethod
    @retry(tries=5, delay=1, logger=logger)
    def init_dealer(cls, user):
        market_client = MarketClient()
        client = cls(market_client, user)
        client.start()
        return client


class DealerClient(BaseDealerClient):
    def __init__(self, market_client: MarketClient, user: User, url=WS_URL):
        super().__init__(market_client=market_client, user=user, url=url)
        self.client_type = 'dealer'
        self.high_stop_profit = True
        self.not_buy = False


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

    def after_buy(self, symbol, price):
        if self.targets[symbol].buy_price:
            return

        if REPORT_PRICE:
            self.publish(topic=Topic.AFTER_BUY, symbol=symbol, price=price)

        if price == 0:
            del self.targets[symbol]
        else:
            self.targets[symbol].set_buy_price(price)

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

    def sell_in_buy_price(self):
        self.user.cancel_and_sell_in_buy_price(self.targets.values())

    def check_and_sell(self, limit=True):
        self.user.check_and_sell(self.targets.values(), limit)

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


class LossDealerClient(BaseDealerClient):
    def __init__(self, market_client: MarketClient, user: LossUser, url=WS_URL):
        super().__init__(market_client=market_client, user=None, url=url)
        self.user: LossUser = user
        self.targets: dict[str, dict[str, LossTarget]] = {}
        self.date = ts2date()
        self.client_type = 'loss_dealer'

    def resume(self):
        now = time.time()
        start_date = ts2date(now - MAX_DAY * 86400)
        targets: list[LossTargetSQL] = LossTargetSQL.get_targets([
            LossTargetSQL.date >= start_date
        ])

        # target_dict: dict[str, dict[str, LossTarget]] = {}
        for target in targets:
            date_target_dict = self.targets.setdefault(target.date, {})
            date_target_dict[target.symbol] = LossTarget(
                target.symbol, target.date, target.open, target.close, target.vol
            )
        


        orders: list[OrderSQL] = OrderSQL.get_orders([
            OrderSQL.account==str(self.user.account_id),
            OrderSQL.date>=start_date
        ])

        for order in orders:
            order_id = int(order.order_id)
            symbol = order.symbol
            try:
                detail = self.user.trade_client.get_order(order_id)
            except:
                continue

            summary = OrderSummary(order_id, symbol, order.direction, order.date)
            summary.status = {
                'created': 1,
                'partial-filled': 2,
                'filled': 3,
                'canceled': 4
            }[detail.state]
            summary.amount = float(detail.filled_amount)
            summary.vol = float(detail.filled_cash_amount)
            summary.aver_price = summary.vol / summary.amount
            summary.fee = summary.vol * 0.002
            summary.label = order.date

            if 'market' in detail.type:
                summary.limit = False
                summary.create_price = 0
            else:
                summary.create_price = float(detail.price)

            if detail.type in ['buy-market']:
                summary.create_amount = float(detail.amount)
                summary.remain = summary.created_amount - summary.amount
                if summary.remain / summary.created_amount < 1e-6:
                    summary.remain = 0
                
            elif detail.type in ['buy-limit', 'sell-limit']:
                summary.created_vol = float(detail.amount)
                summary.remain = summary.created_vol - summary.vol
                if summary.remain / summary.created_vol < 1e-6:
                    summary.remain = 0
            
            targets = self.targets.setdefault(order.date, {})
            if symbol not in targets:
                klines = self.market_client.get_candlestick(symbol, '1day', 5)
                num = int((now - date2ts(order.date)) // 86400)
                kline = klines[num]
                self.targets[order.date][symbol] = LossTarget(symbol, order.date, kline.open, kline.close, kline.vol)

            target = self.targets[order.date][symbol]

            if order.direction == 'buy':
                target.set_buy(summary.aver_price, summary.vol, summary.amount)
                self.user.buy_id.append(order_id)
            else:
                target.set_sell(summary.amount)
                self.user.sell_id.append(order_id)
            
            self.user.orders[order_id] = summary


        for date, targets in self.targets.items():
            date_symbols = list(set([summary.symbol for summary in self.user.orders.values() if summary.label == date]))
            for symbol in targets.copy():
                if symbol not in date_symbols:
                    del self.targets[date][symbol]

    def check_target(self, target: LossTarget):
        if target.high_mark:
            if target.own and target.price <= target.high_mark_back_price * (1+SELL_UP_RATE):
                self.sell_limit_target(target, target.high_mark_back_price)
                return
        elif target.price >= target.high_mark_price:
            target.high_mark = True
        
        if target.low_mark:
            if target.own and target.price <= target.sell_price * (1+SELL_UP_RATE):
                self.sell_limit_target(target, target.sell_price, selling_level=2)
                return
        elif target.price >= target.low_mark_price:
            target.low_mark = True

    def find_targets(self, symbols=[], min_loss_rate=MIN_LOSS_RATE,
        break_loss_rate=BREAK_LOSS_RATE, end=0, min_before=180
    ):
        infos = self.market_client.get_all_symbols_info()
        if not symbols:
            symbols = infos.keys()
        targets = {}
        now = time.time()

        def worker(symbol):
            try:
                klines = self.market_client.get_candlestick(symbol, '1day', min_before+end+1)[end:]
            except Exception as e:
                print(symbol, e)
                return

            if len(klines) <= min_before:
                return

            rate = get_rate(klines[0].close, klines[0].open)
            if rate >= 0:
                return

            cont_loss_list = [rate]

            for kline in klines[1:]:
                if kline.close < kline.open:
                    cont_loss_list.append(get_rate(kline.close, kline.open))
                else:
                    break
            
            kline = klines[0]
            cont_loss = sum(cont_loss_list)
            if (rate == min(cont_loss_list) and cont_loss <= min_loss_rate) or cont_loss <= break_loss_rate:
                target = LossTarget(symbol, ts2date(kline.id), kline.open, kline.close, kline.vol)
                if now - kline.id > 86400:
                    LossTargetSQL.add_target(
                        symbol=symbol,
                        date=target.date,
                        high=kline.high,
                        low=kline.low,
                        open=kline.open,
                        close=kline.close,
                        vol=kline.vol
                    )
                target.set_info(infos[symbol])
                targets[symbol] = target
                return

            return
        
        run_thread_pool([(worker, (symbol,)) for symbol in symbols], True, 8)
        date = ts2date(now - end * 86400)
        logger.info(f'Targets of {self.user.username} in {date} are {",".join(targets.keys())}')
        return targets, date

    def watch_targets(self, aver_length=AVER_LENGTH):
        def worker():
            time_list = []
            
            while True:
                if not self.date or not self.targets.get(self.date, {}):
                    time.sleep(1)
                    continue

                try:
                    tickers = self.market_client.get_market_tickers()
                    now = time.time()
                    time_list.append(now)
                    for start, each in enumerate(time_list.copy()):
                        if each < now - aver_length:
                            time_list.pop(0)
                        else:
                            break

                    for target in self.targets[self.date].values():
                        target.update_price(tickers, start)
                        self.check_target(target)
                except Exception as e:
                    logger.error(e)

                time.sleep(0.5)

        Timer(0, worker).start()

    def buy_limit_target(self, target: LossTarget, price=0, vol=0, limit_rate=BUY_UP_RATE):
        @retry(tries=5, delay=0.05)
        def filled_callback(summary):
            if summary.aver_price <=0:
                logger.error(f'Fail to buy {target.symbol}')
                return

            target.set_buy(summary.aver_price, summary.vol, summary.amount)

        @retry(tries=5, delay=0.05)
        def cancel_callback(summary):
            if summary.aver_price <=0:
                logger.error(f'Fail to buy {target.symbol}')
                return

            target.set_buy(summary.aver_price, summary.vol, summary.amount)

        if not vol:
            vol = float(self.user.buy_amount) - target.own_amount * target.buy_price

        if not price:
            price = target.price
        price *= 1 + limit_rate

        summary = self.user.buy_limit(target, vol, price)
        if summary != None:
            summary.add_filled_callback(filled_callback, [summary])
            summary.add_cancel_callback(cancel_callback, [summary])
            summary.label = target.date
            OrderSQL.add_order(summary, target.date, self.user.account_id)
        else:
            logger.error(f'Failed to buy {target.symbol}')
        return summary

    @retry(tries=10, delay=0.05)
    def get_sell_amount(self, target):
        available_amount = self.user.get_amount(target.base_currency, True, False)
        assert available_amount > 0.95 * target.own_amount, 'Some unavailable'
        return min(target.own_amount, available_amount)

    def sell_limit_target(self, target: LossTarget, price, sell_amount=0, selling_level=1):
        @retry(tries=5, delay=0.05)
        def filled_callback(summary):
            target.selling = 0
            target.set_sell(summary.amount)

        @retry(tries=5, delay=0.05)
        def cancel_callback(summary):
            target.selling = 0
            target.set_sell(summary.amount)

        if target.selling >= selling_level:
            return
        elif target.selling != 0:
            return self.cancel_and_sell_limit_target(target, price, selling_level)

        target.selling = selling_level
        sell_amount = sell_amount if sell_amount else self.get_sell_amount(target)

        summary = self.user.sell_limit(target, sell_amount, price)
        if summary != None:
            summary.add_filled_callback(filled_callback, [summary])
            summary.add_cancel_callback(cancel_callback, [summary])
            summary.label = target.date
            OrderSQL.add_order(summary, target.date, self.user.account_id)
        else:
            logger.error(f'Failed to sell {target.symbol}')
        return summary

    def cancel_and_sell_limit_target(self, target: LossTarget, price, selling_level=1, direction='sell'):
        @retry(tries=5, delay=0.05)
        def cancel_sell_callback(summary=None):
            if summary:
                target.set_sell(summary.amount)

            sell_amount = self.get_sell_amount(target)
            self.sell_limit_target(target, price, sell_amount, selling_level)

        @retry(tries=5, delay=0.05)
        def cancel_buy_callback(summary=None):
            if summary:
                target.set_buy(summary.aver_price, summary.vol, summary.amount)

            sell_amount = self.get_sell_amount(target)
            self.sell_limit_target(target, price, sell_amount, selling_level)

        symbol = target.symbol
        is_canceled = False
        callback = cancel_sell_callback if direction == 'sell' else cancel_buy_callback
        
        for summary in self.user.orders:
            if (summary.symbol == target.symbol and summary.order_id in (self.user.sell_id if direction == 'sell' else self.user.buy_id)
                and summary.status in [OrderSummaryStatus.PARTIAL_FILLED, OrderSummaryStatus.CREATED] and summary.label == target.date
            ):
                try:
                    summary.add_cancel_callback(callback, [summary])
                    self.user.trade_client.cancel_order(summary.symbol, summary.order_id)
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

    def cancel_and_buy_limit_target(self, target: LossTarget, price=0, limit_rate=BUY_UP_RATE):
        @retry(tries=5, delay=0.05)
        def callback(summary=None):
            if summary:
                target.set_buy(summary.aver_price, summary.vol, summary.amount)

            vol = float(self.buy_amount) - target.own_amount * target.buy_price
            self.buy_limit_target(target, price, vol, limit_rate)

        symbol = target.symbol
        is_canceled = False

        for summary in self.user.orders:
            if (summary.symbol == target.symbol and summary.order_id in self.user.buy_id and summary.label == target.date
                and summary.status in [OrderSummaryStatus.PARTIAL_FILLED, OrderSummaryStatus.CREATED]
            ):
                try:
                    summary.add_cancel_callback(callback, [summary])
                    self.user.trade_client.cancel_order(summary.symbol, summary.order_id)
                    logger.info(f'Cancel open buy order for {symbol}')
                    is_canceled = True
                except Exception as e:
                    logger.error(f'{summary.order_id} {summary.status} {summary.symbol} {e}')
                    # break

        if not is_canceled:
            try:
                callback()
            except Exception as e:
                logger.error(e)
