import time
import math
import hashlib
from retry import retry
from target import LossTarget as Target
from utils import config, logger, user_config, get_rate, datetime, parallel
from order import OrderSummary, OrderSummaryStatus
from client import BaseDealerClient
from user.base import BaseUser as User
from dataset.pgsql import Order as OrderSQL, LossTarget as TargetSQL, Asset
from report import wx_loss_report
from threading import Timer

TEST = user_config.getboolean('setting', 'TEST')

MIN_LOSS_RATE = config.getfloat('loss', 'MIN_LOSS_RATE')
BREAK_LOSS_RATE = config.getfloat('loss', 'BREAK_LOSS_RATE')
UP_LOSS_RATE = config.getfloat('loss', 'UP_LOSS_RATE')
BUY_UP_RATE = config.getfloat('loss', 'BUY_UP_RATE')
SELL_UP_RATE = config.getfloat('loss', 'SELL_UP_RATE')
MAX_DAY = config.getint('loss', 'MAX_DAY')
MIN_NUM = config.getint('loss', 'MIN_NUM')
MAX_NUM = config.getint('loss', 'MAX_NUM')
MIN_VOL = config.getfloat('loss', 'MIN_VOL')
MAX_VOL = config.getfloat('loss', 'MAX_VOL')
MIN_PRICE = config.getfloat('loss', 'MIN_PRICE')
MAX_PRICE = config.getfloat('loss', 'MAX_PRICE')
MIN_BEFORE_DAYS = config.getint('loss', 'MIN_BEFORE_DAYS')
SPECIAL_SYMBOLS = config.get('loss', 'SPECIAL_SYMBOLS')
SPLIT_RATE = config.getfloat('loss', 'SPLIT_RATE')

UP_STOP_CONT_LOSS_RATE = config.getfloat('loss', 'UP_STOP_CONT_LOSS_RATE')
UP_STOP_SMALL_LOSS_RATE = config.getfloat('loss', 'UP_STOP_SMALL_LOSS_RATE')
UP_BREAK_LOSS_RATE = config.getfloat('loss', 'UP_BREAK_LOSS_RATE')

BAN_LIST = ['SLPUSDT']

class LossDealerClient(BaseDealerClient):
    def __init__(self, user: User):
        super().__init__(user=user)
        self.special_symbols = SPECIAL_SYMBOLS.split(',')
        self.targets: dict[str, dict[str, Target]] = {}
        self.date = datetime.ts2date()
        self.client_type = 'loss_dealer'
        logger.info('Start loss strategy.')

    def resume(self):
        def callback(target, summary):
            if summary.direction == 'sell':
                target.selling = 0
                target.set_sell(summary.amount)
            else:
                target.set_buy(summary.vol, summary.amount)

        now = time.time()
        self.date = datetime.ts2date(now - 86400)
        self.targets[self.date] = {}

        start_date = datetime.ts2date(now - (MAX_DAY + 60) * 86400)
        clear_date = datetime.ts2date(now - (MAX_DAY + 1) * 86400)
        targets: list[TargetSQL] = TargetSQL.get_targets([
            TargetSQL.date >= start_date,
            TargetSQL.exchange == self.user.user_type
        ])
        infos = self.market.all_symbol_info

        for target in targets:
            date_target_dict = self.targets.setdefault(target.date, {})
            loss_target = Target(
                target.symbol, target.date, target.open, target.close, target.vol
            )
            klines = self.market.get_candlestick(target.symbol, '1day', 20+1)
            loss_target.his_close = [each.close for each in klines[1:20+1]]
            loss_target.his_close_tmp = False
            loss_target.set_boll()

            loss_target.set_info(infos[target.symbol], self.user.fee_rate)
            loss_target.price = loss_target.now_price = self.market.mark_price[target.symbol]
            date_target_dict[target.symbol] = loss_target

        orders: list[OrderSQL] = OrderSQL.get_orders([
            OrderSQL.account==str(self.user.account_id),
            OrderSQL.date >= start_date
        ])

        for order in orders:
            try:
                if order.symbol not in self.targets.setdefault(order.date, {}):
                    klines = self.market.get_candlestick(order.symbol, '1day', 20+1)
                    ts = datetime.date2ts(order.date)
                    for kline in klines:
                        if kline.id == ts:
                            break
                    
                    loss_target = Target(
                        order.symbol, order.date, kline.open, kline.close, kline.vol
                    )
                    loss_target.his_close = [each.close for each in klines[1:20+1]]
                    loss_target.his_close_tmp = False
                    loss_target.set_boll()
                    loss_target.set_info(infos[order.symbol], self.user.fee_rate)
                    loss_target.price = loss_target.now_price = self.market.mark_price[loss_target.symbol]
                    self.targets[order.date][order.symbol] = loss_target
                    if now - kline.id > 86400:
                        logger.info(f'Update target {loss_target.symbol} in DB')
                        TargetSQL.add_target(
                            symbol=loss_target.symbol,
                            exchange=self.user.user_type,
                            date=loss_target.date,
                            high=kline.high,
                            low=kline.low,
                            open=kline.open,
                            close=kline.close,
                            vol=kline.vol
                        )

                target = self.targets[order.date][order.symbol]
                summary = self.check_order(order, target)
                if summary and summary.status in [1, 2]:
                    summary.add_filled_callback(callback, (target, summary))
                    summary.add_cancel_callback(callback, (target, summary))
                    if summary.direction == 'sell' and not target.selling:
                        target.selling = 2

                time.sleep(0.05)
            except Exception as e:
                logger.error(e)


        for date, targets in self.targets.items():
            date_symbols = [
                summary.symbol for summary
                in list(self.user.orders.values()) + list(self.user.fake_orders.values())
                if summary.label == date
            ]
            for symbol, target in targets.copy().items():
                if symbol not in date_symbols: #or target.own_amount < target.sell_market_min_order_amt:
                    del self.targets[date][symbol]
        
        for symbol, target in self.targets[self.date].items():
            [ticker] = self.market.get_candlestick(symbol, '1day', 1)
            if ticker.high >= target.high_mark_price:
                logger.info(f'{symbol} of {self.date} already reach high mark {target.high_mark_price}, now price {target.now_price}')
                target.high_mark = target.low_mark = True
                if not target.own:
                    logger.info(f'{symbol} of {self.date} is empty')
                elif target.selling > 0:
                    logger.info(f'{symbol} of {self.date} is high back selling')
                    target.high_selling = True
                else:
                    logger.info(f'{symbol} of {self.date} is holding')
            elif ticker.high >= target.low_mark_price:
                logger.info(f'{symbol} of {self.date} already reach low mark {target.low_mark_price}, now price {target.now_price}')
                target.low_mark = True
                if not target.own:
                    logger.info(f'{symbol} of {self.date} is empty')
                elif target.selling > 0:
                    logger.info(f'{symbol} of {self.date} is low back selling')
                    target.low_selling = True
                else:
                    logger.info(f'{symbol} of {self.date} is holding')

        for date, targets in self.targets.items():
            if date >= self.date:
                continue

            for target in targets.values():
                if not target.own:
                    continue

                amount = min(self.user.available_balance[target.base_currency], target.own_amount)
                if amount < target.sell_market_min_order_amt:
                    continue
                
                if target.date > clear_date:
                    klines = self.market.get_candlestick(target.symbol, '1day', 21)
                    target.his_close = [each.close for each in klines[2:]]
                    target.update_boll(klines[1].close)
                    
                    logger.info(f'Find {amount} {target.symbol} of {target.date}, long sell with price {target.long_sell_price}')
                    self.sell_target(
                        target,
                        price=target.long_sell_price,
                        sell_amount=amount,
                        selling_level=1,
                        limit=amount * target.now_price > target.min_order_value
                    )
                else:
                    logger.info(f'Find {amount} {target.symbol} of {target.date}, sell with market price')
                    self.sell_target(
                        target,
                        price=target.clear_price,
                        sell_amount=amount,
                        selling_level=30,
                        limit=False
                    )

        logger.info('Finish loading data')

    def check_target_price(self, target: Target):
        def low_callback(target):
            logger.info(f'Cancel buy {target.symbol} since reach low mark')
            self.cancel_and_sell_target(target, price=0, direction='buy', force=True, sell=False)

        if target.high_check():
            logger.info(f'High back sell {target.symbol}')
            self.cancel_and_sell_target(
                target, target.high_mark_back_price,
                selling_level=3, force=True
            )

        elif target.low_check(low_callback):
            logger.info(f'Low back sell {target.symbol}')
            self.cancel_and_sell_target(
                target, target.low_mark_back_price,
                selling_level=3.5, force=True
            )

    def filter_targets(self, targets, symbols=[], vol=0):
        symbols_num = len(symbols)
        targets_num = len(targets)
        usdt_amount = self.user.get_amount('usdt', available=True, check=False) + vol
        buy_num = max(
            min(targets_num-symbols_num, MAX_NUM-symbols_num),
            MIN_NUM-symbols_num
        )

        if TEST or buy_num == 0:
            buy_amount = self.user.buy_amount
        else:
            buy_amount = usdt_amount // buy_num

        if buy_amount < self.user.min_usdt_amount:
            buy_amount = self.user.min_usdt_amount
            buy_num = int(usdt_amount // buy_amount)

        old_targets_list = [target for symbol, target in targets.items() if symbol in symbols]
        new_targets_list = sorted(
            [target for symbol, target in targets.items() if symbol not in symbols],
            key=lambda x: (x.close > x.boll, -x.vol)
        )[:buy_num]
        logger.info(f'Now money {usdt_amount}U')
        logger.info(f'{len(old_targets_list)} old targets')
        logger.info(f'{len(new_targets_list)} new targets')
        
        
        for target in new_targets_list:
            target.init_buy_amount = buy_amount
            logger.info(f'Select {target.symbol} as new target, buy {buy_amount}U')

        targets = {target.symbol: target for target in old_targets_list + new_targets_list}

        return targets

    def is_buy(self, klines, symbol=''):
        if len(klines) <= MIN_BEFORE_DAYS and symbol not in self.special_symbols:
            return False

        cont_loss_list = []
        for kline in klines:
            rate = get_rate(kline.close, kline.open)
            if rate < 0:
                cont_loss_list.append(rate)
            else:
                break
        
        if not cont_loss_list:
            return False

        kline = klines[0]
        rate = cont_loss_list[0]
        # cont_loss = sum(cont_loss_list)
        cont_loss = 1
        for each in cont_loss_list:
            cont_loss *= 1+each
        cont_loss = cont_loss-1
        max_loss = min(cont_loss_list)
        min_loss = max(cont_loss_list)
        cont_loss_days = len(cont_loss_list)
        
        price = [klines[0].close] + [kline.close for kline in klines[:20-1]]
        boll = sum(price) / len(price)
    
        if (
            MIN_VOL <= kline.vol <= MAX_VOL and
            MIN_PRICE <= kline.close <= MAX_PRICE
        ) and (
            (
                kline.close > boll
                and cont_loss_days==1
                and cont_loss <= UP_LOSS_RATE
            ) or (
                kline.close > boll
                and cont_loss_days > 2
                and cont_loss <= UP_STOP_CONT_LOSS_RATE
                and rate == min_loss
                and rate >= UP_STOP_SMALL_LOSS_RATE
            ) or (
                kline.close > boll
                and cont_loss <= UP_BREAK_LOSS_RATE
                and cont_loss_days > 2
            ) or (
                kline.close <= boll
                and cont_loss <= BREAK_LOSS_RATE
            ) or (
                kline.close <= boll
                and rate == max_loss
                and cont_loss <= MIN_LOSS_RATE
            )
        ):
            return True
        return False

    def find_targets(self, symbols=[], end=0, min_before_days=MIN_BEFORE_DAYS, force=False):
        infos = self.market.get_all_symbols_info()
        ori_symbols = symbols
        if not force:
            symbols = infos.keys()
        targets = {}
        now = time.time()

        @retry(tries=5, delay=1)
        def worker(symbol):
            try:
                klines = self.market.get_candlestick(symbol, '1day', min_before_days+end+1)[end:]
            except Exception as e:
                logger.error(f'[{symbol}]  {e}')
                raise e

            if symbol not in BAN_LIST and (symbol in ori_symbols or self.is_buy(klines, symbol)):
                kline = klines[0]
                target = Target(symbol, datetime.ts2date(kline.id), kline.open, kline.close, kline.vol)
                target.his_close = [each.close for each in klines[:20]]
                target.his_close_tmp = (end == 0)

                target.set_boll()
                if now - kline.id > 86400 and not TEST:
                    TargetSQL.add_target(
                        symbol=symbol,
                        exchange=self.user.user_type,
                        date=target.date,
                        high=kline.high,
                        low=kline.low,
                        open=kline.open,
                        close=kline.close,
                        vol=kline.vol
                    )
                target.set_info(infos[symbol], self.user.fee_rate)
                targets[symbol] = target

        parallel.run_thread_pool([(worker, (symbol,)) for symbol in symbols], True, 4)
        date = datetime.ts2date(now - end * 86400)
        logger.info(f'Targets of {self.user.username} in {date} are {",".join(targets.keys())}')
        return targets, date
    
    def update_targets(self, end=1, min_before_days=MIN_BEFORE_DAYS):
        @retry(tries=5, delay=1)
        def worker(symbol):
            try:
                klines = self.market.get_candlestick(symbol, '1day', min_before_days+end+1)[end:]
            except Exception as e:
                logger.error(f'[{symbol}]  {e}')
                raise e

            kline = klines[0]
            target = self.targets[date][symbol]
            target.vol = kline.vol
            target.set_mark_price(kline.close)
            if end != 0 and target.his_close_tmp:
                target.his_close_tmp = False
                target.his_close[0] = kline.close
            target.set_boll()
            
            if now - kline.id > 86400 and not TEST:
                TargetSQL.add_target(
                    symbol=target.symbol,
                    exchange=self.user.user_type,
                    date=target.date,
                    high=kline.high,
                    low=kline.low,
                    open=kline.open,
                    close=kline.close,
                    vol=kline.vol
                )


        now = time.time()
        date = datetime.ts2date(now - end * 86400)
        parallel.run_thread_pool([
            (worker, (symbol,)) for symbol
            in self.targets.setdefault(date, {}).keys()
        ], True, 4)

    def watch_targets(self):
        if not self.date or not self.targets.get(self.date, {}):
            return

        try:
            tickers = self.market.get_market_tickers()
        except Exception as e:
            logger.error(e)
            return

        for target in self.targets[self.date].values():
            target.update_price(tickers)
            self.check_target_price(target)
    
    def get_sell_amount(self, target):
        @retry(tries=10, delay=0.1)
        def _get_sell_amount():
            available_amount = self.user.get_amount(target.base_currency, True, False)
            assert_word = f'{target.base_currency} not enough, want {target.own_amount} but only have {available_amount}'
            assert available_amount > 0.9 * target.own_amount, assert_word
            return min(target.own_amount, available_amount)

        try:
            return _get_sell_amount()
        except Exception as e:
            if isinstance(e, AssertionError):
                self.user.update_currency(target.base_currency)
                return _get_sell_amount()
            else:
                raise e

    def buy_target(self, target: Target, price=0, vol=0, limit_rate=BUY_UP_RATE, filled_callback=None, cancel_callback=None, limit=True, random=True):
        @retry(tries=5, delay=0.05)
        def _filled_callback(summary):
            if summary.aver_price <=0:
                logger.error(f'Fail to buy {target.symbol}')
                return

            target.set_buy(summary.vol, summary.amount)

        @retry(tries=5, delay=0.05)
        def _cancel_callback(summary):
            if summary.aver_price <=0:
                logger.error(f'Fail to buy {target.symbol}')
                return

            target.set_buy(summary.vol, summary.amount)
        
        if target.high_mark or target.low_mark:
            return

        filled_callback = filled_callback or _filled_callback
        cancel_callback = cancel_callback or _cancel_callback

        if not vol:
            vol = float(target.init_buy_amount) - target.own_amount * target.buy_price

        if vol < target.min_order_value:
            logger.error(f'At least buy {target.min_order_value} but now {vol}')
            return

        if not price:
            price = target.init_price

        if limit:
            if random:
                random_rate = (int(hashlib.sha1(target.date.encode()).hexdigest(), 16)%10) * 0.0001 - 0.0005
            else:
                random_rate = 0
            # random_rate = float(str(hash(target.date))[-1]) / 10000 - 0.0005
            summary = self.user.buy_limit(target, vol, price * (1+limit_rate+random_rate))
        else:
            summary = self.user.buy(target, vol)

        if summary != None:
            summary.add_filled_callback(filled_callback, [summary])
            summary.add_cancel_callback(cancel_callback, [summary])
            summary.label = target.date
            OrderSQL.add_order(summary, target.date, self.user.account_id)
        else:
            logger.error(f'Failed to buy {target.symbol}')
        return summary

    def sell_target(self, target: Target, price, sell_amount=0, selling_level=1, filled_callback=None, cancel_callback=None, limit=True):
        @retry(tries=5, delay=0.05)
        def _filled_callback(summary):
            target.selling = 0
            target.set_sell(summary.amount)

        @retry(tries=5, delay=0.05)
        def _cancel_callback(summary):
            target.selling = 0
            target.set_sell(summary.amount)

        if target.selling >= selling_level:
            return
        elif target.selling != 0:
            return self.cancel_and_sell_target(target, price, selling_level)

        filled_callback = filled_callback or _filled_callback
        cancel_callback = cancel_callback or _cancel_callback
        sell_amount = sell_amount or self.get_sell_amount(target)
        
        if sell_amount * price < target.min_order_value:
            logger.error(f'At least sell {target.min_order_value / price} but now {sell_amount}')
            return
        if limit:
            summary = self.user.sell_limit(target, sell_amount, price)
        else:
            summary = self.user.sell(target, sell_amount)

        if summary != None:
            target.selling = selling_level
            summary.add_filled_callback(filled_callback, [summary])
            summary.add_cancel_callback(cancel_callback, [summary])
            summary.label = target.date
            OrderSQL.add_order(summary, target.date, self.user.account_id)
        else:
            logger.error(f'Failed to sell {target.symbol}')
        return summary

    def cancel_target(self, target: Target, direction='sell'):
        self.cancel_and_sell_target(target, price=0, direction=direction, sell=False, force=True)

    def cancel_and_sell_target(self, target: Target, price, selling_level=1, direction='sell', filled_callback=None, cancel_callback=None, force=False, sell=True, limit=True):
        @retry(tries=5, delay=0.05)
        def cancel_and_sell_callback(summary=None):
            if summary:
                if direction == 'sell' and target.selling != 0:
                    target.set_sell(summary.amount)
                    target.selling = 0
                elif direction == 'buy':
                    target.set_buy(summary.vol, summary.amount)

            if sell:
                sell_amount = self.get_sell_amount(target)
                self.sell_target(
                    target, price, sell_amount, selling_level,
                    filled_callback=filled_callback,
                    cancel_callback=cancel_callback,
                    limit=sell_amount * price > target.min_order_value if limit else False
                )

        if not force and direction=='sell' and selling_level <= target.selling:
            return

        symbol = target.symbol
        is_canceled = False
        
        order_id_list = list(self.user.orders.keys())
        for order_id in order_id_list:
            summary = self.user.orders[order_id]
            if (summary.symbol == target.symbol and summary.order_id in (self.user.sell_id if direction == 'sell' else self.user.buy_id)
                and summary.status in [OrderSummaryStatus.PARTIAL_FILLED, OrderSummaryStatus.CREATED] and summary.label == target.date
            ):
                try:
                    summary.add_cancel_callback(cancel_and_sell_callback, [summary])
                    self.user.cancel_order(summary.symbol, summary.order_id)
                    logger.info(f'Cancel open sell order for {symbol}')
                    is_canceled = True
                except Exception as e:
                    logger.error(f'{summary.order_id} {summary.status} {summary.symbol} {e}')

        if not is_canceled:
            try:
                cancel_and_sell_callback()
            except Exception as e:
                logger.error(e)

    def cancel_and_buy_target(self, target: Target, price=0, limit_rate=BUY_UP_RATE, filled_callback=None, cancel_callback=None, limit=True, split_rate=0):
        @retry(tries=5, delay=0.05)
        def cancel_and_buy_callback(summary=None):
            if summary:
                target.set_buy(summary.vol, summary.amount)

            vol = float(target.init_buy_amount) - target.buy_vol
            if vol < self.user.min_usdt_amount:
                self.buy_target(
                    target, price, vol, limit_rate,
                    filled_callback=filled_callback,
                    cancel_callback=cancel_callback,
                    limit=False
                )
            elif split_rate > 0 and vol * split_rate > self.user.min_usdt_amount:
                self.buy_target(
                    target, price, vol * split_rate, 0,
                    filled_callback=filled_callback,
                    cancel_callback=cancel_callback,
                    limit=limit, random=False
                )
                self.buy_target(
                    target, price, vol * (1-split_rate), limit_rate,
                    filled_callback=filled_callback,
                    cancel_callback=cancel_callback,
                    limit=limit
                )
            else:
                self.buy_target(
                    target, price, vol, limit_rate,
                    filled_callback=filled_callback,
                    cancel_callback=cancel_callback,
                    limit=limit
                )
                

        symbol = target.symbol
        is_canceled = False

        order_id_list = list(self.user.orders.keys())
        for order_id in order_id_list:
            summary = self.user.orders[order_id]
            if (summary.symbol == target.symbol and summary.order_id in self.user.buy_id and summary.label == target.date
                and summary.status in [OrderSummaryStatus.PARTIAL_FILLED, OrderSummaryStatus.CREATED]
            ):
                try:
                    summary.add_cancel_callback(cancel_and_buy_callback, [summary])
                    self.user.cancel_order(summary.symbol, summary.order_id)
                    logger.info(f'Cancel open buy order for {symbol}')
                    is_canceled = True
                except Exception as e:
                    logger.error(f'{summary.order_id} {summary.status} {summary.symbol} {e}')
                    # break

        if not is_canceled:
            try:
                cancel_and_buy_callback()
            except Exception as e:
                logger.error(e)


    @retry(tries=5, delay=0.1)
    def check_order(self, order: OrderSQL, target: Target):
        order_id = int(order.order_id)
        symbol = order.symbol

        if order_id < 0:
            summary = OrderSummary(order_id, symbol, order.direction, order.date)
            summary.limit = True
            summary.created_price = summary.aver_price = order.aver_price
            summary.created_amount = summary.amount = order.amount
            summary.created_ts = summary.ts = order.tm
            summary.status = 3
            summary.vol = order.vol
            summary.fee = 0
            self.user.fake_orders[order_id] = summary
            if order.direction == 'buy':
                target.set_buy(summary.vol, summary.amount, fee_rate=0)
            else:
                target.set_sell(summary.amount)
        else:
            try:
                detail = self.user.get_order(symbol, order_id)
            except Exception as e:
                if 'record invalid' not in str(e):
                    raise e
                else:
                    if not order.finished:
                        order.update([OrderSQL.order_id==order.order_id], {'finished': 1})
                    return

            if order_id in self.user.orders:
                summary = self.user.orders[order_id]
            else:
                summary = OrderSummary(order_id, symbol, order.direction, order.date)
                self.user.orders[order_id] = summary
                if order.direction == 'buy':
                    self.user.buy_id.append(order_id)
                else:
                    self.user.sell_id.append(order_id)
            
            if 'market' in detail.type:
                summary.limit = False
                summary.created_price = 0
            else:
                summary.created_price = detail.price
            

            if detail.type in ['buy-limit', 'sell-limit', 'sell-market']:
                summary.created_amount = detail.amount
                summary.remain = summary.created_amount - summary.amount

            elif detail.type in ['buy-market']:
                summary.created_vol = detail.amount
                summary.remain = summary.created_vol - summary.vol
            summary.created_ts = detail.created_at / 1000
            summary.ts = max(detail.created_at, detail.finished_at, detail.canceled_at) / 1000
            status = {
                'submitted': 1,
                'partial-filled': 2,
                'filled': 3,
                'canceled': 4,
                'partial-canceled': 4,
                'NEW': 1,
                'PARTIALLY_FILLED': 2,
                'FILLED': 3,
                'CANCELED': 4,
                'REJECTED': 0,
                'EXPIRED': 4
            }[detail.state]

            if (detail.filled_amount != summary.amount
                or detail.filled_cash_amount != summary.vol
                or status != summary.status
            ):
                if order.direction == 'buy':
                    target.set_sell(summary.amount, summary.vol)
                    target.set_buy(detail.filled_cash_amount, detail.filled_amount)
                else:
                    target.set_sell(detail.filled_amount - summary.amount)

                summary.status = status
                summary.amount = detail.filled_amount
                summary.vol = detail.filled_cash_amount
                summary.aver_price = summary.vol / summary.amount if summary.amount else summary.created_price
                summary.fee = summary.vol * self.user.fee_rate

        return summary

    def report(self, force, send=True):
        now = time.time()
        start_date = datetime.ts2date(now - (MAX_DAY + 2) * 86400)
        orders: list[OrderSQL] = OrderSQL.get_orders([
            OrderSQL.account==str(self.user.account_id),
            OrderSQL.date >= start_date,
            OrderSQL.finished == 0
        ])

        report_info = {
            'new_buy': [],
            'new_sell': [],
            'opening': [],
            'holding': [],
            'summary': []
        }

        for order in orders:
            order_id = int(order.order_id)
            target = self.targets[order.date][order.symbol]
            summary = self.check_order(order, target)

            if order_id not in self.user.orders:
                continue

            if summary.amount != order.amount:
                amount = summary.amount - order.amount
                vol = summary.vol - order.vol
                price = vol / amount
                # fee = vol * 0.02
                symbol = summary.symbol
                tm = datetime.ts2time(summary.ts)
                profit_rate = summary.aver_price  / target.buy_price * (1-summary.fee_rate)**2 - 1
                profit = summary.amount * target.buy_price * profit_rate
                report_info[f'new_{order.direction}'].append((
                    tm, symbol, amount, vol, price, profit, profit_rate, target.buy_price
                ))
                
                
                update_load = {
                    'amount': summary.amount,
                    'vol': summary.vol,
                    'aver_price': summary.aver_price,
                    'finished': 0 if summary.status in [1, 2] else 1,
                    'tm': tm
                }
                OrderSQL.update([OrderSQL.order_id==order.order_id], update_load)

            elif summary.status in [1, 2]:
                symbol = summary.symbol
                price = summary.created_price
                amount = summary.remain
                report_info['opening'].append((
                    datetime.ts2time(summary.created_ts), symbol, amount, price, summary.direction
                ))
                if summary.amount == 0 and summary.aver_price != order.aver_price:
                    update_load = {
                        'aver_price': summary.aver_price,
                        'tm': datetime.ts2time(summary.ts)
                    }
                    OrderSQL.update([OrderSQL.order_id==order.order_id], update_load)

            elif summary.status in [-1, 3, 4]:
                update_load = {'finished': 1}
                if summary.ts:
                    update_load['tm'] = datetime.ts2time(summary.ts)
                OrderSQL.update([OrderSQL.order_id==order.order_id],update_load)
            
                

        if not force and not report_info['new_sell'] + report_info['new_buy']:
            return

        target_info = {}
        for targets in self.targets.values():
            for target in targets.values():
                symbol = target.symbol
                price = target.price
                amount = target.own_amount
                vol = amount * price
                if target.own and amount and vol >= 1:
                    target_info.setdefault(symbol, {'amount': 0, 'vol': 0, 'price': 0, 'buy_vol': 0, 'date': target.date})
                    target_info[symbol]['amount'] += amount
                    target_info[symbol]['buy_vol'] += amount * target.real_buy_price

        tickers = self.market.get_market_tickers()
        for ticker in tickers:
            if ticker.symbol in target_info:
                info = target_info[ticker.symbol]
                info['price'] = ticker.close
                info['vol'] = info['amount'] * info['price']
        
        for symbol, info in target_info.items():
            amount = info['amount']
            buy_vol = info['buy_vol']
            buy_price = buy_vol / amount
            price = info['price']
            profit = price * amount - buy_vol
            percent = profit / buy_vol
            date = info['date']
            report_info['holding'].append((
                symbol, amount, buy_price, price, profit, percent, date
            ))

        if not report_info['holding'] + report_info['new_sell'] + report_info['new_buy']:
            return

        usdt = self.user.get_amount('usdt', True, False)
        float_profit = sum([each[4] for each in report_info['holding']])
        day_profit, month_profit, all_profit = OrderSQL.get_profit(self.user.account_id)

        if send:
            wx_loss_report(self.user.user_type, self.user.wxuid, self.user.username, report_info, usdt, day_profit, month_profit, all_profit)

        logger.info('Summary')
        for each in report_info['new_buy']:
            logger.info(f'New buy: {each[0]}, {each[1]}, amount {each[2]:.4f}, vol {each[3]:.3f}U, price {each[4]:.6g}U')
        for each in report_info['new_sell']:
            logger.info(f'New sell: {each[0]}, {each[1]}, amount {each[2]:.4f}, vol {each[3]:.3f}U, price {each[4]:.6g}U, buy price {each[7]:.6g}U, profit {each[5]:.3f}U, {each[6]:.2%}')
        for each in report_info['opening']:
            logger.info(f'Opening order: {each[0]}, {each[1]}, {each[4]}, left amount {each[2]:.4f}, price {each[3]:.6g}U')
        for each in report_info['holding']:
            logger.info(f'Holding: {each[0]}, holding amount {each[1]:.4f}, buy price {each[2]:.6g}U, now price {each[3]:.6g}U, profit {each[4]:.3f}U, {each[5]:.2%}')
        logger.info(f'Holding profit {float_profit:.3f}U, Usable money {usdt:.3f}U')
        logger.info(f'Day profit {day_profit:.3f}U, Month profit {month_profit:.3f}U, All profit: {all_profit:.3f}')

    def fake_trade(self, target: Target, data: dict, direction: str):
        now = data['T']
        trans_amount = data['z']
        trans_vol = data['Z']
        if direction == 'sell':
            id = int(f'-{now}{self.user.account_id}{0}')
            target.set_sell(trans_amount)
        elif direction == 'buy':
            id = int(f'-{now}{self.user.account_id}{1}')
            target.set_buy(trans_vol, trans_amount, fee_rate=0)
            
        summary = OrderSummary(id, target.symbol, direction, target.date)
        summary.update(data, fee_rate=0)
        self.user.fake_orders[id] = summary
        OrderSQL.add_order(summary, target.date, self.user.account_id)

    def fake_trans(self, target: Target, new_target: Target, price: float):
        new_target_amount = new_target.init_buy_amount / price
        new_target_need_amount = max(new_target_amount - new_target.own_amount, 0)
        trans_amount = target.check_amount(min(new_target_need_amount, target.own_amount))
        if trans_amount > 0.1 ** target.amount_precision:
            logger.info(f'{target.symbol} overlaps, transfer {trans_amount} from {target.date} to {new_target.date} with price {price}')
            now = datetime.time2ts()
            trans_vol = trans_amount * price
            data = {
                'from': 'binance',
                'T': round(now * 1000),
                'z': trans_amount,
                'Z': trans_vol,
                'X': 'FILLED'
            }
            self.fake_trade(target, data, 'sell')
            self.fake_trade(new_target, data, 'buy')

    @retry(tries=10, delay=1, logger=logger)
    def sell_targets(self, date=None):
        def clear_buy(yesterday):
            for target in self.targets.get(yesterday, {}).values():
                self.cancel_target(target, 'buy')
        
        def clear_yesterday_targets(targets: 'list[Target]', limit=True, level=4):
            try:
                for target in targets:
                    if not target.own or target.own_amount < target.sell_market_min_order_amt:
                        target.own = False
                        continue

                    logger.info(f'Sell {target.symbol} of {target.date}(yesterday)')
                    market_price = target.now_price * (1-SELL_UP_RATE)
                    sell_price = max(target.clear_price, market_price)
                    self.cancel_and_sell_target(target, sell_price, level, limit=limit)
            except Exception as e:
                logger.error(e)

        def long_sell_yesterday_targets(targets: 'list[Target]', limit=True, level=5):
            try:
                for target in targets:
                    if not target.own or target.own_amount < target.sell_market_min_order_amt:
                        target.own = False
                        continue
                    
                    logger.info(f'Long sell {target.symbol} of {target.date}(yesterday)')
                    self.cancel_and_sell_target(
                        target, target.long_sell_price,
                        level, limit=limit
                    )
            except Exception as e:
                logger.error(e)

        def clear_old_targets(targets: 'list[Target]', limit=True, level=6):
            try:
                tickers = self.market.get_market_tickers()
                for target in targets:
                    target.update_price(tickers)
                    symbol = target.symbol
                    now_price = target.now_price
                    if not target.own or target.own_amount < target.sell_market_min_order_amt:
                        target.own = False
                        continue
                        
                    if symbol in self.targets[self.date]:
                        new_target = self.targets[self.date][symbol]
                        if new_target.own_amount == 0:
                            self.fake_trans(target, new_target, now_price)
                            if not target.own or target.own_amount < target.sell_market_min_order_amt:
                                target.own = False
                                continue
                        
                    logger.info(f'Finally sell {target.symbol} of {target.date}')
                    market_price = now_price * (1-SELL_UP_RATE)
                    self.cancel_and_sell_target(target, market_price, level, limit=limit)
            except Exception as e:
                logger.error(e)

        def get_targets(clear_date, yesterday):
            sell_vol = 0
            old_targets = []
            clear_targets = []
            long_sell_targets = []
            tickers = self.market.get_market_tickers()

            for day, targets in self.targets.items():
                for target in targets.values():
                    if not target.own or target.own_amount < target.sell_market_min_order_amt:
                        target.own = False
                        continue
                        
                    target.update_price(tickers)
                    if day <= clear_date:
                        old_targets.append(target)
                        sell_vol += target.own_amount * target.now_price
                    elif day == yesterday:
                        
                        if target.now_price <= target.clear_price:
                            target.update_boll(target.now_price)
                            long_sell_targets.append(target)
                        else:
                            clear_targets.append(target)
                            sell_vol += target.own_amount * target.now_price
            
            return sell_vol, old_targets, clear_targets, long_sell_targets
        
        date = date or self.date
        clear_date = datetime.ts2date(datetime.date2ts(date) - (MAX_DAY-1) * 86400)
        yesterday = date

        logger.info('Cancel all buy orders')
        clear_buy(yesterday)
        time.sleep(10)

        logger.info('Find new target and check old')

        targets, new_date = self.find_targets()
        sell_vol, old_targets, yesterday_targets, long_sell_targets = get_targets(clear_date, yesterday)
        targets = self.filter_targets(targets, vol=sell_vol)
        self.targets[new_date] = targets
        self.date = max(self.targets.keys())

        logger.info('Start to sell')
        old_symbols = ",".join(set([target.symbol for target in old_targets]))
        logger.info(f'Clear old before {clear_date}. targets are {old_symbols}')
        symbols = ','.join(set([target.symbol for target in yesterday_targets]))
        logger.info(f'Clear yesterday {yesterday}. targets are {symbols}')
        logger.info(f'After sell will get {sell_vol}U')

        Timer(0, clear_old_targets, kwargs={'targets': old_targets, 'level': 6}).start()
        Timer(15, clear_old_targets, kwargs={'targets': old_targets, 'level': 6.1}).start()
        Timer(30, clear_old_targets, kwargs={'targets': old_targets, 'level': 6.2}).start()
        Timer(45, clear_old_targets, kwargs={'targets': old_targets, 'level': 6.3}).start()
        Timer(90, clear_old_targets, kwargs={'targets': old_targets, 'level': 6.5, 'limit': False}).start()

        Timer(0, clear_yesterday_targets, kwargs={'targets': yesterday_targets, 'level': 4}).start()
        Timer(15, clear_yesterday_targets, kwargs={'targets': yesterday_targets, 'level': 4.1}).start()
        Timer(30, clear_yesterday_targets, kwargs={'targets': yesterday_targets, 'level': 4.2}).start()
        Timer(45, clear_yesterday_targets, kwargs={'targets': yesterday_targets, 'level': 4.3}).start()
        Timer(90, clear_yesterday_targets, kwargs={'targets': yesterday_targets, 'level': 4.5, 'limit': False}).start()

        Timer(95, long_sell_yesterday_targets, kwargs={'targets': long_sell_targets, 'level': 4}).start()

    def update_and_buy_targets(self, end=1):
        def get_buy_vol(own_vols, target_num, total_vol, min_vol):
            left_vol = total_vol
            own_num = len(own_vols)
            for i, own_vol in enumerate(own_vols):
                last_vol = own_vols[i-1] if i else 0
                vol_diff = own_vol - last_vol
                num = target_num - own_num + i
                if num * vol_diff > left_vol:
                    buy_vol = left_vol / num + last_vol
                    break
                else:
                    left_vol -= num * vol_diff
            else:
                buy_vol = left_vol / target_num + max(own_vols+[0])
                
            buy_num = target_num
            for own_vol in reversed(own_vols):
                if own_vol > buy_vol:
                    buy_num -= 1
                    continue
                
                new_vol = buy_vol - own_vol
                if new_vol < min_vol:
                    buy_num -= 1
                    if buy_num > 0:
                        buy_vol += new_vol / buy_num

            return math.floor(max(min_vol, buy_vol))
        
        logger.info('Start to update and buy today\'s targets')
        date = datetime.ts2date(time.time()-end*86400)

        self.update_targets(end)
        targets = self.targets[date]

        usdt_amount = self.user.get_amount('usdt', available=True, check=False)
        logger.info(f'Now available {usdt_amount}U')
        min_usdt_amount = self.user.min_usdt_amount
        own_vols = sorted([target.buy_vol for target in targets.values() if target.buy_vol])
        target_num = len(targets)
        left_vol = total_vol = usdt_amount * min(target_num/ MIN_NUM,  1)
        buy_vol = get_buy_vol(own_vols, target_num, total_vol, min_usdt_amount)
        logger.info(f'Each buy {buy_vol}U')

        order_targets = sorted(
            [target for target in targets.values()],
            key=lambda x: (-x.buy_vol, x.close > x.boll, -x.vol)
        )

        for target in order_targets:
            now_target = self.targets[date][target.symbol]
            add_vol = math.floor(buy_vol - now_target.buy_vol)
            if left_vol < min_usdt_amount or add_vol < min_usdt_amount or add_vol > left_vol:
                now_target.init_buy_amount = now_target.buy_vol
            else:
                now_target.init_buy_amount = buy_vol
                left_vol -= add_vol
                self.cancel_and_buy_target(now_target, target.boll_target_buy_price, limit_rate=0)

    def update_asset(self, limit=1):
        if limit > 1:
            limit = min(limit, 30)
            asset_his = self.user.get_asset_history(limit)
            for date, asset in asset_his:
                Asset.add_asset(self.user.account_id, date, asset)
        else:
            date, asset = self.user.get_asset()
            Asset.add_asset(self.user.account_id, date, asset)
