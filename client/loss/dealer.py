import time

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

        start_date = datetime.ts2date(now - (MAX_DAY + 2) * 86400)
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
                    klines = self.market.get_candlestick(order.symbol, '1day', 10)
                    ts = datetime.date2ts(order.date)
                    for kline in klines:
                        if kline.id == ts:
                            break

                    loss_target = Target(
                        order.symbol, order.date, kline.open, kline.close, kline.vol
                    )
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
                in self.user.orders.values()
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
                    logger.info(f'{symbol} of {self.date} has been sold')
                elif target.selling > 0:
                    logger.info(f'{symbol} of {self.date} is high back selling')
                    target.high_selling = True
                else:
                    logger.info(f'{symbol} of {self.date} is holding')
            elif ticker.high >= target.low_mark_price:
                logger.info(f'{symbol} of {self.date} already reach low mark {target.low_mark_price}, now price {target.now_price}')
                target.low_mark = True
                if not target.own:
                    logger.info(f'{symbol} of {self.date} has been sold')
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
                logger.info(f'Find old {amount} {target.symbol} of {target.date}, long sell with price {target.long_sell_price}')
                self.sell_target(
                    target,
                    price=target.long_sell_price,
                    sell_amount=amount,
                    selling_level=1,
                    limit=amount * target.now_price > target.min_order_value
                )

        logger.info('Finish loading data')

    def check_target_price(self, target: Target):
        def low_callback(target):
            logger.info(f'Cancel buy {target.symbol} since reach low mark')
            self.cancel_and_sell_limit_target(target, price=0, direction='buy', force=True, sell=False)

        if target.high_check():
            logger.info(f'High back sell {target.symbol}')
            self.cancel_and_sell_limit_target(
                target, target.high_mark_back_price,
                selling_level=3, force=True
            )

        elif target.low_check(low_callback):
            logger.info(f'Low back sell {target.symbol}')
            self.cancel_and_sell_limit_target(
                target, target.low_mark_back_price,
                selling_level=3.5, force=True
            )

    def filter_targets(self, targets, symbols=[]):
        symbols_num = len(symbols)
        targets_num = len(targets)
        usdt_amount = self.user.get_amount('usdt', available=True, check=False)
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
        cont_loss = sum(cont_loss_list)
        max_loss = min(cont_loss_list)
        boll = sum([kline.close for kline in klines[:20]]) / 20
        if (
            (
                (len(cont_loss_list)==1 and cont_loss <= UP_LOSS_RATE and kline.close > boll) or
                (rate == max_loss and cont_loss <= MIN_LOSS_RATE) or 
                cont_loss <= BREAK_LOSS_RATE
            )
            and MIN_VOL <= kline.vol <= MAX_VOL and MIN_PRICE <= kline.close <= MAX_PRICE
        ):
            return True
        return False

    def find_targets(self, symbols=[], end=0, min_before_days=MIN_BEFORE_DAYS, force=False):
        infos = self.market.get_all_symbols_info()
        ori_symbols = symbols
        if len(symbols) < MIN_NUM and not force:
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

            if symbol in ori_symbols or self.is_buy(klines, symbol):
                kline = klines[0]
                target = Target(symbol, datetime.ts2date(kline.id), kline.open, kline.close, kline.vol)
                target.boll = sum([kline.close for kline in klines[:20]]) / 20
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
        @retry(tries=10, delay=0.05)
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

    def buy_target(self, target: Target, price=0, vol=0, limit_rate=BUY_UP_RATE, filled_callback=None, cancel_callback=None, limit=True):
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
            price = target.now_price

        if limit:
            random_rate = float(str(hash(target.date))[-1]) / 10000 - 0.0005
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
            return self.cancel_and_sell_limit_target(target, price, selling_level)

        filled_callback = filled_callback or _filled_callback
        cancel_callback = cancel_callback or _cancel_callback
        target.selling = selling_level
        sell_amount = sell_amount or self.get_sell_amount(target)
        if limit:
            if sell_amount * price < target.min_order_value:
                logger.error(f'At least sell {target.min_order_value / price} but now {sell_amount}')
                return
            summary = self.user.sell_limit(target, sell_amount, price)
        else:
            if sell_amount < target.sell_market_min_order_amt:
                logger.error(f'At least market sell {target.sell_market_min_order_amt} but now {sell_amount}')
                return
            summary = self.user.sell(target, sell_amount)

        if summary != None:
            summary.add_filled_callback(filled_callback, [summary])
            summary.add_cancel_callback(cancel_callback, [summary])
            summary.label = target.date
            OrderSQL.add_order(summary, target.date, self.user.account_id)
        else:
            logger.error(f'Failed to sell {target.symbol}')
        return summary

    def cancel_target(self, target: Target, direction='sell'):
        self.cancel_and_sell_limit_target(target, price=0, direction=direction, sell=False, force=True)

    def cancel_and_sell_limit_target(self, target: Target, price, selling_level=1, direction='sell', filled_callback=None, cancel_callback=None, force=False, sell=True):
        @retry(tries=5, delay=0.05)
        def cancel_and_sell_callback(summary=None):
            if summary:
                target.selling = 0
                if direction == 'sell':
                    target.set_sell(summary.amount)
                else:
                    target.set_buy(summary.vol, summary.amount)

            if sell:
                sell_amount = self.get_sell_amount(target)
                self.sell_target(
                    target, price, sell_amount, selling_level,
                    filled_callback=filled_callback,
                    cancel_callback=cancel_callback,
                    limit=sell_amount * price > target.min_order_value
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

    def cancel_and_buy_limit_target(self, target: Target, price=0, limit_rate=BUY_UP_RATE, filled_callback=None, cancel_callback=None):
        @retry(tries=5, delay=0.05)
        def cancel_and_buy_callback(summary=None):
            if summary:
                target.set_buy(summary.vol, summary.amount)

            self.buy_target(
                target, price, None, limit_rate,
                filled_callback=filled_callback,
                cancel_callback=cancel_callback
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

            if summary.amount != order.amount or summary.vol != order.vol:
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

    @retry(tries=10, delay=1, logger=logger)
    def sell_targets(self, date=None):
        def clear_buy():
            for target in self.targets.get(date, {}).values():
                self.cancel_target(target, 'buy')
        
        def clear_today_targets(level=4):
            for target in self.targets.get(date, {}).values():
                if not target.own or target.own_amount < target.sell_market_min_order_amt:
                    target.own = False
                    continue

                logger.info(f'Sell {target.symbol} of {target.date}(yestoday)')
                market_price = target.now_price * (1-SELL_UP_RATE)
                sell_price = max(target.clear_price, market_price)
                self.cancel_and_sell_limit_target(target, sell_price, level)

        def long_sell_today_targets(level=5):
            for target in self.targets.get(date, {}).values():
                if not target.own or target.own_amount < target.sell_market_min_order_amt:
                    target.own = False
                    continue
                
                logger.info(f'Long sell {target.symbol} of {target.date}(yestoday)')
                self.cancel_and_sell_limit_target(target, target.long_sell_price, level)

        def clear_old_targets(level=6):
            tickers = self.market.get_market_tickers()
            for ticker in tickers:
                symbol = ticker.symbol
                for target in clear_targets.get(symbol, []):
                    if not target.own or target.own_amount < target.sell_market_min_order_amt:
                        target.own = False

                    logger.info(f'Finally sell {target.symbol} of {target.date}')
                    market_price = ticker.close * (1-SELL_UP_RATE)
                    self.cancel_and_sell_limit_target(target, market_price, level)

        logger.info('Start to sell')
        date = date or self.date
        clear_date = datetime.ts2date(datetime.date2ts(date) - (MAX_DAY-1) * 86400)
        clear_targets = {}
        for day, targets in self.targets.items():
            for symbol, target in targets.items():
                if (
                    day <= clear_date and target.own and 
                    target.own_amount > target.sell_market_min_order_amt
                ):
                    clear_targets.setdefault(symbol, []).append(target)

        Timer(0, clear_buy).start()
        
        clear_symbols = ",".join(clear_targets.keys())
        logger.info(f'Clear old: {clear_date}. targets are {clear_symbols}')
        Timer(0, clear_old_targets, args=[6]).start()
        Timer(15, clear_old_targets, args=[6.1]).start()
        Timer(30, clear_old_targets, args=[6.2]).start()

        symbols = ','.join(self.targets.get(date, {}).keys())
        logger.info(f'Clear yesterday: {date}. targets are {symbols}')
        Timer(0, clear_today_targets, args=[4]).start()
        Timer(15, clear_today_targets, args=[4.1]).start()
        Timer(30, clear_today_targets, args=[4.2]).start()

        Timer(120, long_sell_today_targets, args=[5]).start()

        

    def buy_targets(self, end=0):
        logger.info('Start to find today new targets')
        targets, date = self.find_targets(end=end)
        targets = self.filter_targets(targets)
        self.targets[date] = targets
        self.date = max(self.targets.keys())

        for target in self.targets[self.date].values():
            self.buy_target(target)

    def update_targets(self, end=1):
        def cancel(target):
            logger.info(f'Too long time, stop to buy {target.symbol}')
            order_id_list = list(self.user.orders.keys())
            for order_id in order_id_list:
                summary = self.user.orders[order_id]
                if (summary.symbol == target.symbol and summary.order_id in self.user.buy_id 
                    and summary.label == target.date and summary.status in [1, 2]
                ):
                    try:
                        self.user.cancel_order(summary.symbol, summary.order_id)
                        logger.info(f'Cancel open buy order for {target.symbol}')
                    except Exception as e:
                        logger.error(f'{summary.order_id} {summary.status} {summary.symbol} {e}')

        logger.info('Start to update today\'s targets')
        date = datetime.ts2date(time.time()-end*86400)
        symbols = self.targets[date].keys()
        targets, _ = self.find_targets(symbols=symbols, end=end)
        targets = self.filter_targets(targets, symbols)
        for symbol, target in targets.items():
            now_target = self.targets[date].setdefault(symbol, target)
            now_target.set_mark_price(target.init_price)
            self.cancel_and_buy_limit_target(now_target, target.init_price)
            Timer(85000, cancel, args=[now_target]).start()
            
    def update_asset(self, limit=1):
        if limit > 1:
            limit = min(limit, 30)
            asset_his = self.user.get_asset_history(limit)
            for date, asset in asset_his:
                Asset.add_asset(self.user.account_id, date, asset)
        else:
            date, asset = self.user.get_asset()
            Asset.add_asset(self.user.account_id, date, asset)
