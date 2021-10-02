import time

from retry import retry
from target import LossTarget as Target
from utils import config, logger, user_config, get_rate, datetime, parallel
from order import OrderSummary, OrderSummaryStatus
from client import BaseDealerClient
from user import BaseUser as User
from dataset.pgsql import Order as OrderSQL, LossTarget as TargetSQL
from report import wx_loss_report

MIN_LOSS_RATE = config.getfloat('loss', 'MIN_LOSS_RATE')
BREAK_LOSS_RATE = config.getfloat('loss', 'BREAK_LOSS_RATE')
BUY_UP_RATE = config.getfloat('loss', 'BUY_UP_RATE')
SELL_UP_RATE = config.getfloat('loss', 'SELL_UP_RATE')
MAX_DAY = config.getint('loss', 'MAX_DAY')
MIN_NUM = config.getint('loss', 'MIN_NUM')
MAX_NUM = config.getint('loss', 'MAX_NUM')
TEST = user_config.getboolean('setting', 'TEST')


class LossDealerClient(BaseDealerClient):
    def __init__(self, user: User):
        super().__init__(user=user)
        self.targets: dict[str, dict[str, Target]] = {}
        self.date = datetime.ts2date()
        self.client_type = 'loss_dealer'

    def resume(self):
        now = time.time()
        start_date = datetime.ts2date(now - (MAX_DAY + 2) * 86400)
        targets: list[TargetSQL] = TargetSQL.get_targets([
            TargetSQL.date >= start_date,
            TargetSQL.exchange == self.user.user_type
        ])
        infos = self.market_client.all_symbol_info

        for target in targets:
            date_target_dict = self.targets.setdefault(target.date, {})
            date_target_dict[target.symbol] = Target(
                target.symbol, target.date, target.open, target.close, target.vol
            )
            date_target_dict[target.symbol].set_info(infos[target.symbol], self.user.fee_rate)

        orders: list[OrderSQL] = OrderSQL.get_orders([
            OrderSQL.account==str(self.user.account_id),
            OrderSQL.date >= start_date
        ])

        for order in orders:
            try:
                if order.symbol not in self.targets.setdefault(order.date, {}):
                    klines = self.market_client.get_candlestick(order.symbol, '1day', 5)
                    num = int((now - datetime.date2ts(order.date)) // 86400)
                    kline = klines[num]
                    self.targets[order.date][order.symbol] = Target(
                        order.symbol, order.date, kline.open, kline.close, kline.vol
                    )
                    self.targets[order.date][order.symbol].set_info(infos[order.symbol], self.user.fee_rate)

                target = self.targets[order.date][order.symbol]
                self.check_order(order, target)
                time.sleep(0.05)
            except Exception as e:
                logger.error(e)


        for date, targets in self.targets.items():
            date_symbols = list(set([summary.symbol for summary in self.user.orders.values() if summary.label == date]))
            for symbol in targets.copy():
                if symbol not in date_symbols:
                    del self.targets[date][symbol]
        
        self.date = max(self.targets.keys())

    def check_target(self, target: Target):
        if target.high_mark:
            if target.own and target.price <= target.high_mark_back_price * (1+SELL_UP_RATE):
                self.sell_target(target, target.high_mark_back_price)
                return
        elif target.price >= target.high_mark_price:
            target.high_mark = True
        
        if target.low_mark:
            if target.own and target.price <= target.sell_price * (1+SELL_UP_RATE):
                self.sell_target(target, target.sell_price, selling_level=2)
                return
        elif target.price >= target.low_mark_price:
            target.low_mark = True

    def filter_targets(self, targets):
        targets_num = len(targets)
        usdt_amount = self.user.get_amount('usdt', available=True, check=False)
        buy_num = max(min(targets_num, MAX_NUM), MIN_NUM)
        buy_amount = usdt_amount // buy_num
        if buy_amount < self.user.min_usdt_amount:
            buy_amount = self.user.min_usdt_amount
            buy_num = int(usdt_amount // buy_amount)

        if not TEST:
            self.user.buy_amount = buy_amount

        targets = {
            target.symbol: target for target in
            sorted(targets.values(), key=lambda x: -x.vol)[:buy_num]
        }
        return targets

    def find_targets(self, symbols=[], min_loss_rate=MIN_LOSS_RATE,
        break_loss_rate=BREAK_LOSS_RATE, end=0, min_before=180
    ):
        infos = self.market_client.get_all_symbols_info()
        if not symbols:
            symbols = infos.keys()
        targets = {}
        now = time.time()

        @retry(tries=5, delay=1)
        def worker(symbol):
            klines = self.market_client.get_candlestick(symbol, '1day', min_before+end+1)[end:]

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
                target = Target(symbol, datetime.ts2date(kline.id), kline.open, kline.close, kline.vol)
                if now - kline.id > 86400:
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

        
        parallel.run_thread_pool([(worker, (symbol,)) for symbol in symbols], True, 8)
        date = datetime.ts2date(now - end * 86400)
        logger.info(f'Targets of {self.user.username} in {date} are {",".join(targets.keys())}')
        return targets, date

    def watch_targets(self):
        if not self.date or not self.targets.get(self.date, {}):
            return

        tickers = self.market_client.get_market_tickers()
        for target in self.targets[self.date].values():
            target.update_price(tickers)
            self.check_target(target)
    
    @retry(tries=10, delay=0.05)
    def get_sell_amount(self, target):
        available_amount = self.user.get_amount(target.base_currency, True, False)
        assert_word = f'{target.base_currency} not enough, want {target.own_amount} but only have {available_amount}'
        assert available_amount > 0.9 * target.own_amount, assert_word
        return min(target.own_amount, available_amount)

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
        
        filled_callback = filled_callback or _filled_callback
        cancel_callback = cancel_callback or _cancel_callback

        if not vol:
            vol = float(self.user.buy_amount) - target.own_amount * target.buy_price

        if not price:
            price = target.now_price
        price *= 1 + limit_rate

        if limit:
            summary = self.user.buy_limit(target, vol, price)
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
            summary = self.user.sell_limit(target, sell_amount, price)
        else:
            summary = self.user.sell(target, sell_amount)

        if summary != None:
            summary.add_filled_callback(filled_callback, [summary])
            summary.add_cancel_callback(cancel_callback, [summary])
            summary.label = target.date
            OrderSQL.add_order(summary, target.date, self.user.account_id)
        else:
            logger.error(f'Failed to sell {target.symbol}')
        return summary

    def cancel_and_sell_limit_target(self, target: Target, price, selling_level=1, direction='sell', filled_callback=None, cancel_callback=None):
        @retry(tries=5, delay=0.05)
        def cancel_and_sell_callback(summary=None):
            if summary:
                target.selling = 0
                if direction == 'sell':
                    target.set_sell(summary.amount)
                else:
                    target.set_buy(summary.vol, summary.amount)

            sell_amount = self.get_sell_amount(target)
            if sell_amount > target.sell_market_min_order_amt:
                self.sell_target(
                    target, price, sell_amount, selling_level,
                    filled_callback=filled_callback,
                    cancel_callback=cancel_callback,
                    limit=sell_amount * price > target.min_order_value
                )

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
                    self.user.trade_client.cancel_order(summary.symbol, summary.order_id)
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

            vol = float(self.user.buy_amount) - target.own_amount * target.buy_price
            self.buy_target(
                target, price, vol, limit_rate,
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
                    self.user.trade_client.cancel_order(summary.symbol, summary.order_id)
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
            detail = self.user.get_order(order_id)
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
        summary.ts = max(detail.created_at, detail.finished_at) / 1000 
        status = {
            'submitted': 1,
            'partial-filled': 2,
            'filled': 3,
            'canceled': 4,
            'partial-canceled': 4,
            'NEW': 1,
            'PARTIAL_FILLED': 2,
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
            summary.aver_price = summary.vol / summary.amount if summary.amount else 0
            summary.fee = summary.vol * self.user.fee_rate

    def report(self, force):
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
            self.check_order(order, self.targets[order.date][order.symbol])

            if order_id not in self.user.orders:
                continue

            summary = self.user.orders[order_id]
            if summary.amount != order.amount or summary.vol != order.vol:
                amount = summary.amount - order.amount
                vol = summary.vol - order.vol
                price = vol / amount
                # fee = vol * 0.02
                symbol = summary.symbol
                tm = datetime.ts2time(summary.ts)
                report_info[f'new_{order.direction}'].append((
                    tm, symbol, amount, vol, price
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

            elif summary.status in [-1, 3, 4]:
                load = {'finished': 1}
                if summary.ts:
                    load['tm'] = datetime.ts2time(summary.ts)
                OrderSQL.update([OrderSQL.order_id==order.order_id],load)

        if not force and not report_info['new_sell'] and not report_info['new_buy']:
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
                    target_info[symbol]['buy_vol'] += target.buy_vol

        tickers = self.market_client.get_market_tickers()
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

        usdt = self.user.get_amount('usdt', True, False)
        float_profit = sum([each[4] for each in report_info['holding']])
        day_profit, month_profit, all_profit = OrderSQL.get_profit(self.user.account_id)

        wx_loss_report(self.user.account_id, self.user.wxuid, self.user.username, report_info, usdt, day_profit, month_profit, all_profit)

        logger.info('Summary')
        for each in report_info['new_buy']:
            logger.info(f'New buy: {each[0]}, {each[1]}, amount {each[2]:.4f}, vol {each[3]:.3f}U, price {each[4]:.6g}U')
        for each in report_info['new_sell']:
            logger.info(f'New sell: {each[0]}, {each[1]}, amount {each[2]:.4f}, vol {each[3]:.3f}U, price {each[4]:.6g}U')
        for each in report_info['opening']:
            logger.info(f'Opening order: {each[0]}, {each[1]}, {each[4]}, left amount {each[2]:.4f}, price {each[3]:.6g}U')
        for each in report_info['holding']:
            logger.info(f'Holding: {each[0]}, holding amount {each[1]:.4f}, buy price {each[2]:.6g}U, now price {each[3]:.6g}U, profit {each[4]:.3f}U, {each[5]:.2%} |')
        logger.info(f'Holding profit {float_profit:.3f}U, Usable money {usdt:.3f}U')
        logger.info(f'Day profit {day_profit:.3f}U, Month profit {month_profit:.3f}U, All profit: {all_profit:.3f}')
