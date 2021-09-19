import time

from threading import Timer
from retry import retry
from market import MarketClient
from target import Target, LossTarget
from utils import config, logger, user_config, get_rate
from utils.parallel import run_thread_pool
from order import OrderSummary, OrderSummaryStatus

from wampy.roles.subscriber import subscribe
from client import ControlledClient, Topic, State, WS_URL
from user import User

STOP_PROFIT_SLEEP = config.getfloat('time', 'STOP_PROFIT_SLEEP')
REPORT_PRICE = user_config.getboolean('setting', 'REPORT_PRICE')
MIN_LOSS_RATE = config.getfloat('loss', 'MIN_LOSS_RATE')
BREAK_LOSS_RATE = config.getfloat('loss', 'BREAK_LOSS_RATE')
AVER_LENGTH = config.getfloat('loss', 'AVER_LENGTH')


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
    def __init__(self, market_client: MarketClient, user: User, url=WS_URL):
        super().__init__(market_client=market_client, user=user, url=url)
        self.targets: dict[str, LossTarget] = {}
        self.client_type = 'loss_dealer'
        self.record_list = []

    # @subscribe(topic=Topic.BUY_SIGNAL)
    # def buy_signal_handler(self, symbol, price, init_price, now, *args, **kwargs):
    #     pass

    def after_buy(self, symbol, summary: OrderSummary):
        if self.targets[symbol].buy_price:
            return

        if summary.aver_price != 0:
            self.targets[symbol].set_buy_price(summary.aver_price, summary.vol)


    def check_target(self, target: LossTarget):
        print(target.price, target.high_mark_price, target.high_mark_back_price, target.high_mark)
        if target.high_mark:
            if target.own and target.price <= target.high_mark_back_price:
                self.sell_limit_target(target, target.high_mark_back_price)
                return
        elif target.price >= target.high_mark_price:
            target.high_mark = True
        
        if target.low_mark:
            if target.own and target.price <= target.sell_price:
                self.sell_limit_target(target, target.sell_price, selling_level=2)
                return
        elif target.price >= target.low_mark_price:
            target.low_mark = True

    def find_targets(self, min_loss_rate=MIN_LOSS_RATE,
        break_loss_rate=BREAK_LOSS_RATE, end=0, min_before=180
    ):
        infos = self.market_client.get_all_symbols_info()
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
                target = LossTarget(symbol, kline.close, now, kline.open, kline.vol)
                target.set_info(infos[symbol])
                targets[symbol] = target
                return

            return
        
        run_thread_pool([(worker, (symbol,)) for symbol in symbols], True, 8)
        self.targets = self.user.filter_targets(targets)
        logger.info(f'Targets of {self.user.username} are {",".join(targets.keys())}')

    def watch_targets(self, aver_length=AVER_LENGTH):
        def worker():
            time_list = []
            
            while True:
                tickers = self.market_client.get_market_tickers()
                now = time.time()
                time_list.append(now)
                for start, each in enumerate(time_list.copy()):
                    if each < now - aver_length:
                        time_list.pop(0)
                    else:
                        break

                for target in self.targets.values():
                    target.update_price(tickers, start)
                    self.check_target(target)

                time.sleep(0.5)

        Timer(0, worker).start()

    def buy_limit_target(self, target: LossTarget, buy_amount=0, limit_rate=0):
        @retry(tries=5, delay=0.05)
        def filled_callback(summary):
            self.after_buy(target.symbol, summary)
            if summary.aver_price <=0:
                logger.error(f'Fail to buy {target.symbol}')
                return

        @retry(tries=5, delay=0.05)
        def cancel_callback(summary):
            self.after_buy(target.symbol, summary)

            if target.keep_buy:
                self.buy_limit_target(target, summary.created_vol - summary.vol)

        if not buy_amount:
            buy_amount = float(self.user.buy_amount)

        buy_price = target.get_target_buy_price(rate=limit_rate)
        summary = self.user.buy_limit(target, buy_amount, buy_price)
        if summary != None:
            summary.check_after_buy(self, 600)
            summary.add_filled_callback(filled_callback, [summary])
            summary.add_cancel_callback(cancel_callback, [summary])
        else:
            logger.error(f'Failed to buy {target.symbol}')

    def sell_limit_target(
        self, target: LossTarget, price,
        sell_amount=None, selling_level=1
    ):
        @retry(tries=5, delay=0.05)
        def filled_callback(summary):
            target.selling = 0
            target.own = False

        @retry(tries=5, delay=0.05)
        def cancel_callback(summary=None):
            target.selling = 0
            if summary:
                sell_amount = min(self.user.get_amount(target.base_currency), summary.remain_amount)
                assert sell_amount - 0.9 * summary.remain_amount > 0, "Not yet arrived"
            else:
                sell_amount = self.user.get_amount(target.base_currency)
            self.sell_limit_target(target, price, sell_amount, selling_level)

        if target.selling >= selling_level:
            return
        elif target.selling == 0:
            target.selling = selling_level
            if not sell_amount:
                sell_amount = self.user.get_amount(target.base_currency)

            summary = self.user.sell_limit(target, sell_amount, price)
            if summary != None:
                # summary.check_after_buy(self, 60)
                summary.add_filled_callback(filled_callback, [summary])
                # summary.add_cancel_callback(cancel_callback, [summary])
            else:
                logger.error(f'Failed to sell {target.symbol}')
        else:
            self.cancel_and_sell(target, cancel_callback)

    def cancel_and_sell(self, target, callback):
        symbol = target.symbol
        is_canceled = False
        
        if symbol in self.user.orders['sell']:
            for summary in self.user.sorders['sell'][symbol]:
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
