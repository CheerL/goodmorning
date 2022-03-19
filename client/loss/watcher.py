from utils import logger, config, datetime, user_config, parallel, get_rate, get_level
from dataset.pgsql import get_holding_symbol
from dataset.redis import Redis
from retry import retry
from target import LossTarget as Target
from dataset.pgsql import LossTarget as TargetSQL
from report import wx_tmr_target_report
import time

TEST = user_config.getboolean('setting', 'TEST')

MIN_LOSS_RATE = config.getfloat('loss', 'MIN_LOSS_RATE')
BREAK_LOSS_RATE = config.getfloat('loss', 'BREAK_LOSS_RATE')
UP_LOSS_RATE = config.getfloat('loss', 'UP_LOSS_RATE')
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

UP_STOP_CONT_LOSS_RATE = config.getfloat('loss', 'UP_STOP_CONT_LOSS_RATE')
UP_STOP_SMALL_LOSS_RATE = config.getfloat('loss', 'UP_STOP_SMALL_LOSS_RATE')
UP_BREAK_LOSS_RATE = config.getfloat('loss', 'UP_BREAK_LOSS_RATE')
UP_STOP_SMALL_MIN_VOL = config.getfloat('loss', 'UP_STOP_SMALL_MIN_VOL')


BAN_LIST = ['SLPUSDT']
LEVEL = config.get('loss', 'LEVEL')

class LossWatcherClient:
    def __init__(self, user) -> None:
        self.user = user
        self.market = user.market
        self.client_type = 'loss_watcher'
        self.targets = []
        self.special_symbols = SPECIAL_SYMBOLS.split(',')
        self.level_coff, self.level_ts = get_level(LEVEL)
        logger.info('Start loss watcher.')
        self.redis = Redis()
        self.state = 0
        self.get_targets()
        
    def get_targets(self):
        while True:
            try:
                self.redis.ping()
                break
            except Exception as e:
                logger.error(e)
                time.sleep(5)
                self.redis = Redis()

        new_targets = get_holding_symbol()
        for symbol in set(self.targets) - set(new_targets):
            self.redis.delete(f'Binance_price_{symbol}')

        self.targets = new_targets

    def update_target_price(self):
        if not self.targets:
            return
        elif len(self.targets) == 1:
            symbol = self.targets[0]
            try:
                ticker = self.user.market.get_market_tickers(symbol=symbol, raw=True)
                price = float(ticker['price'])
                self.redis.set(f'Binance_price_{symbol}', price)
            except:
                pass
        else:
            try:
                tickers = self.user.market.get_market_tickers(raw=True)
                for ticker in tickers:
                    if ticker['symbol'] in self.targets:
                        symbol = ticker['symbol']
                        price = float(ticker['price'])
                        self.redis.set(f'Binance_price_{symbol}', price)
            except:
                pass
    
    def wait_state(self, state=1):
        while self.state != state:
            time.sleep(0.1)

    def is_buy(self, klines, symbol=''):
        if len(klines) <= MIN_BEFORE_DAYS*self.level_coff and symbol not in self.special_symbols:
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
    
        if (MIN_PRICE <= kline.close <= MAX_PRICE) and (
            (
                kline.close > boll
                and cont_loss_days==1
                and cont_loss <= UP_LOSS_RATE
                and MIN_VOL <= kline.vol <= MAX_VOL
            ) or (
                kline.close > boll
                and cont_loss_days > 2
                and cont_loss <= UP_STOP_CONT_LOSS_RATE
                # and rate == min_loss
                and rate >= UP_STOP_SMALL_LOSS_RATE
                and kline.vol >= UP_STOP_SMALL_MIN_VOL
            ) or (
                kline.close > boll
                and cont_loss <= UP_BREAK_LOSS_RATE
                and cont_loss_days > 1
                and MIN_VOL <= kline.vol <= MAX_VOL 
            ) or (
                kline.close <= boll
                and cont_loss <= BREAK_LOSS_RATE
                and MIN_VOL <= kline.vol <= MAX_VOL 
            ) or (
                kline.close <= boll
                and rate == max_loss
                and cont_loss <= MIN_LOSS_RATE
                and MIN_VOL <= kline.vol <= MAX_VOL 
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
                klines = self.market.get_candlestick(symbol, LEVEL, min_before_days*self.level_coff+end+1)[end:]
            except Exception as e:
                logger.error(f'[{symbol}]  {e}')
                raise e

            if symbol not in BAN_LIST and (symbol in ori_symbols or self.is_buy(klines, symbol)):
                kline = klines[0]
                target = Target(symbol, datetime.ts2level_hour(kline.id, self.level_ts), kline.open, kline.close, kline.vol)
                target.his_close = [each.close for each in klines[:20]]
                target.his_close_tmp = (end == 0)

                target.set_boll()
                if now - kline.id > self.level_ts and not TEST:
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
        date = datetime.ts2level_hour(now - end * self.level_ts, self.level_ts)
        logger.info(f'Targets of {self.user.username} in {date} are {",".join(targets.keys())}')
        return targets, date

    def tmr_targets(self):
        targets, _ = self.find_targets(end=0)
        wx_tmr_target_report(self.user.wxuid, ", ".join(targets.keys()))
