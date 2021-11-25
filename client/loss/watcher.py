from utils import logger, config, datetime, user_config, parallel, get_rate
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

class LossWatcherClient:
    def __init__(self, user) -> None:
        self.user = user
        self.market = user.market
        self.client_type = 'loss_watcher'
        self.targets = []
        self.special_symbols = SPECIAL_SYMBOLS.split(',')
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
    
    def tmr_targets(self):
        targets, _ = self.find_targets(end=0)

        if targets:
            wx_tmr_target_report(self.user.wxuid, ", ".join(targets.keys()))
