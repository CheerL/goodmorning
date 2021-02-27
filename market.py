import time

from huobi.client.generic import GenericClient
from huobi.client.market import MarketClient as _MarketClient
from huobi.constant import *
from huobi.utils import *

from utils import config, logger, timeout_handle

BEFORE = config.getint('setting', 'Before')
BOOT_PRECENT = config.getfloat('setting', 'BootPrecent')
END_PRECENT = config.getfloat('setting', 'EndPrecent')
AFTER = config.getint('setting', 'After')
BATCH_SIZE = config.getint('setting', 'Batchsize')
MAX_AFTER = config.getint('setting', 'MaxAfter')
MIN_VOL = config.getfloat('setting', 'MinVol')
MIDNIGHT_MIN_VOL = config.getfloat('setting', 'MidnightMinVol')

class MarketClient(_MarketClient):
    exclude_list = ['htusdt', 'btcusdt', 'bsvusdt', 'bchusdt', 'etcusdt', 'ethusdt']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.midnight = False
        generic_client = GenericClient()

        self.symbols_info = {
            info.symbol: info
            for info in generic_client.get_exchange_symbols()
            if info.symbol.endswith('usdt') and info.symbol not in self.exclude_list
        }
        self.target_symbol = []

    @timeout_handle({})
    def get_price(self):
        market_data = self.get_market_tickers()
        price = {
            pair.symbol: (pair.close, pair.vol)
            for pair in market_data
            if pair.symbol in self.symbols_info
        }
        return price

    @timeout_handle(0)
    def get_vol(self, symbol):
        [kline] = self.get_candlestick(symbol, CandlestickInterval.MIN1, 1)
        return kline.vol

    def get_base_price(self, target_time):
        while True:
            now = time.time()
            if now < target_time - 310:
                logger.info('Wait 5mins')
                time.sleep(300)
            else:
                base_price = self.get_price()
                if now > target_time - BEFORE:
                    base_price_time = now
                    logger.info(f'Get base price successfully')
                    break
                else:
                    time.sleep(1)
        
        return base_price, base_price_time

    def get_increase(self, initial_price):
        price = self.get_price()
        increase = [
            (
                symbol, close,
                round((close - initial_price[symbol][0]) / initial_price[symbol][0] * 100, 4),
                round(vol - initial_price[symbol][1], 4)
            )
            for symbol, (close, vol) in price.items()
        ]
        increase = sorted(increase, key=lambda pair: pair[2], reverse=True)
        return increase, price

    def get_big_increase(self, increase):
        big_increase = [
            item for item in increase
            if END_PRECENT > item[2] > BOOT_PRECENT
            and item[0] not in self.target_symbol
        ][:BATCH_SIZE]

        if big_increase:
            big_increase_vol = [self.get_vol(item[0]) for item in big_increase]
            big_increase = [
                (symbol, close, increment, vol)
                for ((symbol, close, increment, _), vol) in zip(big_increase, big_increase_vol)
                if vol > (MIDNIGHT_MIN_VOL if self.midnight else MIN_VOL)
            ]

        return big_increase

    def handle_big_increase(self, big_increase, base_price):
        targets = []
        for symbol, now_price, target_increase, vol in big_increase:
            init_price, _ = base_price[symbol]
            target = self.symbols_info[symbol]
            target.buy_price = now_price
            target.init_price = init_price
            targets.append(target)
            self.target_symbol.append(symbol)
            logger.info(f'Find target: {symbol.upper()}, initial price {init_price}, now price {now_price} , increase {target_increase}%, vol {vol} USDT')
        return targets

    def get_target(self, target_time, base_price, base_price_time=None, change_base=True, interval=MAX_AFTER, unstop=False):
        targets = []
        while True:
            now = time.time()
            increase, price = self.get_increase(base_price)
            big_increase = self.get_big_increase(increase)

            if big_increase:
                targets = self.handle_big_increase(big_increase, base_price)
                break
            elif not unstop and now > target_time + interval:
                logger.warning(f'Fail to find target in {interval}s')
                break
            elif unstop and now > target_time + 60:
                logger.warning(f'Fail to find target in 60s, end unstop model')
                break
            else:
                logger.info('\t'.join([
                    f'{index+1}. {symbol.upper()} {increment}% {vol} USDT'
                    for index, (symbol, _, increment, vol) in enumerate(increase[:3])
                ]))
                if change_base and now - base_price_time > AFTER:
                    base_price_time = now
                    base_price = price
                    logger.info('User now base price')
                time.sleep(0.05)

        return targets

    @staticmethod
    def _precent_modify(t):
        return max(min(0.5 * t, 0.9), 0.5)
