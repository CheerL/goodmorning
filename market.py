import time

from huobi.client.generic import GenericClient
from huobi.client.market import MarketClient as _MarketClient
from huobi.constant import CandlestickInterval
# from huobi.constant import *
# from huobi.utils import *

from utils import config, logger, timeout_handle
from target import Target

BEFORE = config.getint('setting', 'Before')
BOOT_RATE = config.getfloat('setting', 'BootRate')
END_RATE = config.getfloat('setting', 'EndRate')
AFTER = config.getint('setting', 'After')
BATCH_SIZE = config.getint('setting', 'Batchsize')
MIN_VOL = config.getfloat('setting', 'MinVol')

class MarketClient(_MarketClient):
    exclude_list = ['htusdt', 'btcusdt', 'bsvusdt', 'bchusdt', 'etcusdt', 'ethusdt', 'botusdt','mcousdt','lendusdt','venusdt','yamv2usdt']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.midnight = False
        self.boot_rate = BOOT_RATE
        self.end_rate = END_RATE
        self.min_vol = MIN_VOL
        generic_client = GenericClient()

        self.symbols_info = {
            info.symbol: info
            for info in generic_client.get_exchange_symbols()
            if info.symbol.endswith('usdt') and info.symbol not in self.exclude_list
        }
        self.targets = {}
        

    def exclude_expensive(self, base_price):
        self.symbols_info = {
            symbol: info
            for symbol, info in self.symbols_info.items()
            if symbol in base_price
            and base_price[symbol][0] < 10
        }

    @timeout_handle({})
    def get_price(self) -> 'dict[str, tuple[float, float]]':
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
            if self.end_rate > item[2] > self.boot_rate
            and item[0] not in self.targets.keys()
        ][:BATCH_SIZE]

        if big_increase:
            big_increase_vol = [self.get_vol(item[0]) for item in big_increase]
            big_increase = [
                (symbol, close, increment, vol)
                for ((symbol, close, increment, _), vol) in zip(big_increase, big_increase_vol)
                if vol > self.min_vol
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
            self.targets[symbol] = Target(symbol, now_price)
            logger.info(f'Find target: {symbol.upper()}, initial price {init_price}, now price {now_price} , increase {target_increase}%, vol {vol} USDT')
        return targets

    # def get_target(self, target_time, base_price, base_price_time=None, change_base=True, max_wait=MAX_WAIT, unstop=False):
    #     targets = []
    #     while True:
    #         now = time.time()
    #         if now <= target_time+0.5:
    #             continue

    #         increase, price = self.get_increase(base_price)
    #         big_increase = self.get_big_increase(increase)

    #         if big_increase:
    #             targets = self.handle_big_increase(big_increase, base_price)
    #             break
    #         elif not unstop and now > target_time + max_wait:
    #             logger.warning(f'Fail to find target in {max_wait}s')
    #             break
    #         elif unstop and now > target_time + UNSTOP_MAX_WAIT:
    #             logger.warning(f'Fail to find target in {UNSTOP_MAX_WAIT}s, end unstop model')
    #             break
    #         else:
    #             logger.info('\t'.join([
    #                 f'{index+1}. {symbol.upper()} {increment}% {vol} USDT'
    #                 for index, (symbol, _, increment, vol) in enumerate(increase[:3])
    #             ]))
    #             if change_base and now - base_price_time > AFTER:
    #                 base_price_time = now
    #                 base_price = price
    #                 logger.info('User now base price')
    #             time.sleep(0.03)

    #     return targets

    @staticmethod
    def _percent_modify(t):
        return max(min(0.5 * t, 0.9), 0.5)
