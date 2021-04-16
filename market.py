import time

from huobi.client.generic import GenericClient
from huobi.client.market import MarketClient as _MarketClient

from utils import timeout_handle


class MarketClient(_MarketClient):
    exclude_list = [
        'htusdt', 'btcusdt', 'bsvusdt', 'bchusdt', 'etcusdt',
        'ethusdt', 'botusdt','mcousdt','lendusdt','venusdt',
        'yamv2usdt', 'bttusdt', 'dogeusdt'
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        generic_client = GenericClient()

        self.symbols_info = {
            info.symbol: info
            for info in generic_client.get_exchange_symbols()
            if info.symbol.endswith('usdt') and info.symbol not in self.exclude_list
        }

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


    @staticmethod
    def _percent_modify(t):
        return max(min(0.5 * t, 0.9), 0.5)
