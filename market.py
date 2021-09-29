import re

from huobi.client.generic import GenericClient
from huobi.client.market import MarketClient as HuobiMarketClient
from huobi.model.generic.symbol import Symbol

from utils import timeout_handle


class MarketClient(HuobiMarketClient):
    exclude_list = [
        'htusdt', 'btcusdt', 'bsvusdt', 'bchusdt', 'etcusdt',
        'ethusdt', 'botusdt','mcousdt','lendusdt','venusdt',
        'yamv2usdt', 'bttusdt', 'dogeusdt', 'shibusdt',
        'filusdt', 'xrpusdt', 'trxusdt', 'nftusdt',
        'thetausdt', 'dotusdt', 'eosusdt', 'maticusdt',
        'linkusdt', 'adausdt', 'jstusdt', 'vetusdt', 'xmxusdt',
        'newusdt', 'uipusdt', 'smtusdt'
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.generic_client = GenericClient()
        self.all_symbol_info: 'dict[str, Symbol]' = {}
        self.symbols_info: 'dict[str, Symbol]' = {}
        self.mark_price: 'dict[str, float]' = {}
        self.update_symbols_info()

    def exclude(self, infos, base_price) -> 'dict[str, Symbol]':
        return {
            symbol: info
            for symbol, info in infos.items()
            if symbol not in self.exclude_list
            and symbol in base_price
            and base_price[symbol] < 10
        }

    def get_all_symbols_info(self):
        return {
            info.symbol: info
            for info in self.generic_client.get_exchange_symbols()
            if info.symbol.endswith('usdt')
            and not re.search('\d', info.symbol)
            and info.symbol not in [
                'bchausdt', 'mcousdt', 'borusdt',
                'venusdt', 'botusdt', 'lendusdt'
            ]
        }

    def update_symbols_info(self) -> 'tuple[list[str], list[str]]':
        new_symbols_info = self.get_all_symbols_info()
        price = self.get_price()
        symbols_info = self.exclude(new_symbols_info, price)
        new_symbols = [symbol for symbol in symbols_info.keys() if symbol not in self.symbols_info]
        removed_symbols = [symbol for symbol in self.symbols_info.keys() if symbol not in symbols_info]
        self.symbols_info = symbols_info
        self.mark_price = price
        return new_symbols, removed_symbols

    @timeout_handle({})
    def get_price(self) -> 'dict[str, float]':
        return {
            pair.symbol: pair.close
            for pair in self.get_market_tickers()
        }

    @timeout_handle({})
    def get_vol(self) -> 'dict[str, float]':
        return {
            pair.symbol: pair.vol
            for pair in self.get_market_tickers()
        }
