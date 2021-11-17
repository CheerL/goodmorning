from utils import logger
from dataset.pgsql import get_holding_symbol
from dataset.redis import Redis
import time

class LossWatcherClient:
    def __init__(self, user) -> None:
        self.user = user
        self.client_type = 'loss_watcher'
        self.targets = []
        logger.info('Start loss watcher.')
        self.redis = Redis()
        self.state = 0
        self.get_targets()
        
    def get_targets(self):
        new_targets = get_holding_symbol()
        for symbol in set(self.targets) - set(new_targets):
            self.redis.delete(f'Binance_price_{symbol}')

        self.targets = new_targets

    def update_target_price(self):
        if not self.targets:
            return
        elif len(self.targets) == 1:
            symbol = self.targets[0]
            ticker = self.user.market.get_market_tickers(symbol=symbol, raw=True)
            price = float(ticker['price'])
            self.redis.set(f'Binance_price_{symbol}', price)
        else:
            tickers = self.user.market.get_market_tickers(raw=True)
            for ticker in tickers:
                if ticker['symbol'] in self.targets:
                    symbol = ticker['symbol']
                    price = float(ticker['price'])
                    self.redis.set(f'Binance_price_{symbol}', price)
    
    def wait_state(self, state=1):
        while self.state != state:
            time.sleep(0.1)