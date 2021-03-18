from huobi.model import market
import thriftpy2
from thriftpy2.rpc import make_client, make_server

from market import MarketClient
from utils import config, get_target_time, logger


SELL_INTERVAL = config.getfloat('setting', 'SellInterval')
SELL_AFTER = config.getfloat('setting', 'SellAfter')
MIDNIGHT = config.getboolean('setting', 'Midnight')
MIDNIGHT_INTERVAL = config.getfloat('setting', 'MidnightInterval')
MIDNIGHT_SELL_AFTER = config.getfloat('setting', 'MidnightSellAfter')
MIDNIGHT_MAX_WAIT = config.getfloat('setting', 'MidnightMaxWait')
MIDNIGHT_MIN_VOL = config.getfloat('setting', 'MidnightMinVol')
MIDNIGHT_BOOT_PERCENT = config.getfloat('setting', 'MidnightBootPercent')
MIDNIGHT_ONLY = [each == 'true' for each in config.get('setting', 'MidnightOnly').split(',')]

WATCHER_MODE = config.get('setting', 'WatcherMode')

def main():
    target_time = get_target_time()
    market_client = MarketClient()

    if WATCHER_MODE == 'master':
        server = make_server()

if __name__ == '__main__':
    main()
