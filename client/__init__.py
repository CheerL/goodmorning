import time

from market import MarketClient
from user import BaseUser as User
from retry import retry
from utils import logger
from utils.logging import quite_logger

class BaseDealerClient:
    def __init__(self, market_client: MarketClient, user: User, *args, **kwargs):
        self.market_client : MarketClient = market_client
        self.targets = {}
        self.user = user
        self.client_type = 'base dealer'
        self.state = 0

    @classmethod
    @retry(tries=5, delay=1, logger=logger)
    def init_dealer(cls, user):
        market_client = MarketClient()
        quite_logger(all_logger=True)
        client = cls(market_client, user)
        return client

    def wait_state(self, state=1):
        while self.state != state:
            time.sleep(0.1)