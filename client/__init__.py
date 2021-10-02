import time

from threading import Timer
from user import BaseUser as User
from retry import retry
from utils import logger
from utils.logging import quite_logger

class BaseDealerClient:
    def __init__(self, user: User, *args, **kwargs):
        self.market_client = user.market_client
        self.targets = {}
        self.user = user
        self.client_type = 'base dealer'
        self.state = 0

    @classmethod
    @retry(tries=5, delay=1, logger=logger)
    def init_dealer(cls, user):
        client = cls(user)
        Timer(2, quite_logger, kwargs={'all_logger':True}).start()
        return client

    def wait_state(self, state=1):
        while self.state != state:
            time.sleep(0.1)