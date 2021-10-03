import time

from pytz import timezone

from user.base import BaseUser as User
from retry import retry
from utils import logger, datetime
from apscheduler.schedulers.background import BackgroundScheduler

class BaseDealerClient:
    def __init__(self, user: User, *args, **kwargs):
        self.market_client = user.market_client
        self.targets = {}
        self.user = user
        self.client_type = 'base dealer'
        self.report_scheduler = BackgroundScheduler(timezone=datetime.TZ_DICT[8])
        self.report_scheduler.start()
        self.state = 0

    @classmethod
    @retry(tries=5, delay=1, logger=logger)
    def init_dealer(cls, user):
        client = cls(user)
        return client

    def wait_state(self, state=1):
        while self.state != state:
            time.sleep(0.1)