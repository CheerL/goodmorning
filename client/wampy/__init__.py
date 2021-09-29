import time

from utils import config, logger

from wampy.constants import DEFAULT_REALM, DEFAULT_ROLES, DEFAULT_TIMEOUT
from wampy.peers.clients import Client
from wampy.roles.subscriber import subscribe

WS_HOST = config.get('data', 'WsHost')
WS_PORT = config.getint('data', 'WsPort')
WS_URL = f'ws://{WS_HOST}:{WS_PORT}'

class Topic:
    CLIENT_INFO = 'CLIENT_INFO'
    STATE = 'STATE'
    TIME = 'TIME'
    BUY_SIGNAL = 'BUY_SIGNAL'
    SELL_SIGNAL = 'SELL_SIGNAL'
    AFTER_BUY = 'AFTER_BUY'
    HIGH = 'HIGH'
    STOP_PROFIT = 'STOP_PROFIT'
    STOP_LOSS = 'STOP_LOSS'
    CLEAR = 'CLEAR'

class State:
    STOPPED = 0
    STARTED = 1
    RUNNING = 2


class ControlledClient(Client):
    def __init__(
        self, url=WS_URL, cert_path=None, ipv=4, name=None,
        realm=DEFAULT_REALM, roles=DEFAULT_ROLES, call_timeout=DEFAULT_TIMEOUT,
        message_handler_cls=None
    ):
        super().__init__(
            url=url, cert_path=cert_path, ipv=ipv, name=name,
            realm=realm, roles=roles, call_timeout=call_timeout,
            message_handler_cls=message_handler_cls
        )
        self.state = State.STOPPED
        self.target_time = None
        self.client_type = 'unknown'

    @subscribe(topic=Topic.STATE)
    def state_handler(self, state, *args, **kwargs):
        if self.state != state:
            self.state = state
            logger.info(f"Change state to {['stopped', 'started', 'running'][self.state]}")

    @subscribe(topic=Topic.TIME)
    def time_handler(self, target_time,  *args, **kwargs):
        if self.target_time != target_time:
            self.target_time = target_time
            logger.info(f'Change target time {self.target_time}')

    def wait_state(self, state=State.STARTED):
        while self.state != state:
            time.sleep(0.1)

    def start(self):
        super().start()
        self.after_start()

    def after_start(self):
        self.publish(topic=Topic.CLIENT_INFO, client_type=self.client_type, remove=False)

    def stop(self):
        self.before_stop()
        try:
            super().stop()
        except Exception as e:
            logger.error(e)

    def before_stop(self):
        self.publish(topic=Topic.CLIENT_INFO, client_type=self.client_type, remove=True)
