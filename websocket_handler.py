import threading

from apscheduler.schedulers.gevent import GeventScheduler as Scheduler
# from apscheduler.schedulers.blocking import BlockingScheduler as Scheduler
from huobi.connection.impl.private_def import ConnectionState
from huobi.connection.impl.websocket_manage import WebsocketManage
from huobi.connection.impl.websocket_manage import websocket_connection_handler as WEBSOCKET_CONNECTION_HANDLER
from huobi.connection.impl.websocket_watchdog import WebSocketWatchDog
from huobi.connection.subscribe_client import SubscribeClient
from huobi.utils.time_service import get_current_timestamp

from utils import logger

HEART_BEAT_MS = 30000
RECONNECT_MS = 32000
RESTART_MS = 6215790
RESTART_RANGE = 600000
ConnectionState.RECONNECTING = 6

def replace_watch_dog():
    old_watch_dog = SubscribeClient.subscribe_watch_dog
    [job] = old_watch_dog.scheduler.get_jobs()
    job.pause()

    watch_dog = WatchDog()
    SubscribeClient.subscribe_watch_dog = watch_dog
    return watch_dog

def close_and_wait_reconnect(wm: WebsocketManage, delay_in_ms: int):
    if wm.original_connection is not None:
        wm.original_connection.close()
        del WEBSOCKET_CONNECTION_HANDLER[wm.original_connection]
        wm.original_connection = None
        wm.state = ConnectionState.WAIT_RECONNECT
        wm.reconnect_at = delay_in_ms

def check_reconnect(watch_dog: 'WatchDog'):
    for name, websocket_manage in watch_dog.websocket_manage_dict.items():
        ts = get_current_timestamp()
        if not hasattr(websocket_manage, 'created_at'):
            setattr(websocket_manage, 'created_at', ts)

        if websocket_manage.request.auto_close:  # setting auto close no need reconnect
            pass

        elif websocket_manage.state == ConnectionState.CONNECTED:
            if watch_dog.is_auto_connect:
                if ts > websocket_manage.last_receive_time + watch_dog.heart_beat_limit_ms:
                    watch_dog.logger.warning(f"[{name}] No response from server")
                    close_and_wait_reconnect(websocket_manage, watch_dog.wait_reconnect_millisecond())

                elif ts > watch_dog.get_random_restart_at(websocket_manage):
                    close_and_wait_reconnect(websocket_manage, ts+100)

        elif websocket_manage.state == ConnectionState.WAIT_RECONNECT:
            if ts > websocket_manage.reconnect_at:
                watch_dog.logger.warning(f"[{name}] Reconnect")
                websocket_manage.state = ConnectionState.RECONNECTING
                websocket_manage.re_connect()
                websocket_manage.created_at = ts
                repeat_connection = [
                    conn for conn, wm in WEBSOCKET_CONNECTION_HANDLER.items()
                    if wm == websocket_manage
                    and conn != wm.original_connection
                ]
                for conn in repeat_connection:
                    conn.close()
                    del WEBSOCKET_CONNECTION_HANDLER[conn]

        elif websocket_manage.state == ConnectionState.CLOSED_ON_ERROR:
            if watch_dog.is_auto_connect:
                close_and_wait_reconnect(websocket_manage, ts + watch_dog.reconnect_after_ms)

class WatchDog(WebSocketWatchDog):
    websocket_manage_dict = dict()

    def __init__(self, is_auto_connect=True, heart_beat_limit_ms=HEART_BEAT_MS, reconnect_after_ms=RECONNECT_MS, restart_ms=RESTART_MS):
        threading.Thread.__init__(self)
        self.is_auto_connect = is_auto_connect
        self.heart_beat_limit_ms = heart_beat_limit_ms
        self.reconnect_after_ms = reconnect_after_ms if reconnect_after_ms > heart_beat_limit_ms else heart_beat_limit_ms
        self.restart_ms = restart_ms
        self.logger = logger
        self.scheduler = Scheduler()
        self.scheduler.add_job(check_reconnect, "interval", max_instances=1, seconds=1, args=[self])
        self.start()

    def get_random_restart_at(self, wm):
        return wm.created_at + self.restart_ms + hash(wm) % RESTART_RANGE

    def on_connection_closed(self, websocket_manage):
        self.mutex.acquire()
        self.websocket_manage_list.remove(websocket_manage)
        [name] = [name for name, wm in self.websocket_manage_dict.items() if wm == websocket_manage]
        del self.websocket_manage_dict[name]
        self.mutex.release()

    def after_connection_created(self, name):
        [wm] = [wm for wm in self.websocket_manage_list if wm not in self.websocket_manage_dict.values()]
        self.mutex.acquire()
        self.websocket_manage_dict[name] = wm
        self.mutex.release()
