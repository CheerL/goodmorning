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
RESTART_MS = 621500
RESTART_RANGE = 60000
ConnectionState.RECONNECTING = 6

def replace_watch_dog(is_auto_connect=True, heart_beat_limit_ms=HEART_BEAT_MS, reconnect_after_ms=RECONNECT_MS, restart_ms=RESTART_MS):
    old_watch_dog = SubscribeClient.subscribe_watch_dog
    [job] = old_watch_dog.scheduler.get_jobs()
    job.pause()

    watch_dog = WatchDog(is_auto_connect, heart_beat_limit_ms, reconnect_after_ms, restart_ms)
    SubscribeClient.subscribe_watch_dog = watch_dog
    return watch_dog

class WatchDog(WebSocketWatchDog):
    websocket_manage_dict = dict()
    callback_dict = dict()

    def __init__(self, is_auto_connect=True, heart_beat_limit_ms=HEART_BEAT_MS, reconnect_after_ms=RECONNECT_MS, restart_ms=RESTART_MS):
        threading.Thread.__init__(self)
        self.is_auto_connect = is_auto_connect
        self.heart_beat_limit_ms = heart_beat_limit_ms
        self.reconnect_after_ms = reconnect_after_ms
        self.restart_ms = restart_ms
        self.scheduler = Scheduler()
        self.scheduler.add_job(self.check_reconnect, "interval", max_instances=1, seconds=1)
        self.start()

    def get_random_restart_at(self, wm):
        return wm.created_at + self.restart_ms + hash(wm) % RESTART_RANGE

    def on_connection_closed(self, websocket_manage):
        self.mutex.acquire()
        self.websocket_manage_list.remove(websocket_manage)
        [name] = [name for name, wm in self.websocket_manage_dict.items() if wm == websocket_manage]
        del self.websocket_manage_dict[name]
        self.mutex.release()

    def after_connection_created(self, names, callbacks=[]):
        wms = [wm for wm in self.websocket_manage_list if wm not in self.websocket_manage_dict.values()]
        for i, (wm, name) in enumerate(zip(wms, names)):
            self.mutex.acquire()
            self.websocket_manage_dict[name] = wm
            self.mutex.release()
            self.callback_dict[name] = callbacks[i] if i < len(callbacks) else None

    def check_reconnect(self):
        for name, websocket_manage in self.websocket_manage_dict.items():
            ts = get_current_timestamp()
            if not hasattr(websocket_manage, 'created_at'):
                setattr(websocket_manage, 'created_at', ts)

            if websocket_manage.request.auto_close:  # setting auto close no need reconnect
                pass

            elif websocket_manage.state == ConnectionState.CONNECTED:
                if self.is_auto_connect:
                    if ts > websocket_manage.last_receive_time + self.heart_beat_limit_ms:
                        logger.warning(f"[{name}] No response from server")
                        self.close_and_wait_reconnect(websocket_manage, ts+self.reconnect_after_ms)

                    elif ts > self.get_random_restart_at(websocket_manage):
                        logger.warning(f"[{name}] Regular close and wait reconnect")
                        self.close_and_wait_reconnect(websocket_manage, ts+self.reconnect_after_ms)

            elif websocket_manage.state == ConnectionState.WAIT_RECONNECT:
                if ts > websocket_manage.reconnect_at:
                    logger.warning(f"[{name}] Reconnect")
                    if self.callback_dict[name]:
                        callback, args = self.callback_dict[name]
                        threading.Timer(0, callback, args).start()

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
                if self.is_auto_connect:
                    self.close_and_wait_reconnect(websocket_manage, ts + self.reconnect_after_ms)


    def close_and_wait_reconnect(self, wm: WebsocketManage, delay_in_ms: int=0):
        if wm.original_connection is not None:
            if delay_in_ms == 0:
                delay_in_ms = get_current_timestamp()+self.reconnect_after_ms

            wm.original_connection.close()
            del WEBSOCKET_CONNECTION_HANDLER[wm.original_connection]
            wm.original_connection = None
            wm.state = ConnectionState.WAIT_RECONNECT
            wm.reconnect_at = delay_in_ms