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

def replace_watch_dog(is_auto_connect=True, heart_beat_limit_ms=HEART_BEAT_MS, reconnect_after_ms=RECONNECT_MS, restart_ms=RESTART_MS, restart_range=RESTART_RANGE):
    old_watch_dog = SubscribeClient.subscribe_watch_dog
    [job] = old_watch_dog.scheduler.get_jobs()
    job.pause()

    watch_dog = WatchDog(is_auto_connect, heart_beat_limit_ms, reconnect_after_ms, restart_ms, restart_range)
    SubscribeClient.subscribe_watch_dog = watch_dog
    return watch_dog

class WatchDog(WebSocketWatchDog):
    websocket_manage_dict = dict()
    callback_dict = dict()

    def __init__(self, is_auto_connect=True, heart_beat_limit_ms=HEART_BEAT_MS, reconnect_after_ms=RECONNECT_MS, restart_ms=RESTART_MS, restart_range=RESTART_RANGE):
        threading.Thread.__init__(self)
        self.is_auto_connect = is_auto_connect
        self.heart_beat_limit_ms = heart_beat_limit_ms
        self.reconnect_after_ms = reconnect_after_ms
        self.restart_ms = restart_ms
        self.restart_range = restart_range
        self.scheduler = Scheduler()
        self.scheduler.add_job(self.check_reconnect, "interval", max_instances=1, seconds=1)
        self.start()

    def get_random_restart_at(self, wm):
        return wm.created_at + self.restart_ms + hash(wm) % self.restart_range

    def on_connection_closed(self, wm):
        self.mutex.acquire()
        self.websocket_manage_list.remove(wm)
        [name] = [name for name, wm in self.websocket_manage_dict.items() if wm == wm]
        del self.websocket_manage_dict[name]
        self.mutex.release()

    def after_connection_created(self, names, callbacks=[]):
        now = get_current_timestamp()
        wms = [wm for wm in self.websocket_manage_list if wm not in self.websocket_manage_dict.values()]
        for i, (wm, name) in enumerate(zip(wms, names)):
            wm.created_at = now
            wm.restart_at = self.get_random_restart_at(wm)
            self.mutex.acquire()
            self.websocket_manage_dict[name] = wm
            self.mutex.release()
            self.callback_dict[name] = callbacks[i] if i < len(callbacks) else None

    def check_reconnect(self):
        for name, wm in self.websocket_manage_dict.items():
            now = get_current_timestamp()
            # if not hasattr(wm, 'created_at'):
            #     setattr(wm, 'created_at', ts)

            if wm.request.auto_close:  # setting auto close no need reconnect
                pass

            elif wm.state == ConnectionState.CONNECTED:
                if self.is_auto_connect:
                    if now > wm.last_receive_time + self.heart_beat_limit_ms:
                        logger.warning(f"[{name}] No response from server")
                        self.close_and_wait_reconnect(wm, now+self.reconnect_after_ms)

                    elif now > wm.restart_at:
                        logger.warning(f"[{name}] Regular close and wait reconnect")
                        self.close_and_wait_reconnect(wm, now+self.reconnect_after_ms)

            elif wm.state == ConnectionState.WAIT_RECONNECT:
                if now > wm.reconnect_at:
                    logger.warning(f"[{name}] Reconnect")
                    if self.callback_dict[name]:
                        callback, args = self.callback_dict[name]
                        threading.Timer(0, callback, args).start()

                    wm.state = ConnectionState.RECONNECTING
                    wm.re_connect()
                    wm.created_at = now
                    wm.restart_at = self.get_random_restart_at(wm)
                    repeat_connection = [
                        conn for conn, conn_wm in WEBSOCKET_CONNECTION_HANDLER.items()
                        if conn_wm == wm
                        and conn != wm.original_connection
                    ]
                    for conn in repeat_connection:
                        conn.close()
                        del WEBSOCKET_CONNECTION_HANDLER[conn]

            elif wm.state == ConnectionState.CLOSED_ON_ERROR:
                if self.is_auto_connect:
                    self.close_and_wait_reconnect(wm, now + self.reconnect_after_ms)

    def close_and_wait_reconnect(self, wm: WebsocketManage, delay_in_ms: int=0):
        if wm.original_connection is not None:
            now = get_current_timestamp()
            if delay_in_ms == 0:
                delay_in_ms = now + self.reconnect_after_ms

            wm.original_connection.close()
            del WEBSOCKET_CONNECTION_HANDLER[wm.original_connection]
            wm.original_connection = None
            wm.state = ConnectionState.WAIT_RECONNECT
            wm.reconnect_at = delay_in_ms