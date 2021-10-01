import math

from threading import Timer
from market import MarketClient
from dataset.redis import Redis
from target import MorningTarget as Target
from utils import config, get_target_time, logger

from wampy.roles.subscriber import subscribe
from wampy.roles.callee import callee
from client.wampy import ControlledClient, Topic, State, WS_URL

WATCHER_TASK_NUM = config.getint('watcher', 'WATCHER_TASK_NUM')
IOC_BATCH_NUM = config.getint('sell', 'IOC_BATCH_NUM')
IOC_INTERVAL = config.getfloat('time', 'IOC_INTERVAL')

class MorningWatcherClient(ControlledClient):
    def __init__(self, user, url=WS_URL):
        super().__init__(url=url)
        self.market_client = user.market_client
        self.client_type = 'watcher'
        self.high_stop_profit = True
        self.task : list[str] = []
        self.targets : dict[str, Target] = {}
        self.redis_conn: Redis = Redis()

    def get_task(self, num) -> 'list[str]':
        self.task = self.rpc.req_task(num)

    def send_buy_signal(self, symbol, price, init_price, trade_time, now):
        if now > trade_time + 0.2:
            logger.info(f'Buy. {symbol} with price {price}USDT at {trade_time}. Recieved at {now}. Too late')
            return

        self.publish(topic=Topic.BUY_SIGNAL, symbol=symbol, price=price, init_price=init_price, now=trade_time)
        self.market_client.symbols_info[symbol].init_price = init_price
        target = Target(symbol, price, now, self.high_stop_profit)
        target.set_info(self.market_client.symbols_info[symbol])
        self.targets[symbol] = target
        logger.info(f'Buy. {symbol} with price {price}USDT at {trade_time}. recieved at {now}')
        self.redis_conn.write_target(symbol)

    def send_stop_loss_signal(self, target: Target, price, trade_time, now):
        self.publish(topic=Topic.STOP_LOSS, symbol=target.symbol, price=target.stop_loss_price)
        target.own = False
        logger.info(f'Stop loss. {target.symbol}: {price}USDT at {trade_time}. recieved at {now}')

    def send_stop_profit_signal(self, target: Target, price, trade_time, now):
        if not self.high_stop_profit:
            return

        self.publish(topic=Topic.STOP_PROFIT, status=False, symbol=target.symbol, price=target.stop_profit_price)
        target.own = False
        self.high_stop_profit = False
        logger.info(f'Stop profit. {target.symbol}: {price}USDT at {trade_time}. recieved at {now}')

    @subscribe(topic=Topic.STOP_PROFIT)
    def stop_profit_handler(self, status, *arg, **kwargs):
        self.high_stop_profit = status

    @subscribe(topic=Topic.AFTER_BUY)
    def after_buy_handler(self, symbol, price, *args, **kwargs):
        if symbol not in self.targets:
            return

        if price == 0:
            # del self.targets[symbol]
            pass
        else:
            self.targets[symbol].set_buy_price(price)


class MorningWatcherMasterClient(MorningWatcherClient):
    def __init__(self, market_client: MarketClient, url=WS_URL):
        super().__init__(market_client=market_client, url=url)
        self.client_info = {
            'watcher': 0,
            'dealer': 0
        }
        vols = self.market_client.get_vol()
        self.symbols : list[str] = sorted(
            self.market_client.symbols_info.keys(),
            key=lambda symbol: vols[symbol],
            reverse=True
        )
        k = math.ceil(len(self.symbols) / WATCHER_TASK_NUM)

        self.tasks = [[] for _ in range(k)]
        tasks_vol = [0] * k
        for symbol in self.symbols:
            i = tasks_vol.index(min(tasks_vol))
            self.tasks[i].append(symbol)
            tasks_vol[i] += vols[symbol]

    def set_state(self, state):
        self.state = state
        self.publish(topic=Topic.STATE, state=state)

    def set_time(self, target_time):
        self.target_time = target_time
        self.publish(topic=Topic.TIME, target_time=target_time)

    def after_start(self):
        self.info_handler(self.client_type)

    def before_stop(self):
        self.info_handler(self.client_type, True)

    def get_task(self, num=0):
        self.task = self.req_task(num)

    @callee
    def req_task(self, num=0) -> 'list[str]':
        task = self.tasks[0]
        self.tasks = self.tasks[1:]
        return task

    @subscribe(topic=Topic.CLIENT_INFO)
    def info_handler(self, client_type, remove=False, *args, **kwargs):
        if remove:
            self.client_info[client_type] -= 1
            if client_type == 'dealer' and self.client_info['dealer'] == 0:
                self.stop_running()
        else:
            self.client_info[client_type] += 1
        
            self.publish(topic=Topic.STATE, state=self.state)
            self.publish(topic=Topic.TIME, target_time=self.target_time)

    def starting(self):
        if self.state == State.STOPPED:
            self.set_state(State.STARTED)
            logger.info(f"Change state to started")

    def running(self):
        if self.state != State.RUNNING:
            self.set_state(State.RUNNING)
            self.set_time(get_target_time())
            logger.info(f"Change state to running")

    def stopping(self):
        if self.state != State.STOPPED:
            self.set_state(State.STOPPED)
            logger.info(f"Change state to stopped")

    def stop_running(self):
        if self.state == State.RUNNING:
            self.set_state(State.STARTED)
            logger.info(f"Change state to not running")

    def clear(self, num=IOC_BATCH_NUM):
        [_, targets] = self.redis_conn.get_target()
        logger.info(f'Start clear, targets are {targets}')
        if targets:
            targets = targets.split(',')
            for i in range(num):
                Timer(
                    IOC_INTERVAL * i,
                    self.clear_handler,
                    [targets, i]
                ).start()

    def clear_handler(self, targets, count):
        data = [(symbol, self.redis_conn.get_new_price(symbol)) for symbol in targets]
        self.publish(topic=Topic.CLEAR, data=data, count=count)
        logger.info(f'Start ioc clear for round {count+1}, data={data}')