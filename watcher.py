import sys
import time
import datetime

from wampyapp import State, WatcherClient, WatcherMasterClient
from huobi.model.market.trade_detail_event import TradeDetailEvent
from huobi.model.market.trade_detail import TradeDetail
from retry import retry

from market import MarketClient
from utils import config, kill_all_threads, logger, user_config, test_config
from websocket_handler import replace_watch_dog, WatchDog
from target import Target

TEST = user_config.getboolean('setting', 'Test')
MIN_RATE = config.getfloat('buy', 'MIN_RATE')
MAX_RATE = config.getfloat('buy', 'MAX_RATE')
MIN_VOL = config.getfloat('buy', 'MIN_VOL')
BIG_VOL = config.getfloat('buy', 'BIG_VOL')
BUY_NUM = config.getint('buy', 'BUY_NUM')
MIN_STOP_PROFIT_HOLD_TIME = config.getfloat('time', 'MIN_STOP_PROFIT_HOLD_TIME')
BUY_BACK_RATE = config.getfloat('buy', 'BUY_BACK_RATE')
CLEAR_TIME = int(config.getfloat('time', 'CLEAR_TIME'))
STOP_BUY_TIME = config.getfloat('time', 'STOP_BUY_TIME')
LOW_STOP_PROFIT_TIME = config.getfloat('time', 'LOW_STOP_PROFIT_TIME')
WATCHER_TASK_NUM = config.getint('watcher', 'WATCHER_TASK_NUM')
GOOD_SYMBOL = config.get('buy', 'GOOD_SYMBOL').split(',')

BUY_BACK_RATE = BUY_BACK_RATE / 100

def check_buy_signal(client: WatcherClient, symbol, info, price, trade_time, now):
    if (
        info['vol'] > MIN_VOL
        and info['max_back'] < BUY_BACK_RATE
        and info['min_buy_price'] < price < info['max_buy_price']
    ):

        if not info['big']:
            logger.info(f'No big buy for {symbol}')
            return

        try:
            client.send_buy_signal(symbol, price, info['open'], trade_time, now)
        except Exception as e:
            logger.error(e)

def check_sell_signal(client: WatcherClient, target: Target, info, price, trade_time, now):
    if client.high_stop_profit and price > target.stop_profit_price and trade_time > client.target_time + MIN_STOP_PROFIT_HOLD_TIME:
        try:
            client.send_stop_profit_signal(target, price, trade_time, now)
            for target in client.targets.values():
                target.set_high_stop_profit(False)
        except Exception as e:
            logger.error(e)

    elif price < target.stop_loss_price and trade_time > target.min_stop_loss_hold_time:
        try:
            client.send_stop_loss_signal(target, price, trade_time, now)
        except Exception as e:
            logger.error(e)

def trade_detail_callback(symbol: str, client: WatcherClient, interval=300, redis=True):
    def warpper(event: TradeDetailEvent):
        if not event.data:
            return
        
        now = time.time()
        detail: TradeDetail = event.data[0]
        trade_time = detail.ts / 1000

        if client.state == State.RUNNING and trade_time > client.target_time:
            last = trade_time // interval
            price = detail.price
            vol = sum([each.price * each.amount for each in event.data])

            if last > info['last']:
                info['last'] = last
                info['open'] = event.data[-1].price
                info['vol'] = vol
                info['min_buy_price'] = info['open'] * (1 + MIN_RATE / 100)
                info['max_buy_price'] = info['open'] * (1 + MAX_RATE / 100)
                info['high'] = max(info['open'], price)
            else:
                info['vol'] += vol
                info['high'] = max(info['high'], price)
                info['max_back'] = max(info['max_back'], 1 - price / info['high'])
                
            if vol > BIG_VOL and not info['big']:
                info['big'] = True
                logger.info(f'Big buy: {symbol} {vol}USDT at {trade_time}')

            if symbol in client.targets:
                target = client.targets[symbol]
                
                if target.own:
                    check_sell_signal(client, target, info, price, trade_time, now)
                elif now > target.time + 2:
                    del client.targets[symbol]
                    client.redis_conn.del_target(symbol)

            if (
                symbol not in client.targets 
                and (trade_time < client.target_time + STOP_BUY_TIME or symbol in GOOD_SYMBOL)
                and len(client.targets) < BUY_NUM
            ):
                check_buy_signal(client, symbol, info, price, trade_time, now)

        if redis:
            client.redis_conn.write_trade(symbol, event.data)


    info = {
        'last': 0,
        'vol': 0,
        'open': 0,
        'high': 0,
        'min_buy_price': 0,
        'max_buy_price': 0,
        'max_back': 0,
        'big': False
    }
    return warpper

def error_callback(symbol):
    def warpper(error):
        logger.error(f'[{symbol}] {error}')
    
    return warpper

def update_symbols(client: WatcherClient, watch_dog: WatchDog):
    new_symbols, _ = client.market_client.update_symbols_info()
    if new_symbols:
        logger.info(f'Find new symbols: {", ".join(new_symbols)}')
        for i, symbol in enumerate(new_symbols):
            client.market_client.sub_trade_detail(
                symbol, trade_detail_callback(symbol, client), error_callback(symbol)
            )
            watch_dog.after_connection_created(symbol)
            if not i % 10:
                time.sleep(0.5)

@retry(tries=5, delay=1, logger=logger)
def init_watcher(Client=WatcherClient) -> WatcherClient:
    market_client = MarketClient()
    client = Client(market_client)
    client.start()
    return client

def main():
    print(GOOD_SYMBOL)
    is_master = len(sys.argv) > 1 and sys.argv[1] == 'master'
    is_wait_stop = len(sys.argv) <= 1 or sys.argv[1] != 'nowait'
    is_redis = is_wait_stop
    watch_dog = replace_watch_dog()

    if is_master:
        logger.info('Master watcher')
        client : WatcherMasterClient = init_watcher(WatcherMasterClient)
        client.get_task(WATCHER_TASK_NUM)
        if TEST:
            now = datetime.datetime.now()
            run_time = now + datetime.timedelta(seconds=5)
            clear_time = run_time + datetime.timedelta(seconds=CLEAR_TIME)
            stop_time = clear_time + datetime.timedelta(seconds=10)
            end_time = stop_time + datetime.timedelta(seconds=5)
            watch_dog.scheduler.add_job(client.running, trigger='cron', hour=run_time.hour, minute=run_time.minute, second=run_time.second)
            watch_dog.scheduler.add_job(client.clear, trigger='cron', hour=clear_time.hour, minute=clear_time.minute, second=clear_time.second)
            watch_dog.scheduler.add_job(client.stop_running, trigger='cron', hour=stop_time.hour, minute=stop_time.minute, second=stop_time.second)
            watch_dog.scheduler.add_job(client.stopping, trigger='cron', hour=end_time.hour, minute=end_time.minute, second=end_time.second)
        else:
            watch_dog.scheduler.add_job(client.running, trigger='cron', hour=23, minute=59, second=30)
            watch_dog.scheduler.add_job(client.clear, trigger='cron', hour=0, minute=0, second=CLEAR_TIME)
            watch_dog.scheduler.add_job(client.stop_running, trigger='cron', hour=0, minute=0, second=CLEAR_TIME+10)
            watch_dog.scheduler.add_job(client.stopping, trigger='cron', hour=23, minute=56, second=0)
            watch_dog.scheduler.add_job(update_symbols, trigger='cron', minute='*/5', kwargs={'client': client, 'watch_dog': watch_dog})
        client.starting()
    else:
        logger.info('Sub watcher')
        client : WatcherClient = init_watcher(WatcherClient)
        client.wait_state(State.STARTED)
        client.get_task(WATCHER_TASK_NUM)

    if not client.task:
        return

    logger.info(f'Watcher task are: {", ".join(client.task)}')
    for i, symbol in enumerate(client.task):
        client.market_client.sub_trade_detail(
            symbol, trade_detail_callback(symbol, client, redis=is_redis), error_callback
        )
        watch_dog.after_connection_created(symbol)
        if not i % 10:
            time.sleep(0.5)

    client.wait_state(State.RUNNING)
    client.wait_state(State.STARTED)
    if is_wait_stop:
        client.wait_state(State.STOPPED)

    client.stop()
    kill_all_threads()
    logger.info('Watcher stop')

if __name__ == '__main__':
    main()
