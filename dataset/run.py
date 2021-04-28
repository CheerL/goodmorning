from dataset.redis import Redis
from dataset.pgsql import get_Trade, get_session, Session, Target, get_trade_from_redis, get_day, MS_IN_DAY
from sqlalchemy import func
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

def write_trade(redis_conn: Redis, session: Session):
    for keys, values in redis_conn.scan_iter_with_data('trade_*', 500):
        trades = [get_trade_from_redis(key, value) for key, value in zip(keys, values)]
        session.add_all(trades)
        session.commit()
        redis_conn.delete(*keys)

def write_target(redis_conn: Redis, session: Session):
    keys = redis_conn.keys('target_*')
    if keys:
        values = redis_conn.mget(keys)
        targets = [Target.from_redis(key, value) for key, value in zip(keys, values)]
        session.add_all(targets)
        session.commit()
        redis_conn.delete(*keys)

def trans():
    redis_conn = Redis()
    with get_session() as session:
        write_target(redis_conn, session)
        write_trade(redis_conn, session)


def vacuum(session: Session=None, table: str=''):
    def _vacuum(table: str):
        engine = session.bind
        connection = engine.raw_connection() 
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
        cursor = connection.cursor() 
        cursor.execute(f"VACUUM FULL {table}")
        cursor.close()
        connection.close()
    
    if session:
        _vacuum(table)
    else:
        with get_session() as session:
            _vacuum(table)

def delete_many(session: Session, table:str, ids: 'list[int]'):
    ids_str = ','.join(ids)
    sql = f'DELETE FROM {table} WHERE id IN ({ids_str})'
    session.execute(sql)

def move_trade(Trade, session, start_day, end_day):
    for day in range(start_day, end_day):
        DayTrade = get_Trade(day)
        start = str(day * MS_IN_DAY)
        end = str((day + 1) * MS_IN_DAY)

        while True:
            trades = session.query(Trade).filter(Trade.ts >= start, Trade.ts < end).limit(1000).all()
            if not trades:
                vacuum(session, Trade.__tablename__)
                break

            new_trades = [DayTrade.from_trade(trade) for trade in trades]
            session.bulk_save_objects(new_trades)
            delete_many(session, Trade.__tablename__, [str(trade.id) for trade in trades])
            session.commit()

def check_trade_tables():
    with get_session() as session:
        tables = [name for name in session.bind.table_names() if name.startswith('trade')]
        for table in tables:
            day = int(table.split('_')[1])
            Trade = get_Trade(day)
            min_ts = float(session.query(func.min(Trade.ts)).scalar())
            if min_ts < day * MS_IN_DAY:
                min_day = int(min_ts // MS_IN_DAY)
                move_trade(Trade, session, min_day, day)

            max_ts = float(session.query(func.max(Trade.ts)).scalar())
            if max_ts > (day + 1) * MS_IN_DAY:
                max_day = int(max_ts // MS_IN_DAY)
                move_trade(Trade, session, day+1, max_day+1)

            vacuum(session, Trade.__tablename__)

def main():
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == 'trans':
            trans()
        elif arg == 'vacuum':
            table = sys.argv[2] if len(sys.argv) > 2 else ''
            vacuum(table=table)
        elif arg == 'check':
            check_trade_tables()

if __name__ == '__main__':
    main()