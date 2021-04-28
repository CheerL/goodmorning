from dataset.redis import Redis
from dataset.pgsql import get_session, Session, Target, get_trade_from_redis, get_day
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

def check_trade_tables():
    with get_session() as session:
        tables = [name for name in session.bind.table_names() if name.startswith('trade')]

def main():
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == 'trans':
            trans()
        elif arg == 'vacuum':
            table = sys.argv[2] if len(sys.argv) > 2 else ''
            vacuum(table=table)

if __name__ == '__main__':
    main()