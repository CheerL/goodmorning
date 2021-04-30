from dataset.redis import Redis
from dataset.pgsql import get_Trade, get_session, Session, Target, get_trade_from_redis, get_day, MS_IN_DAY
from sqlalchemy import func, inspect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import argparse

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

def vacuum(session: Session=None, table: str='', full=True):
    def _vacuum():
        engine = session.bind
        connection = engine.raw_connection() 
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
        cursor = connection.cursor() 
        cursor.execute(f"VACUUM {'FULL' if full else ''} {table}")
        cursor.close()
        connection.close()

    if session:
        _vacuum()
    else:
        with get_session() as session:
            _vacuum()

def delete_many(session: Session, table:str, ids: 'list[int]'):
    ids_str = ','.join(ids)
    sql = f'DELETE FROM {table} WHERE id IN ({ids_str})'
    session.execute(sql)

def move_trade(Trade, session, start_day, end_day):
    for day in range(start_day, end_day):
        print(day)
        DayTrade = get_Trade(day)
        start = str(day * MS_IN_DAY)
        end = str((day + 1) * MS_IN_DAY)

        while True:
            trades = session.query(Trade).filter(Trade.ts >= start, Trade.ts < end).limit(1000).all()
            print(len(trades))
            if not trades:
                break

            new_trades = [DayTrade.from_trade(trade) for trade in trades]
            session.bulk_save_objects(new_trades)
            delete_many(session, Trade.__tablename__, [str(trade.id) for trade in trades])
            session.commit()

    vacuum(session, Trade.__tablename__, False)

def check_trade_tables():
    with get_session() as session:
        inspector = inspect(session.bind)
        tables = [name for name in inspector.get_table_names() if name.startswith('trade')]
        print(tables)
        for table in tables:
            day = int(table.split('_')[1])
            Trade = get_Trade(day)
            print(day, Trade.__tablename__)
            min_ts = float(session.query(func.min(Trade.ts)).scalar())
            print(min_ts)
            if min_ts < day * MS_IN_DAY:
                print('move small')
                min_day = int(min_ts // MS_IN_DAY)
                move_trade(Trade, session, min_day, day)

            max_ts = float(session.query(func.max(Trade.ts)).scalar())
            print(max_ts)
            if max_ts > (day + 1) * MS_IN_DAY:
                print('move big')
                max_day = int(max_ts // MS_IN_DAY)
                move_trade(Trade, session, day+1, max_day+1)

        vacuum(session)

def reorder(days, symbols):
    with get_session() as session:
        for day in days.split(','):
            day = int(day)
            Trade = get_Trade(day)
            if symbols:
                symbols = symbols.split(',')
            else:
                symbols = [each[0] for each in session.execute(f'SELECT DISTINCT symbol FROM trade_{day}')]

            for symbol in symbols:
                ts = ''
                count = 0
                trades = session.execute(f"""
                SELECT id, ts
                FROM trade_{day} as tb
                WHERE tb.symbol='{symbol}'
                ORDER BY tb.ts ASC,
                CASE WHEN tb.direction='buy' THEN tb.price END ASC,
                CASE WHEN tb.direction='sell' THEN tb.price END DESC
                """)
                update_mappings = []
                for trade_id, trade_ts in trades:
                    if '.' in trade_ts:
                        continue

                    if ts == trade_ts:
                        count += 1
                    else:
                        count = 0
                    update_mappings.append({
                        'id': trade_id,
                        'ts': str(int(trade_ts)+count/1000)
                    })
                session.bulk_update_mappings(Trade, update_mappings)
                session.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', default='trans')
    parser.add_argument('-t', '--table', default='')
    parser.add_argument('-s', '--symbol', default='')
    parser.add_argument('-d', '--day', '')
    args = parser.parse_args()

    if args.command == 'trans':
        trans()
    elif args.command == 'vacuum':
        vacuum(table=args.table)
    elif args.command == 'check':
        check_trade_tables()
    elif args.command == 'reorder':
        reorder(args.day, args.symbol)

if __name__ == '__main__':
    main()
