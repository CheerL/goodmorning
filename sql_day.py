from pgsql import get_pgsql_session, Trade, Session
from sqlalchemy import Column, VARCHAR, INTEGER, REAL, func
import time
from sqlalchemy.ext.declarative import declarative_base
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

MS_IN_DAY = 60*60*24*1000
Base = declarative_base()

def vacuum(session: Session, table: str):
    engine = session.bind
    connection = engine.raw_connection() 
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
    cursor = connection.cursor() 
    cursor.execute(f"VACUUM FULL {table}")
    cursor.close()
    connection.close() 


def create_kline(symbol, start, end, interval=60):
    mark_day = start // MS_IN_DAY
    if mark_day == time.time() * 1000 // MS_IN_DAY:
        mark = ''
    else:
        mark = f'_{int(mark_day)}'
    f'''
SELECT
  DIV(CAST("ts" AS BIGINT), {interval * 1000}) * {interval} AS "time",
  MAX("price") AS "high",
  MIN("price") AS "low",
  SUM("amount" * "price") AS "vol",
  SUM(CASE WHEN "ts" IN (SELECT MIN("ts") FROM "trade{mark}"
    WHERE "ts" > '{start}' AND "ts" < '{end}' AND "symbol"='{symbol}'
    GROUP BY DIV(CAST("ts" AS BIGINT), {interval * 1000})) THEN "price" ELSE 0 END) AS "open",
  SUM(CASE WHEN "ts" IN (SELECT MAX("ts") FROM "trade{mark}"
    WHERE "ts" > '{start}' AND "ts" < '{end}' AND "symbol"='{symbol}'
    GROUP BY DIV(CAST("ts" AS BIGINT), {interval * 1000})) THEN "price" ELSE 0 END) AS "close",
  MIN("ts") AS "start",
  MAX("ts") AS "end",
  COUNT("price") AS "count"
FROM "trade{mark}"
WHERE "ts" > '{start}' AND "ts" < '{end}' AND "symbol"='{symbol}'
GROUP BY "{interval * 1000}"
'''

def order(limit=100, reverse=False):
    mark = ''
    f'''
    SELECT *
FROM trade{mark}
ORDER BY
  ts {'DESC' if reverse else 'ASC'},
  CASE WHEN direction='buy' THEN price END {'DESC' if not reverse else 'ASC'},
  CASE WHEN direction='sell' THEN price END {'DESC' if reverse else 'ASC'}
LIMIT {limit}'''

def create_table(session, day):
    class DayTrade(Base):
        __tablename__ = f'trade_{day}'
        id = Column(INTEGER, primary_key=True)
        symbol = Column(VARCHAR(10))
        ts = Column(VARCHAR(20))
        price = Column(REAL)
        amount = Column(REAL)
        direction = Column(VARCHAR(5))

        @staticmethod
        def from_trade(trade):
            return DayTrade(
                symbol=trade.symbol,
                ts=trade.ts,
                price=trade.price,
                amount=trade.amount,
                direction=trade.direction
            )

    Base.metadata.create_all(session.bind)
    return DayTrade

def delete_many(session: Session, ids: 'list[int]'):
    ids_str = ','.join(ids)
    sql = f'DELETE FROM trade WHERE id IN ({ids_str})'
    session.execute(sql)

def main():
    now_day = int(time.time() * 1000 // MS_IN_DAY)

    with get_pgsql_session() as session:
        min_ts = float(session.query(func.min(Trade.ts)).scalar())
        min_day = int(min_ts // MS_IN_DAY)
        # min_day = 18741

        for day in range(min_day, now_day):
            print(day)
            DayTrade = create_table(session, day)
            start = str(day * MS_IN_DAY)
            end = str((day + 1) * MS_IN_DAY)

            while True:
                trades = session.query(Trade).filter(Trade.ts >= start, Trade.ts < end).limit(1000).all()

                print(len(trades))
                if not trades:
                    vacuum(session, 'trade')
                    break

                new_trades = [DayTrade.from_trade(trade) for trade in trades]
                session.bulk_save_objects(new_trades)
                delete_many(session, [str(trade.id) for trade in trades])
                session.commit()

            

if __name__ == '__main__':
    main()
