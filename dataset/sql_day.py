from dataset.pgsql import get_session, get_Trade, Session
from sqlalchemy import func
import time

MS_IN_DAY = 60*60*24*1000

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

def delete_many(session: Session, ids: 'list[int]'):
    ids_str = ','.join(ids)
    sql = f'DELETE FROM trade WHERE id IN ({ids_str})'
    session.execute(sql)

def main():
    now_day = int(time.time() * 1000 // MS_IN_DAY)

    with get_session() as session:
        Trade = get_Trade(0)
        min_ts = float(session.query(func.min(Trade.ts)).scalar())
        min_day = int(min_ts // MS_IN_DAY)
        # min_day = 18745

        for day in range(min_day, now_day+1):
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
                delete_many(session, [str(trade.id) for trade in trades])
                session.commit()

if __name__ == '__main__':
    main()
