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
    f"""
SELECT
  DIV(CAST(ts AS BIGINT), {interval * 1000}) * {interval} AS time,
  MAX(price) AS high,
  MIN(price) AS low,
  SUM(amount * price) AS vol,
  SUM(CASE WHEN ts IN (SELECT MIN(ts) FROM trade{mark}
    WHERE ts > '{start}' AND ts < '{end}' AND symbol='{symbol}'
    GROUP BY DIV(CAST(ts AS BIGINT), {interval * 1000})) THEN price ELSE 0 END) AS open,
  SUM(CASE WHEN ts IN (SELECT MAX(ts) FROM trade{mark}
    WHERE ts > '{start}' AND ts < '{end}' AND symbol='{symbol}'
    GROUP BY DIV(CAST(ts AS BIGINT), {interval * 1000})) THEN price ELSE 0 END) AS close,
  MIN(ts) AS start,
  MAX(ts) AS end,
  COUNT(price) AS count
FROM trade{mark}
WHERE ts > '{start}' AND ts < '{end}' AND symbol='{symbol}'
GROUP BY time
"""

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
