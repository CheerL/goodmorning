import redis
from sqlalchemy import Column, create_engine, VARCHAR, INTEGER, REAL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from utils import config, user_config

RHOST = config.get('setting', 'RHost')
RPORT = config.getint('setting', 'RPort')
RPASSWORD = user_config.get('setting', 'RPassword')

PGHOST = config.get('setting', 'PGHost')
PGPORT = config.getint('setting', 'PGPort')
PGPASSWORD = user_config.get('setting', 'PGPassword')
PGNAME = 'goodmorning'

Base = declarative_base()

class Trade(Base):
    __tablename__ = 'trade'
    id = Column(INTEGER, primary_key=True)
    symbol = Column(VARCHAR(10))
    ts = Column(VARCHAR(15))
    price = Column(REAL)
    amount = Column(REAL)
    direction = Column(VARCHAR(5))

    @staticmethod
    def get(key, value):
        key = key.decode('utf-8')
        value = value.decode('utf-8')
        symbol = key.split('_')[1]
        ts, price, amount, direction = value.split(',')
        return Trade(
            symbol=symbol,
            ts=ts,
            price=float(price),
            amount=float(amount),
            direction = direction
        )

class Target(Base):
    __tablename__ = 'target'
    id = Column(INTEGER, primary_key=True)
    tm = Column(VARCHAR(15))
    targets = Column(VARCHAR(500))

    @staticmethod
    def get(key, value):
        key = key.decode('utf-8')
        targets = value.decode('utf-8')
        tm = key.split('_')[1]
        return Target(
            tm=tm,
            targets=targets
        )

class Profit(Base):
    __tablename__ = 'profit'
    id = Column(INTEGER, primary_key=True)
    account = Column(VARCHAR(20))
    month = Column(VARCHAR(20))
    time = Column(REAL)
    pay = Column(REAL)
    income = Column(REAL)
    profit = Column(REAL)
    percent = Column(REAL)

def get_redis_conn():
    redis_conn = redis.StrictRedis(host=RHOST, port=RPORT, db=0, password=RPASSWORD)
    return redis_conn

def get_pgsql_session():
    engine = create_engine(f'postgresql://postgres:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGNAME}')
    session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return session

def write_trade(redis_conn: redis.Redis, session):
    cursor = '0'
    while cursor != 0:
        cursor, keys = redis_conn.scan(cursor, 'trade_*', 500)
        values = redis_conn.mget(keys)
        if keys and values:
            trades = [Trade.get(key, value) for key, value in zip(keys, values)]
            session.add_all(trades)
            session.commit()
            redis_conn.delete(*keys)

def write_target(redis_conn: redis.Redis, session):
    keys = redis_conn.keys('target_*')
    values = redis_conn.mget(keys)
    targets = [Target.get(key, value) for key, value in zip(keys, values)]
    session.add_all(targets)
    session.commit()
    redis_conn.delete(*keys)

def main():
    redis_conn = get_redis_conn()
    Session = get_pgsql_session()

    with Session() as session:
        write_target(redis_conn, session)
        write_trade(redis_conn, session)

if __name__ == '__main__':
    main()
