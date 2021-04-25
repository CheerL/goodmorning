import redis
from sqlalchemy import Column, create_engine, VARCHAR, INTEGER, REAL, TEXT, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from utils import config, user_config

RHOST = config.get('setting', 'RHost')
RPORT = config.getint('setting', 'RPort')
RPASSWORD = user_config.get('setting', 'RPassword')

PGHOST = config.get('setting', 'PGHost')
PGPORT = config.getint('setting', 'PGPort')
PGUSER = 'postgres'
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
    def from_redis(key, value):
        key = key.decode('utf-8')
        value = value.decode('utf-8')
        _, symbol, _, num = key.split('_')
        ts, price, amount, direction = value.split(',')
        return Trade(
            symbol=symbol,
            ts=str(int(ts)+int(num)/1000),
            price=float(price),
            amount=float(amount),
            direction = direction
        )

    @staticmethod
    def get_data(session, symbol, start, end):
        data = session.query(Trade).filter(
            Trade.symbol == symbol,
            Trade.ts >= str(start),
            Trade.ts <= str(end)
        ).order_by(Trade.ts)
        return data

class Target(Base):
    __tablename__ = 'target'
    id = Column(INTEGER, primary_key=True)
    tm = Column(VARCHAR(15))
    targets = Column(VARCHAR(500))

    @staticmethod
    def from_redis(key, value):
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

    @staticmethod
    def get_sum_profit(session, account, month=None):
        result = session.query(func.sum(Profit.profit)).filter(Profit.account == str(account))
        if month:
            result = result.filter(Profit.month == month)
        return result.scalar()

    @staticmethod
    def get_id(session, account, pay, income):
        return session.execute(f"SELECT id FROM profit WHERE account = '{account}' AND pay = '{pay}' AND income = '{income}'").scalar()

class Record(Base):
    __tablename__ = 'record'
    id = Column(INTEGER, primary_key=True)
    profit_id = Column(INTEGER)
    currency = Column(VARCHAR(10))
    tm = Column(VARCHAR(40))
    price = Column(REAL)
    amount = Column(REAL)
    vol = Column(REAL)
    fee = Column(REAL)
    direction = Column(VARCHAR(5))

    @staticmethod
    def from_record_info(infos, profit_id, direction):
        records = [Record(
            profit_id=profit_id,
            currency=record_info['currency'],
            tm=record_info['time'],
            price=record_info['price'],
            amount=record_info['amount'],
            vol=record_info['vol'],
            fee=record_info['fee'],
            direction=direction
        ) for record_info in infos]
        return records

class Message(Base):
    __tablename__ = 'message'
    id = Column(INTEGER, primary_key=True)
    summary = Column(VARCHAR(100))
    msg = Column(TEXT)
    msg_type = Column(INTEGER)
    uids = Column(VARCHAR(200))

def get_redis_conn(host=RHOST, port=RPORT, password=RPASSWORD, db=0):
    redis_conn = redis.StrictRedis(host=host, port=port, db=db, password=password)
    return redis_conn

def get_pgsql_session(host=PGHOST, port=PGPORT, db=PGNAME, user=PGUSER, password=PGPASSWORD):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return Session()

def write_trade(redis_conn: redis.Redis, session):
    cursor = '0'
    while cursor != 0:
        cursor, keys = redis_conn.scan(cursor, 'trade_*', 500)
        values = redis_conn.mget(keys)
        if keys and values:
            trades = [Trade.from_redis(key, value) for key, value in zip(keys, values)]
            session.add_all(trades)
            session.commit()
            redis_conn.delete(*keys)

def write_target(redis_conn: redis.Redis, session):
    keys = redis_conn.keys('target_*')
    if keys:
        values = redis_conn.mget(keys)
        targets = [Target.from_redis(key, value) for key, value in zip(keys, values)]
        session.add_all(targets)
        session.commit()
        redis_conn.delete(*keys)

def main():
    redis_conn = get_redis_conn()

    with get_pgsql_session() as session:
        write_target(redis_conn, session)
        write_trade(redis_conn, session)

if __name__ == '__main__':
    main()
