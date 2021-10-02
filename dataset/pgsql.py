import re
from utils import datetime, config, user_config, datetime
from sqlalchemy import Column, create_engine, VARCHAR, INTEGER, REAL, TEXT, func, Table
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base

PGHOST = config.get('data', 'PGHost')
PGPORT = config.getint('data', 'PGPort')
PGUSER = user_config.get('setting', 'PGUser')
PGPASSWORD = user_config.get('setting', 'PGPassword')
PGNAME = user_config.get('setting', 'PGDatabase')

Base = declarative_base()
TRADE_CLASS = {}
MS_IN_DAY = 60*60*24*1000

def create_Trade(day):
    class Trade(Base):
        __tablename__ = f'trade_{day}' if day else 'trade'
        id = Column(INTEGER, primary_key=True)
        symbol = Column(VARCHAR(20))
        ts = Column(VARCHAR(20))
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

        @staticmethod
        def from_trade(trade):
            return Trade(
                symbol=trade.symbol,
                ts=trade.ts,
                price=trade.price,
                amount=trade.amount,
                direction=trade.direction
            )

    Base.metadata.create_all()
    TRADE_CLASS[day] = Trade
    return Trade

def get_Trade(time):
    day = get_day(time)

    if day in TRADE_CLASS:
        return TRADE_CLASS[day]
    else:
        return create_Trade(day)

def get_day(time):
    if 0 <= time < 50000:
        return time
    elif 1e9 < time < 1e10:
        return time * 1000 // MS_IN_DAY
    elif 1e12 < time < 1e13:
        return time // MS_IN_DAY

def get_trade_from_redis(key, value):
    key = key.decode('utf-8')
    value = value.decode('utf-8')
    _, symbol, _, num = key.split('_')
    ts, price, amount, direction = value.split(',')
    ts = int(ts)
    num = int(num)
    day = int(ts // MS_IN_DAY)
    Trade = get_Trade(day)

    return Trade(
        symbol=symbol,
        ts=str(ts+num/1000),
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
    def from_redis(key, value):
        key = key.decode('utf-8')
        targets = value.decode('utf-8')
        tm = key.split('_')[1]
        return Target(
            tm=tm,
            targets=targets
        )

class LossTarget(Base):
    __tablename__ = 'loss_target'
    id = Column(INTEGER, primary_key=True)
    date = Column(VARCHAR(20))
    symbol = Column(VARCHAR(30))
    exchange = Column(VARCHAR(30))
    open = Column(REAL)
    close = Column(REAL)
    high = Column(REAL)
    low = Column(REAL)
    vol = Column(REAL)

    @classmethod
    def add_target(cls, **kwargs):
        with get_session() as session:
            target = session.query(cls).filter(
                cls.symbol==kwargs['symbol'],
                cls.date==kwargs['date'],
                cls.exchange==kwargs['exchange']
            ).first()
            if not target:
                session.add(cls(**kwargs))
                session.commit()

    @classmethod
    def get_targets(cls, conditions=[]):
        with get_session() as session:
            target = session.query(cls).filter(*conditions).order_by(
                cls.date,
                cls.symbol
            )
            return target

    @classmethod
    def get_target(cls, conditions=[]):
        with get_session() as session:
            target = session.query(cls).filter(*conditions).first()
            return target


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


class Order(Base):
    __tablename__ = 'order'
    id = Column(INTEGER, primary_key=True)
    order_id = Column(VARCHAR(50))
    symbol = Column(VARCHAR(100))
    tm = Column(VARCHAR(50))
    date = Column(VARCHAR(20))
    account = Column(VARCHAR(200))
    direction = Column(VARCHAR(10))
    aver_price = Column(REAL)
    amount = Column(REAL)
    vol = Column(REAL)
    finished = Column(INTEGER)

    @classmethod
    def add_order(cls, summary, date, account_id):
        with get_session() as session:
            order = cls(
                order_id=str(summary.order_id),
                symbol=summary.symbol,
                date=date,
                tm='',
                account=str(account_id),
                direction=summary.direction,
                aver_price=0,
                amount=0,
                vol=0,
                finished=0
            )
            session.add(order)
            session.commit()

    @classmethod
    def get_orders(cls, conditions=[]):
        with get_session() as session:
            return session.query(cls).filter(*conditions).order_by(cls.date, cls.direction)

    @classmethod
    def update(cls, conditions=[], load={}):
        with get_session() as session:
            session.query(cls).filter(*conditions).update(load)
            session.commit()

    @classmethod
    def get_profit(cls, account):
        with get_session() as session:
            today = datetime.ts2date()
            table = Table('order_result', Base.metadata, autoload=True, autoload_with=session.bind)
            data = session.query(table).filter(table.c.account==str(account)).all()
            day_profit = sum([each.profit for each in data if each.sell_tm.startswith(today)])
            month_profit = sum([each.profit for each in data if each.sell_tm.startswith(today[:7])])
            all_profit = sum([each.profit for each in data])
            return day_profit, month_profit, all_profit

def get_session(host=PGHOST, port=PGPORT, db=PGNAME, user=PGUSER, password=PGPASSWORD) -> Session:
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    Session = sessionmaker(bind=engine)
    Base.metadata.bind=engine
    Base.metadata.create_all()
    return Session()

def get_time_from_str(time):
    if isinstance(time, str):
        time = datetime.time2ts(time)

    return time

def get_ms_from_str(time):
    return get_time_from_str(time) * 1000

def get_trade_list(symbol, start, end):
    with get_session() as session:
        start_time = get_ms_from_str(start)
        end_time = get_ms_from_str(end)
        Trade = get_Trade(int(start_time))
        data = Trade.get_data(session, symbol, start_time, end_time).all()
        if not data:
            return []

        trade_list = [
            # {
            #     'ts': int(start_time),
            #     'price': data[0].price,
            #     'vol': 0,
            #     'acc_vol': 0
            # }
        ]

        last_ts = int(start_time)
        last_price = data[0].price
        sum_vol = 0
        acc_vol = 0
        for trade in data:
            ts = int(float(trade.ts))
            price = trade.price
            vol = round(price * trade.amount, 4)

            if last_ts != ts:
                trade_list.append({
                    'ts': last_ts,
                    'price': last_price,
                    'vol': round(sum_vol, 4),
                    'acc_vol': round(acc_vol, 4)
                })

                last_ts = ts
                last_price = price
                sum_vol = vol
                acc_vol += vol
            else:
                sum_vol += vol
                acc_vol += vol
                last_price = price

        else:
            trade_list.append({
                'ts': last_ts,
                'price': last_price,
                'vol': round(sum_vol, 4),
                'acc_vol': round(acc_vol, 4)
            })

        return trade_list if len(trade_list) > 1 else []


def get_open_price(symbol, start):
    with get_session() as session:
        start_time = get_ms_from_str(start)
        open_time = int(((start_time + MS_IN_DAY / 3) //
                        MS_IN_DAY - 1/3) * MS_IN_DAY)
        Trade = get_Trade(open_time)
        data = session.query(Trade).filter(
            Trade.symbol == symbol,
            Trade.ts >= str(open_time),
            Trade.ts < str(open_time + 30000)
        ).order_by(Trade.ts).first()
        return {'open': data.price if data else 1}


def get_profit(name='', month=''):
    with get_session() as session:
        profit_human = Table('profit_human', Base.metadata,
                             autoload=True, autoload_with=session.bind)
        data = session.query(profit_human)
        if name:
            data = data.filter(profit_human.c.name == name)
        if month:
            data = data.filter(profit_human.c.month == month)
        data = data.all()
        res = [{
            'key': index,
            'profit_id': item.id,
            'name': item.name,
            'date': item.date_str.strftime('%Y-%m-%d'),
            'profit': item.profit,
            'percent': item.percent
        } for index, item in enumerate(data)]
        return res


def get_month_profit(name='', month=''):
    with get_session() as session:
        month_profit = Table('month_profit', Base.metadata,
                             autoload=True, autoload_with=session.bind)
        data = session.query(month_profit)
        if name:
            data = data.filter(month_profit.c.name == name)
        if month:
            data = data.filter(month_profit.c.month == month)
        data = data.all()
        res = [{
            'key': index,
            'name': item.name,
            'month': item.month,
            'profit': item.profit,
            'percent': item.percent,
            'fee': item.fee
        } for index, item in enumerate(data)]
        return res


def get_message(date, name, profit=0):
    with get_session() as session:
        data = session.query(Message).filter(
            Message.summary.like(f'{date}%{name[:3]}%'))
        if len(data.all()) > 1:
            data = data.filter(Message.summary.like(f'%{profit}%'))

        if len(data.all()) == 0:
            return '未找到记录'

        data = data[0]
        res = re.findall(r'(### 买入记录\n\n.*)\n### 总结', data.msg, re.DOTALL)
        if res:
            res = res[0]+'\n'
            res = re.sub(date + r' (.+?)000', r'\1', res)
            res = re.sub(r'\|[^~\|]+?\|\n', r'|\n', res)
            res = re.sub(r'\| (\d*?\.\d{0,3})\d*? \|\n', r'| \1 |\n', res)
            res = re.sub(r'----', r':----:', res)
            return res
        else:
            return '未找到记录'


def get_currency_day_profit(currency='', date='', end_date=''):
    with get_session() as session:
        currency_day_profit = Table(
            'currency_day', Base.metadata, autoload=True, autoload_with=session.bind)
        data = session.query(currency_day_profit)
        if currency:
            data = data.filter(currency_day_profit.c.currency == currency)
        if end_date and date:
            data = data.filter(currency_day_profit.c.date <= end_date)
            data = data.filter(currency_day_profit.c.date >= date)
        elif date:
            data = data.filter(currency_day_profit.c.date == date)
        data = data.all()
        res = [{
            'key': index,
            'currency': item.currency,
            'date': item.date,
            'buy_tm': item.buy_tm,
            'sell_tm': item.sell_tm,
            'hold_tm': item.sell_tm - item.buy_tm,
            'buy': item.buy,
            'sell': item.sell,
            'profit': item.sell-item.buy,
            'percent': item.percent,
            'type': 1 if item.high_profit else (2 if item.high_loss else 0),
            'buy_price': item.buy_price,
            'sell_price': item.sell_price
            # 0 for normal, 1 for high profit, 2 for high loss.
        } for index, item in enumerate(data)]
        return res


def get_record(profit_id='', currency='', date=''):
    with get_session() as session:
        record_human = Table('record_human', Base.metadata,
                             autoload=True, autoload_with=session.bind)
        data = session.query(record_human)
        if profit_id:
            data = data.filter(record_human.c.profit_id == profit_id)
        if currency:
            data = data.filter(record_human.c.currency == currency)
        if date:
            data = data.filter(record_human.c.date == date)
        data = data.all()
        res = [{
            'key': index,
            'name': item.name,
            'currency': item.currency,
            'date': item.date,
            'time': item.tm,
            'price': item.price,
            'amount': item.amount,
            'vol': round(item.vol, 2),
            'direction': item.direction,
        } for index, item in enumerate(data)]
        return res


def get_stat():
    with get_session() as session:
        stat = Table('currency_stat', Base.metadata,
                     autoload=True, autoload_with=session.bind)
        data = session.query(stat)
        data = data.all()
        res = [{
            'key': index,
            'currency': item.currency,
            'buy_times': item.buy_times,
            'profit_times': item.profit_times,
            'high_profit_times': item.high_profit_times,
            'high_loss_times': item.high_loss_times,
            'total_profit': item.total_profit,
            'total_percent': item.total_percent,
            'profit_percent': item.profit_percent,
            'high_profit_percent': item.high_profit_percent,
            'high_loss_percent': item.high_loss_percent
        } for index, item in enumerate(data)]
        return res