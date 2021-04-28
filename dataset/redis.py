import redis
import time
from utils import config, user_config

RHOST = config.get('setting', 'RHost')
RPORT = config.getint('setting', 'RPort')
RPASSWORD = user_config.get('setting', 'RPassword')

class Redis(redis.StrictRedis):
    def __init__(self, host=RHOST, port=RPORT,
                db=0, password=RPASSWORD, socket_timeout=None,
                socket_connect_timeout=None,
                socket_keepalive=None, socket_keepalive_options=None,
                connection_pool=None, unix_socket_path=None,
                encoding='utf-8', encoding_errors='strict',
                charset=None, errors=None,
                decode_responses=False, retry_on_timeout=False,
                ssl=False, ssl_keyfile=None, ssl_certfile=None,
                ssl_cert_reqs='required', ssl_ca_certs=None,
                ssl_check_hostname=False,
                max_connections=None, single_connection_client=False,
                health_check_interval=0, client_name=None, username=None):
        super().__init__(host=host, port=port, db=db, password=password,
                        socket_timeout=socket_timeout, 
                        socket_connect_timeout=socket_connect_timeout,
                        socket_keepalive=socket_keepalive,
                        socket_keepalive_options=socket_keepalive_options,
                        connection_pool=connection_pool, unix_socket_path=unix_socket_path,
                        encoding=encoding, encoding_errors=encoding_errors,
                        charset=charset, errors=errors, decode_responses=decode_responses,
                        retry_on_timeout=retry_on_timeout, ssl=ssl, ssl_keyfile=ssl_keyfile,
                        ssl_certfile=ssl_certfile, ssl_cert_reqs=ssl_cert_reqs,
                        ssl_ca_certs=ssl_ca_certs, ssl_check_hostname=ssl_check_hostname,
                        max_connections=max_connections, single_connection_client=single_connection_client,
                        health_check_interval=health_check_interval, client_name=client_name,
                        username=username)

    def scan_iter_with_data(self, match: str, count: int):
        cursor = '0'
        while cursor != 0:
            cursor, keys = self.scan(cursor, match, count)
            values = self.mget(keys)
            if keys and values:
                yield keys, values

    def write_trade(self, symbol: str, data):
        self.mset({
            f'trade_{symbol}_{each.ts}_{i}' : f'{each.ts},{each.price},{each.amount},{each.direction}'
            for i, each in enumerate(reversed(data))
        })

    def write_target(self, symbol):
        now_str = time.strftime('%Y-%m-%d-%H', time.localtime())
        name = f'target_{now_str}'
        targets = self.get(name)
        targets = targets.decode('utf-8') if targets else ''
        
        if not targets:
            self.set(name, symbol)
        elif symbol not in targets:
            self.set(name, ','.join([targets, symbol]))
