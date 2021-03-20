import thriftpy2
import os
import time
from utils import ROOT, config
from thriftpy2.rpc import make_server, make_client
from thriftpy2.thrift import TClient
from thriftpy2.transport import TTransportException
from parallel import run_thread, kill_this_thread
import socket

WATCHER_HOST = config.get('setting', 'WatcherHost')
WATCHER_PORT = config.getint('setting', 'WatcherPort')
DEALER_HOST = config.get('setting', 'DealerHost')
DEALER_PORT = config.getint('setting', 'DealerPort')

Error = (BrokenPipeError, socket.timeout, TTransportException, OSError)

thrift_path = os.path.join(ROOT, 'protocol.thrift')
thrift = thriftpy2.load(thrift_path)


def get_watcher_client():
    client = make_client(thrift.Watcher, WATCHER_HOST, WATCHER_PORT, timeout=10000)
    return client

def get_watcher_server(handler):
    server = make_server(thrift.Watcher, handler, '0.0.0.0', WATCHER_PORT, client_timeout=10000)
    return server

def get_dealer_clients():
    clients = [make_client(thrift.Dealer, host, DEALER_PORT, timeout=10000) for host in DEALER_HOST.split(',')]
    return clients

def get_dealer_server(handler):
    server = make_server(thrift.Dealer, handler, '0.0.0.0', DEALER_PORT, client_timeout=10000)
    return server

def keep_alive(client, timeout=1, is_lock=True):
    def _keep_alive():
        while True:
            try:
                client.alive()
                time.sleep(timeout)
            except Error:
                # print(e)
                if hasattr(client, 'close'):
                    client.close()
                kill_this_thread()

    run_thread([(_keep_alive, ())], is_lock=is_lock)

def close_server(server):
    server.trans.close()
    server.close()