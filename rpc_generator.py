import thriftpy2
import os
from utils import ROOT, config
from thriftpy2.rpc import make_server, make_client


thrift_path = os.path.join(ROOT, 'protocol.thrift')
thrift = thriftpy2.load(thrift_path)
WATCHER_HOST = config.get('setting', 'WatcherHost')
WATCHER_PORT = config.getint('setting', 'WatcherPort')


def get_watcher_client():
    client = make_client(thrift.Watcher, WATCHER_HOST, WATCHER_PORT)
    return client

def get_watcher_server(handler):
    server = make_server(thrift.Watcher, handler, '0.0.0.0', WATCHER_PORT)
    return server
