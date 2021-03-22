import logging
import sys
from logging.handlers import RotatingFileHandler

from wxpusher.wxpusher import BASEURL, WxPusher as _WxPusher
import requests


def quite_logger(name=None, all_logger=False):
    if all_logger:
        root = logging.getLogger()
        for handler in root.handlers:
            root.removeHandler(handler)
        return

    logger = logging.getLogger(name)
    logger.setLevel(logging.WARNING)

def create_logger(name, log_file=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s-%(levelname)s: %(message)s")
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)

    if log_file:
        fileHandler = RotatingFileHandler(filename=log_file, mode='a', maxBytes=10*1024*1024, backupCount=5)
        # fileHandler.setLevel(logging.INFO)
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)

    return logger
