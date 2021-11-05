import logging
import sys
from logging.handlers import RotatingFileHandler

FMT = "%(asctime)s {%(name)s} [%(module)s:%(lineno)d]-%(levelname)s: %(message)s"


def no_addHandler(*args, **kwargs):
    return

def getLogger(self, name):
    logger = self._getLogger(name)
    if name not in self.except_list:
        logger.addHandler = no_addHandler
        for handler in logger.handlers:
            logger.removeHandler(handler)
    return logger

def quite_logger(name=None, all_logger=False, except_list=[]):
    logger = logging.getLogger(name)
    logger.addHandler = no_addHandler
    for handler in logger.handlers:
        logger.removeHandler(handler)
        
    if all_logger:
        logging.lastResort.setLevel(100)
        logging.Manager.except_list = except_list
        logging.Manager._getLogger = logging.Manager.getLogger
        logging.Manager.getLogger = getLogger


def create_logger(name, log_file=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(FMT)
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)

    if log_file:
        fileHandler = RotatingFileHandler(filename=log_file, mode='a', maxBytes=10*1024*1024, backupCount=5)
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)

    return logger
