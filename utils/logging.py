import logging
import sys
from logging.handlers import RotatingFileHandler

FMT = "%(asctime)s {%(processName)s} [%(pathname)s:%(lineno)d] %(name)s-%(levelname)s: %(message)s"

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
    formatter = logging.Formatter(FMT)
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)

    if log_file:
        fileHandler = RotatingFileHandler(filename=log_file, mode='a', maxBytes=10*1024*1024, backupCount=5)
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)

    return logger
