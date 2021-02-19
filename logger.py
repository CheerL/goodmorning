import logging
from logging.handlers import RotatingFileHandler


def create_logger(name, log_file=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s-%(levelname)s: %(message)s")
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)

    if log_file:
        fileHandler = RotatingFileHandler(filename=log_file, mode='a', maxBytes=10*1024*1024, backupCount=5)
        # fileHandler.setLevel(logging.INFO)
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)

    return logger
