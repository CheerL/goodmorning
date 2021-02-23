import logging
from logging.handlers import RotatingFileHandler

from wxpusher.wxpusher import BASEURL, WxPusher as _WxPusher
import requests


def create_logger(name, log_file=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
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


class WxPusher(_WxPusher):
    @classmethod
    def send_message(cls, content, **kwargs):
        """Send Message."""
        payload = {
            'appToken': cls._get_token(kwargs.get('token')),
            'content': content,
            'summary': kwargs.get('summary', content[:20]),
            'contentType': kwargs.get('content_type', 1),
            'topicIds': kwargs.get('topic_ids', []),
            'uids': kwargs.get('uids', []),
            'url': kwargs.get('url'),
        }
        url = f'{BASEURL}/send/message'
        return requests.post(url, json=payload).json()
