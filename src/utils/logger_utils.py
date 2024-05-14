import logging
import os
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv

load_dotenv()

FORMATTER = logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s', datefmt='%m-%d-%Y %H:%M:%S %Z')
LOG_FILE = os.getenv('LOG_FILE')
LOG_LEVEL = os.getenv('LOG_LEVEL', logging.INFO)


def get_console_handler():
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler():
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2000, backupCount=1)
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(LOG_LEVEL)
    if LOG_FILE:
        logger.addHandler(get_file_handler())
    else:
        logger.addHandler(get_console_handler())
    return logger
