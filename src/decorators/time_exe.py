import time
from functools import wraps

from src.utils.logger_utils import get_logger

logger = get_logger('Time Execute')


class TimeExeTag:
    database = 'DATABASE'
    blockchain = 'BLOCKCHAIN'
    execute = 'EXECUTE'
    cache = 'CACHE'
    request = 'REQUEST'
    crawl = 'CRAWL'


def async_log_time_exe(tag: str):
    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            response = await fn(*args, **kwargs)
            logger.debug(f'{tag}:{fn.__name__} executed in {round(time.time() - start_time, 3)}s')
            return response
        return wrapper
    return decorator


def sync_log_time_exe(tag: str):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            response = fn(*args, **kwargs)
            if fn.__qualname__:
                logger.debug(f'{tag}:{fn.__qualname__} executed in {round(time.time() - start_time, 3)}s')
            else:
                logger.debug(f'{tag}:{fn.__name__} executed in {round(time.time() - start_time, 3)}s')
            return response
        return wrapper
    return decorator
