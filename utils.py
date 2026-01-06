
import functools
import logging
from asyncio import sleep as aiotime_sleep
logger = logging.getLogger(__name__)
def fallback(max_retries=3, initial_delay=1, backoff_factor=2, exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        await aiotime_sleep(delay)
                        delay *= backoff_factor
            raise last_exception

        return wrapper
    return decorator
