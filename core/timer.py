import time
import inspect
import functools
from core.logger import get_logger

logger = get_logger("perf_timer")

def timed_step(label: str = None):
    """
    High-precision execution timer decorator.
    Supports:
      1. Asynchronous generator functions (async def ... yield) e.g., Scrapling spider callbacks
      2. Regular coroutines (async def ... return)
      3. Synchronous functions / methods (def ... return)
    """
    def decorator(func):
        step_name = label or func.__qualname__

        @functools.wraps(func)
        async def async_gen_wrapper(*args, **kwargs):
            start = time.perf_counter()
            item_count = 0
            try:
                async for item in func(*args, **kwargs):
                    item_count += 1
                    yield item
            finally:
                elapsed = time.perf_counter() - start
                rate = (item_count / elapsed) if elapsed > 0 else 0
                logger.info(
                    f"⏱️ [PERF] {step_name}: processed in {elapsed:.3f}s "
                    f"({item_count} items yielded | {rate:.1f} items/sec)"
                )

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return await func(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start
                logger.info(f"⏱️ [PERF] {step_name}: completed in {elapsed:.3f}s ({elapsed*1000:.1f}ms)")

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start
                logger.info(f"⏱️ [PERF] {step_name}: completed in {elapsed:.3f}s ({elapsed*1000:.1f}ms)")

        if inspect.isasyncgenfunction(func):
            return async_gen_wrapper
        elif inspect.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator
