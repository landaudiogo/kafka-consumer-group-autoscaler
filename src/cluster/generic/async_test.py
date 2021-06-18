import asyncio 
from functools import wraps, partial
import time

def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run


loop = asyncio.get_event_loop()

@async_wrap
def stream_rows():
    print('STREAMING ROWS')
    time.sleep(3)


start = time.time()

tasks = [
    asyncio.ensure_future(stream_rows()), 
    asyncio.ensure_future(stream_rows()), 
]
loop.run_until_complete(asyncio.wait(tasks))
loop.close()

total_time = time.time() - start
print(total_time)
