import time
from functools import wraps
def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f'Записи вставлены за   {elapsed:0.4}')
        return retval
    return inner