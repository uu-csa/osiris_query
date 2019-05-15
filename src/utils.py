import timeit
import functools

def reporter(func):
    @functools.wraps(func)
    def wrapper_reporter(*args, **kwargs):
        start = timeit.default_timer()
        args_repr = [f'{a!s}' for a in args]
        kwargs_repr = [f'{k}={v!s}' for k, v in kwargs.items()]
        signature = '\n\n'.join(args_repr + kwargs_repr)
        print("=" * 80)
        print(f"CALLING {func.__name__!r}")
        print("=" * 80)
        print(f"{signature}")
        value = func(*args, **kwargs)
        print("-" * 80)
        stop = timeit.default_timer()
        sec = stop - start
        print(f"{func.__name__!r} returned {value!r} in {sec} seconds\n")
        return value
    return wrapper_reporter
