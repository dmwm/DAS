# coding=utf-8
#pylint: disable=C0111
# pylint disabled: missing docstrings

from functools import wraps


def memo(func):
    """ a memo decorator providing cached results """
    cache = {}

    @wraps(func)
    def wrap(*args):
        if args not in cache:
            if len(cache) > 3000:
                cache.popitem()
            cache[args] = func(*args)
        return cache[args]
    return wrap
