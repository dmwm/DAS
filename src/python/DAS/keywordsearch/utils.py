__author__ = 'vidma'

from functools import wraps

def memo(func):
    cache = {}
    @wraps(func)
    def wrap(*args):
        if args not in cache:
            # TODO: lemma/stem caching shall be implemented differently
            if len(cache) > 3000:
                cache.popitem()
            cache[args] = func(*args)
        return cache[args]
    return wrap


def memo_1kwarg_static(func):
    cache = {}
    @wraps(func)
    def wrap(stem=False):
        if stem not in cache:
            cache[stem] = func(stem=stem)
        return cache[stem]
    return wrap
