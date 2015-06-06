#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703,R0912,R0913,R0914,R0915,W0702,R0902

"""
DAS couroutine module
"""

# system modules
import sys
import time
from collections import deque

def coroutine(func):
    "Coroutine decorator"
    def start(*args, **kwds):
        cor = func(*args, **kwds)
        next(cor)
        return cor
    return start

@coroutine
def das_core(dasmgr, target):
    "DAS core coroutine to process DAS queries. Results are sent to given target (sink)"
    while True:
        dasquery = yield
        status = dasmgr.call(dasquery)
        target.send((dasquery.qhash, status))

@coroutine
def collector(output_pool):
    "Collector corouting which consumes results and put them into given pool"
    while True:
        pid, status = yield
        output_pool[pid] = status

def das_scheduler(pool, target):
    "Scheduler who gets new query from the pool and pass it to das core for processing"
    while True:
        try:
            query = pool.popleft()
            target.send(query)
        except IndexError:
            time.sleep(1e-3)
        except KeyboardInterrupt:
            break
