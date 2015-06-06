#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703,R0912,R0913,R0914,R0915,W0702,R0902

"""
DAS couroutine module provides necessary tools to run DAS workflow
via set of coroutines. To setup a processing chain user code should
supply intput/output pools and setup scheduler to process them offline.
For example:

.. doctest::

    input_pool = deque() # input pool for das queries
    output_pool = {} # output pool for results
    # organize workflow
    start_new_thread('scheduler', das_scheduler, (input_pool, das_core(collector(output_pool))))
    queries = ['file dataset=/a/b/c', 'dataset=/a/b/c', 'site=XXX']
    while True:
        qidx = random.randint(0, len(queries)-1)
        query = queries[qidx]
        input_pool.append(query)
        time.sleep(1e-3)
    time.sleep(1)

"""

# system modules
import time
from multiprocessing import Process

# das modules
from DAS.core.das_core import DASCore
from DAS.utils.das_config import das_readconfig

def coroutine(func):
    "Coroutine decorator"
    def start(*args, **kwds):
        "couroutine decorator logic"
        cor = func(*args, **kwds)
        next(cor)
        return cor
    return start

def das_request(dasquery):
    "Process DAS query and send results to target"
    # read DAS config and adjust it for stand-alone multitask mode
    dasconfig = das_readconfig()
    dasconfig['das']['core_workers'] = 1 # we'll run singl das core process
    dasconfig['das']['api_workers'] = 2  # each service API will have 2 threads
    dasconfig['das']['thread_weights'] = ['phedex:1', 'dbs3:1'] # identical weights for dbs/phedex
    dasmgr = DASCore(config=dasconfig, nores=True)
    dasmgr.call(dasquery)

@coroutine
def das_core(target):
    """
    DAS core coroutine to process DAS queries. It process request offline via
    python multiprocessing.Process module. Results are sent to given target (sink)
    """
    while True:
        dasquery = yield
        proc = Process(target=das_request, args=(dasquery,))
        proc.start()
        target.send((dasquery.qhash, proc))
#        status = dasmgr.call(dasquery)
#        target.send((dasquery.qhash, status))

@coroutine
def collector(output_pool):
    "Collector corouting consumes results and put them into given pool"
    while True:
#        pid, status = yield
#        output_pool[pid] = status
        pid, proc = yield
        output_pool[pid] = proc

def das_scheduler(pool, target):
    "Scheduler new query from the pool and pass it to das core for processing"
    while True:
        try:
            query = pool.popleft()
            target.send(query)
        except IndexError:
            time.sleep(1e-3)
        except KeyboardInterrupt:
            break
