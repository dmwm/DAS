#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS cache wrapper. Communitate with DAS core and cache server(s).
"""

__revision__ = "$Id: das_cache.py,v 1.30 2010/04/15 18:05:53 valya Exp $"
__version__ = "$Revision: 1.30 $"
__author__ = "Valentin Kuznetsov"

import time
import traceback
import multiprocessing 
from   multiprocessing import cpu_count

# DAS modules
from DAS.utils.utils import genkey
from DAS.core.das_core import DASCore
from DAS.utils.logger import DASLogger, DummyLogger
from DAS.utils.threadpool import ThreadPool

class DASCacheMgr(object):
    """
    DAS cache manager class. It consists of simple queue and worker.
    Worker method runs in separate thread and monitor internal queue.
    It uses 2*N-cores processes to run external function over there.
    """
    def __init__(self, config):
        """
        Initialize DAS cache manager.
        """
        logfile = config['logfile']
        verbose = config['verbose']
        name    = 'DASCacheMgr'
        self.logger = DASLogger(logfile=logfile, verbose=verbose, name=name)
        self.queue  = [] # keep track of waiting queries
        self.qmap   = {} # map of hash:query

    def add(self, query):
        """Add new query to the queue"""
        if  query in self.queue:
            return "waiting in queue"
        qhash = genkey(str(query))
        self.qmap[qhash] = query
        self.queue.append(qhash)
        self.logger.info("::add, added %s, hash=%s, queue size=%s" \
                % (query, qhash, len(self.queue)))
        return "requested"

    def remove(self, qhash):
        """Remove query from a queue"""
        msg = "::remove, hash=%s, query=%s, queue size=%s" \
                % (qhash, self.qmap[qhash], len(self.queue))
        self.logger.info(msg)
        try:
            self.queue.remove(qhash)
        except:
            pass
        try:
            del self.qmap[qhash]
        except:
            pass

# use this worker for thread pool and let it die/throw Exception
def worker(query, verbose=None):
    """
    Invokes DAS core call to update the cache for provided query
    """
    logger  = DummyLogger()
    dascore = DASCore(logger=logger, nores=True)
    status  = dascore.call(query)

# use this worker for multiprocessing since I can capture the status
def worker_w_status(query, verbose=None):
    """
    Invokes DAS core call to update the cache for provided query
    """
    logger = DummyLogger()
    status = 0
    try:
        dascore = DASCore(logger=logger, nores=True)
        status  = dascore.call(query)
    except:
        traceback.print_exc()
    return status

def multiprocess_monitor(cachemgr, config):
    """
    Monitoring worker. Must run in separate thread. It uses infinitive
    loop to watch internal queue. Once query has been added it pops
    up it for processing by external function. The number of allowed
    processes equal to 2*N-cores on a system.
    """
    sleep    = int(config.get('sleep', 2))
    verbose  = int(config.get('verbose', 0))
    logfile  = config.get('logfile', None)
    name     = 'MULTIPROC_MONITOR'
    logger   = DASLogger(logfile=logfile, verbose=verbose, name=name)
    nprocs   = 2*cpu_count()
    mypool   = multiprocessing.Pool(nprocs)
    time.sleep(5) # sleep to allow main thread with DAS core take off
    logger.info("monitoring_worker started with %s processes" % nprocs)
    orphans     = {} # map of orphans requests
    worker_proc = {} # map of workers {(queyry, lifetime): worker}
    while True: 
        for item in cachemgr.queue:
            if  worker_proc.has_key(item):
                continue # we already working on this
            if  len(worker_proc.keys()) == nprocs:
                break
            try:
                worker_proc[item] = '' # reserve worker process
                if  not cachemgr.qmap.has_key(item):
                    continue
                query  = cachemgr.qmap[item]
                result = mypool.apply_async(worker_w_status, (query, verbose))
                worker_proc[item] = result # bind result with worker
            except:
                traceback.print_exc()
                orphans[item] = orphans.get(item, 0) + 1
                break
            msg = "add %s, workers %s, in queue %s, orphans %s" \
            % (query, len(worker_proc.keys()), len(cachemgr.queue), 
                len(orphans.keys()))
            logger.debug(msg)
            time.sleep(sleep) # separate processes
        msg = "# workers %s, in queue %s, orphans %s" \
        % (len(worker_proc.keys()), len(cachemgr.queue), len(orphans.keys()))
        logger.debug(msg)
        time.sleep(sleep)
        for key in worker_proc.keys():
            proc = worker_proc[key]
            if  isinstance(proc, str):
                msg = "ERROR: process %s get result %s, but should AsyncResult"\
                    % (key, proc)
                logger.error(msg)
                continue
            if  proc.ready():
                status = proc.get()
                if  status:
                    cachemgr.remove(key)
                    del worker_proc[key]
                else:
                    orphans[key] = orphans.get(key, 0) + 1
            # check if we have this request in orphans maps
            # if number of retries more then 2, discard request
            if  orphans.has_key(key):
                cachemgr.remove(key)
                try:
                    del orphans[key]
                except:
                    pass
                try:
                    del worker_proc[key]
                except:
                    pass

def thread_monitor(cachemgr, config):
    """
    Monitoring worker. Must run in separate thread. It uses infinitive
    loop to watch internal queue. Once query has been added it pops
    up it for processing by external function.
    """
    sleep    = config.get('sleep')
    verbose  = config.get('verbose')
    logfile  = config.get('logfile')
    nprocs   = config.get('nprocs')
    mypool   = ThreadPool(nprocs)
    name     = 'THREAD_MONITOR'
    logger   = DASLogger(logfile=logfile, verbose=verbose, name=name)
    time.sleep(2) # sleep to allow main thread with DAS core take off
    logger.info("started with %s threads" % nprocs)
    while True: 
        time.sleep(2)
        for item in cachemgr.queue:
            if  mypool.full():
                logger.debug("### ThreadPool is full")
                time.sleep(sleep)
                continue
            query  = cachemgr.qmap[item]
            logger.debug("Add new task: query=%s, qhash=%s" % (query, item))
            mypool.add_task(worker, query, verbose)
            cachemgr.remove(item)
