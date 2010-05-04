#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS cache wrapper. Communitate with DAS core and cache server(s).
"""

__revision__ = "$Id: das_cache.py,v 1.24 2010/04/13 15:04:52 valya Exp $"
__version__ = "$Revision: 1.24 $"
__author__ = "Valentin Kuznetsov"

import time
import types
import traceback
import multiprocessing 
from   multiprocessing import cpu_count

# DAS modules
from DAS.utils.utils import genkey
from DAS.core.das_core import DASCore

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
        self.logger = config['logger']
        self.queue  = [] # keep track of waiting queries
        self.qmap   = {} # map of hash:query

    def add(self, query):
        """Add new query to the queue"""
        if  query in self.queue:
            return "waiting in queue"
        qhash = genkey(str(query))
        self.qmap[qhash] = query
        self.queue.append(qhash)
        self.logger.info("DASCacheMgr::add, added %s, hash=%s, queue size=%s" \
                % (query, qhash, len(self.queue)))
        return "requested"

    def remove(self, qhash):
        """Remove query from a queue"""
        try:
            self.queue.remove(qhash)
        except:
            pass
        try:
            del self.qmap[qhash]
        except:
            pass
        self.logger.info("DASCacheMgr::remove, hash=%s, queue size=%s" \
                % (qhash, len(self.queue)))

def worker(query):
    """
    Invokes DAS core call to update the cache for provided query
    """
    dascore = DASCore()
    status  = dascore.call(query)
    return status

def monitoring_worker(cachemgr, config):
    """
    Monitoring worker. Must run in separate thread. It uses infinitive
    loop to watch internal queue. Once query has been added it pops
    up it for processing by external function. The number of allowed
    processes equal to 2*N-cores on a system.
    """
    sleep    = int(config.get('sleep', 2))
    logger   = config['logger']
    nprocs   = 2*cpu_count()
    mypool   = multiprocessing.Pool(nprocs)
    time.sleep(5) # sleep to allow main thread with DAS core take off
    logger.info("DASCacheMgr::worker started with %s processes" % nprocs)
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
                result = mypool.apply_async(worker, (query, ))
                worker_proc[item] = result # bind result with worker
            except:
                traceback.print_exc()
                orphans[item] = orphans.get(item, 0) + 1
                break
            time.sleep(sleep) # separate processes
        msg = "workers %s, in queue %s, orphans %s" \
        % (len(worker_proc.keys()), len(cachemgr.queue), len(orphans.keys()))
        logger.debug(msg)
        time.sleep(sleep)
        for key in worker_proc.keys():
            proc = worker_proc[key]
            if  type(proc) is types.StringType:
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
