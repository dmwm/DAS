#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS cache wrapper. Communitate with DAS core and cache server(s).
"""

__revision__ = "$Id: das_cache.py,v 1.30 2010/04/15 18:05:53 valya Exp $"
__version__ = "$Revision: 1.30 $"
__author__ = "Valentin Kuznetsov"

import time

# DAS modules
from DAS.utils.utils import genkey
from DAS.core.das_core import DASCore
from DAS.utils.logger import PrintManager
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
        self.logger = PrintManager('DASCacheMgr', verbose)
        self.qmap   = {} # map of hash:query

    def add(self, query):
        """Add new query to the queue"""
        if  query in self.qmap.keys():
            return "waiting in queue"
        qhash = genkey(str(query))
        self.qmap[qhash] = query
        self.logger.info("added %s, hash=%s, queue size=%s" \
                % (query, qhash, len(self.qmap.keys())))
        return "requested"

    def remove(self, qhash):
        """Remove query from a queue"""
        msg = "hash=%s, query=%s, queue size=%s" \
                % (qhash, self.qmap[qhash], len(self.qmap.keys()))
        self.logger.info(msg)
        try:
            del self.qmap[qhash]
        except:
            pass

# use this worker for thread pool and let it die/throw Exception
def worker(query, verbose=None):
    """
    Invokes DAS core call to update the cache for provided query
    """
    dascore = DASCore(nores=True)
    status  = dascore.call(query)

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
    logger   = PrintManager('thread_monitor', verbose)
    time.sleep(2) # sleep to allow main thread with DAS core take off
    logger.info("started with %s threads" % nprocs)
    while True: 
        time.sleep(2)
        for item in cachemgr.qmap.keys():
            if  mypool.full():
                logger.debug("### ThreadPool is full")
                time.sleep(sleep)
                continue
            query  = cachemgr.qmap[item]
            logger.debug("Add new task: query=%s, qhash=%s" % (query, item))
            mypool.add_task(worker, query, verbose)
            cachemgr.remove(item)
