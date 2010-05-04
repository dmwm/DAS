#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS cache wrapper. Communitate with DAS core and cache server(s).
"""

__revision__ = "$Id: das_cache.py,v 1.22 2010/01/26 21:02:04 valya Exp $"
__version__ = "$Revision: 1.22 $"
__author__ = "Valentin Kuznetsov"

import time
import traceback
try:
    # Python 2.6
    import multiprocessing 
    from multiprocessing import cpu_count
except:
    # Prior to 2.6 requires py-processing package
    import processing as multiprocessing
    from processing import cpuCount as cpu_count
import logging

# DAS modules
from DAS.utils.utils import getarg, genkey

class DASCacheMgr(object):
    """
    DAS cache manager class. It consists of simple queue and worker.
    Worker method runs in separate thread and monitor internal queue.
    It uses 2*N-cores processes to run external function over there.
    """
    def __init__(self, config={}):
        """
        Initialize DAS cache manager.
        """
        sleep       = getarg(config, 'sleep', 2)
        verbose     = getarg(config, 'verbose', None)
        self.logger = config['logger']
        self.queue  = [] # keep track of waiting queries, (query, expire)
        self.qmap   = {} # map of hash:query
        self.sleep  = sleep # in sec. to sleep at each iteration of worker
        self.nprocs = 2*cpu_count()
        self.pool   = multiprocessing.Pool(self.nprocs)
        self.logger.info('Number of CPUs: %s' % self.nprocs)

    def add(self, query, expire):
        """Add new query to the queue"""
        if  (query, expire) in self.queue:
            return "waiting in queue"
        qhash = genkey(str(query))
        self.qmap[qhash] = query
        self.queue.append((qhash, expire))
        if  not self.pool._pool:
            self.pool = multiprocessing.Pool(self.nprocs)
        return "requested"

    def remove(self, key):
        """Remove query from a queue"""
        qhash = key[0] # key = (hash, expire), see add method
        try:
            self.queue.remove(key)
        except:
            pass
        try:
            del self.qmap[qhash]
        except:
            pass

    def worker(self, func):
        """
        Monitoring worker. Must run in separate thread. It uses infinitive
        loop to watch internal queue. Once query has been added it pops
        up it for processing by external function. The number of allowed
        processes equal to 2*N-cores on a system.
        """
        time.sleep(5) # sleep to allow main thread with DAS core take off
        msg = "start DASCacheMgr::worker with %s" % func
        self.logger.info(msg)
        orphans     = {} # map of orphans requests
        worker_proc = {} # map of workers {(queyry, lifetime): worker}
        while True: 
            for item in self.queue:
                if  worker_proc.has_key(item):
                    continue # we already working on this
                if  len(worker_proc.keys()) == self.nprocs:
                    break
                try:
                    worker_proc[item] = '' # reserve worker process
                    if  not self.qmap.has_key(item[0]):
                        continue
                    query  = self.qmap[item[0]] # item=(hash, expire)
                    expire = item[1]
                    result = self.pool.apply_async(func, (query, expire))
                    worker_proc[item] = result # bind result with worker
                except:
                    traceback.print_exc()
                    orphans[item] = orphans.get(item, 0) + 1
                    break
                time.sleep(self.sleep) # separate processes
            msg = "workers %s, in queue %s, orphans %s" \
            % (len(worker_proc.keys()), len(self.queue), len(orphans.keys()))
            self.logger.debug(msg)
            time.sleep(self.sleep)
            for key in worker_proc.keys():
                proc = worker_proc[key]
                if  type(proc) is types.StringType:
                    msg = "ERROR: process %s get result %s, but should AsyncResult"\
                        % (key, proc)
                    self.logger.error(msg)
                    continue
                if  proc.ready():
                    status = proc.get()
                    if  status:
                        self.remove(key)
                        del worker_proc[key]
                    else:
                        orphans[key] = orphans.get(key, 0) + 1
                # check if we have this request in orphans maps
                # if number of retries more then 2, discard request
#                if  orphans.has_key(key) and orphans[key] > 2:
                if  orphans.has_key(key):
                    self.remove(key)
                    try:
                        del orphans[key]
                    except:
                        pass
                    try:
                        del worker_proc[key]
                    except:
                        pass
