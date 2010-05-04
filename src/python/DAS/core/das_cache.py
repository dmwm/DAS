#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS cache wrapper. Communitate with DAS core and cache server(s)
"""

__revision__ = "$Id: das_cache.py,v 1.10 2009/05/28 18:59:10 valya Exp $"
__version__ = "$Revision: 1.10 $"
__author__ = "Valentin Kuznetsov"

import time
import Queue 
import traceback
import processing 
import logging

# DAS modules
from DAS.utils.utils import getarg
from DAS.utils.logger import DASLogger
from DAS.core.cache import Cache, NoResults
from DAS.core.das_memcache import DASMemcache
from DAS.core.das_couchcache import DASCouchcache
from DAS.core.das_filecache import DASFilecache

class DASCache(Cache):
    """
    Base DAS cache class based on memcache and couchdb cache back-end
    servers. The class should be initialized with config dict.
    """
    def __init__(self, config):
        Cache.__init__(self, config)
        self.servers = {'memcache': DASMemcache(config),
                        'filecache': DASFilecache(config),
                        'couchcache': DASCouchcache(config),
        }
        self.logger.info('DASCache using servers = %s' % self.servers)

    def incache(self, query):
        """
        Retreieve results from cache, otherwise return null.
        """
        servers = self.servers.keys()
        servers.sort()
        for name in servers:
            self.logger.info("DASCache::incache, using %s" % name)
            srv = self.servers[name]
            res = srv.incache(query)
            if  res:
                return res

    def get_from_cache(self, query, idx=0, limit=0):
        """
        Retreieve results from cache, otherwise return null.
        """
        servers = self.servers.keys()
        servers.sort()
        for name in servers:
            self.logger.info("DASCache::get_from_cache, using %s" % name)
            srv = self.servers[name]
            res = srv.get_from_cache(query, idx, limit)
            if  res:
                return res

    def update_cache(self, query, results, expire):
        """
        Insert results into cache.
        """
        servers = self.servers.keys()
        servers.sort()
        for name in servers:
            self.logger.info("DASCache::update_cache, using %s" % name)
            srv = self.servers[name]
            results = srv.update_cache(query, results, expire)
        for item in results:
            yield item

    def clean_cache(self, cache=None):
        """
        Clean expired results in cache.
        """
        if  cache: # request to update particular cache server
            if  not self.servers.has_key(cache):
                raise Exception("DASCache doesn't support cache='%s'" % cache)
            srv = self.servers[cache]
            srv.clean_cache()
            return
        servers = self.servers.keys()
        servers.sort()
        for name in servers:
            self.logger.info("DASCache::clean_cache, using %s" % name)
            srv = self.servers[name]
            srv.clean_cache()

    def delete_cache(self, dbname='das', cache=None):
        """
        Delete all results in cache.
        """
        if  cache: # request to update particular cache server
            if  not self.servers.has_key(cache):
                raise Exception("DASCache doesn't support cache='%s'" % cache)
            srv = self.servers[cache]
            srv.delete_cache(dbname)
            return
        servers = self.servers.keys()
        servers.sort()
        for name in servers:
            self.logger.info("DASCache::clean_cache, using %s" % name)
            srv = self.servers[name]
            srv.delete_cache(dbname)

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
        logdir      = getarg(config, 'logdir', '/tmp')
        sleep       = getarg(config, 'sleep', 2)
        verbose     = getarg(config, 'verbose', None)
        self.queue  = [] # keep track of waiting queries, (query, expire)
        self.sleep  = sleep # in sec. to sleep at each iteration of worker
        self.logger = DASLogger(idir=logdir, name='DASCacheMgr', 
                verbose=verbose)

    def add(self, query, expire):
        """Add new query to the queue"""
        self.queue.append((query, expire))

    def worker(self, func):
        """
        Monitoring worker. Must be run in separate thread. It uses infinitive
        loop to watch internal queue. Once queries has been added it pop them
        up for processing by external function. The number of allowed
        processes equal to 2*N-cores on a system.
        """
        time.sleep(5) # sleep to allow main thread with DAS core take off
        msg = "start DASCacheMgr::worker with %s" % func
        self.logger.info(msg)
        nprocs  = 2*processing.cpuCount()
        pool    = processing.Pool(nprocs)
        orphans = {} # map of orphans requests
        def update(orphans, item):
            """Update orphans dict"""
            if  orphans.has_key(item):
                orphans[item] = orphans[item] + 1
            else:
                orphans[item] = 1
        while True: 
            to_remove = {}
            msg = "waiting queue %s" % self.queue
            self.logger.debug(msg)
            for item in self.queue:
                try:
                    result = pool.apply_async(func, (item, ))
                    to_remove[item] = result
                    if  len(to_remove.keys()) == nprocs:
                        break
                except:
                    traceback.print_exc()
                    update(orphans, item)
                    break
                time.sleep(self.sleep) # separate processes
            msg = "will remove %s" % to_remove
            self.logger.debug(msg)
            time.sleep(self.sleep)
            for key in to_remove.keys():
                proc = to_remove[key]
                if  proc.ready():
                    status = proc.get()
                    if  status:
                        self.queue.remove(key)
                    else:
                        update(orphans, item)
                # check if we have this request in orphans maps
                # if number of retries more then 2, discard request
                if  orphans.has_key(key) and orphans[key] > 2:
                    del orphans[key]
                    self.queue.remove(key)

    def worker_v1(self, func):
        time.sleep(5) # sleep to allow main thread with DAS core take off
        print "\n#### start DASCacheMgr::worker with", func
        nprocs = 2*processing.cpuCount()
        pool   = processing.Pool(nprocs)
        while True: 
            to_remove = []
            print "waiting queue", self.queue
            for item in self.queue:
                try:
                    result = pool.apply_async(func, (item, ))
                    to_remove.append(item)
                    if  len(to_remove) == nprocs:
                        break
                except:
                    traceback.print_exc()
                    break
            print "will remove", to_remove
            time.sleep(self.sleep)
            for item in to_remove:
                self.queue.remove(item)
