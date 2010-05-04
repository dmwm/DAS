#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS cache wrapper. Communitate with DAS core and cache server(s)
"""

__revision__ = "$Id: das_cache.py,v 1.1 2009/03/13 21:10:04 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

#import types

# DAS modules
#from utils.utils import genkey
from DAS.core.das_core import DASCore
from DAS.core.cache import Cache
from DAS.core.das_memcache import DASMemcache
from DAS.core.das_couchdb import DASCouchDB

class DASCache(Cache):
    """
    Base DAS cache class based on memcache and couchdb cache back-end
    servers. The class should be initialized with config dict.
    """
    def __init__(self, mode=None, debug=None):
        mgr = DASCore(mode, debug)
        Cache.__init__(self, mgr)
        self.servers = { 'memcache' : DASMemcache(mgr) }
        couchdb = DASCouchDB(mgr)
        self.servers['couch'] = couchdb
        self.logger.info('DASCache using servers = %s' % self.servers)

    def get_from_cache(self, query):
        """
        Retreieve results from cache, otherwise return null.
        """
        servers = self.servers.keys()
        servers.sort()
        for name in servers:
            self.logger.info("DASCache::get_from_cache, using %s" % name)
            srv = self.servers[name]
            res = srv.get_from_cache(query)
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
            srv.update_cache(query, results, expire)

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
