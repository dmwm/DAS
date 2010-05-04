#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS memcache wrapper. Communitate with DAS core and memcache server(s)
"""

__revision__ = "$Id: das_memcache.py,v 1.4 2009/05/15 15:12:22 valya Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "Valentin Kuznetsov"

import memcache
import types

# DAS modules
from DAS.utils.utils import genkey
from DAS.core.cache import Cache

class DASMemcache(Cache):
    """
    Base DAS cache class based on memcached, see
    http://www.danga.com/memcached/
    The client API uses python-memcache module from
    ftp://ftp.tummy.com/pub/python-memcached
    Further optimization can be achieve by using
    cmemcache, a Python extension for libmemcache,
    the C API to memcached, see
    http://gijsbert.org/cmemcache/index.html
    """
    def __init__(self, mgr):
        Cache.__init__(self, mgr)
        cachelist = self.dasmgr.cache_servers.split(',')
        self.memcache  = memcache.Client(cachelist, debug=self.verbose)

        # default hashing is crc32, but we can change that by using
        #from zlib import adler32
        #memcache.serverHashFunction = adler32

        self.limit = self.dasmgr.cache_lifetime
        self.logger.info("Init memcache" % cachelist)

    def get_from_cache(self, query):
        """
        Retreieve results from cache, otherwise return null.
        """
        key = genkey(query)
        res = self.memcache.get(key)
        if  res and type(res) is types.IntType:
            self.logger.info("DASMemcache::result(%s) using cache" % query)
            rowlist = ['%s' % i for i in range(0, res)]
            rowdict = self.memcache.get_multi(rowlist, key_prefix=key)
            return rowdict.values()
        else:
            return

    def update_cache(self, query, results, expire):
        """
        Insert results into cache.
        We use set/get_multi memcache functions to reduce network latency.
          - store each row to avoid max cache size limitation, 1MB. 
          - each row uses unique row_key which are stored into cache 
            within key namespace. 
        """
        self.logger.info("DASMemcache::result(%s) store to cache" % query)
        if  not results:
            return
        key = genkey(query)
        rowdict = {}
        rowid = 0
        for row in results:
            rowkey = '%s' % rowid
            rowdict[rowkey] = row
            rowid += 1
        self.memcache.set_multi(rowdict, time=self.limit, key_prefix=key)
        self.memcache.set(key, rowid, expire)

    def clean_cache(self):
        """
        Clean expired docs in cache 
        """
        return

    def delete(self, dbname=None):
        """
        Delete all results in cache
        dbname is unused parameter to match behavior of couchdb cache
        """
        # Use flush_all, which
        # expire all data currently in the memcache servers.
        self.memcache.flush_all()

