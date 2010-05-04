#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Abstract cache class.
"""

__revision__ = "$Id: cache.py,v 1.2 2009/04/30 21:00:21 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

#from DAS.utils.logger import DASLogger

class Cache(object):
    """
    Base cache class used in DAS. It defines result, keys
    method used by DAS core. And cache/update_cache used by
    concrete cache implementations.
    """
    def __init__(self, mgr):
        self.dasmgr  = mgr
        self.verbose = mgr.verbose
        self.logger  = mgr.logger
#        self.logger  = DASLogger(verbose=mgr.verbose, stdout=mgr.stdout)

    def keys(self):
        """
        Mimic DAS core functionality, return service keys
        """
        return self.dasmgr.service_keys

    def views(self):
        """
        Mimic DAS core functionality, return DAS views
        """
        return self.dasmgr.views()

    def result(self, query):
        """
        Return results for given query. If results found in cache
        use it, otherwire invoke core DAS call and update the cache.
        """
        results = self.get_from_cache(query)
        if  not results:
            results = self.dasmgr.result(query)
            self.update_cache(query, results, expire=600)
        return results

    def get_from_cache(self, query):
        """
        Retreieve results from cache. Must be implemented by child class
        """
        self.logger.info('Cache::cache(%s)' % query)
        return

    def update_cache(self, query, results, expire):
        """
        Insert results into cache. Must be implemented by child class.
        """
        self.logger.info('Cache::update_cache(%s)' % query)
        return

    def clean_cache(self, query, results, expire):
        """
        Clean expired results in cache. Must be implemented by child class.
        """
        self.logger.info('Cache::clean_cache(%s)' % query)
        return

