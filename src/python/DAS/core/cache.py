#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Abstract cache class.
"""

__revision__ = "$Id: cache.py,v 1.3 2009/05/01 17:44:26 valya Exp $"
__version__ = "$Revision: 1.3 $"
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

    def get_view(self, name=None):
        """
        Mimic DAS core functionality, return DAS views
        """
        return self.dasmgr.get_view(name)

    def create_view(self, name, query):
        """
        Mimic DAS core functionality, create DAS view
        """
        return self.dasmgr.create_view(name, query)

    def update_view(self, name, query):
        """
        Mimic DAS core functionality, update DAS view
        """
        return self.dasmgr.update_view(name, query)

    def delete_view(self, name):
        """
        Mimic DAS core functionality, delete DAS view
        """
        return self.dasmgr.delete_view(name)

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

