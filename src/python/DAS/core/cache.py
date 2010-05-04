#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Abstract cache class.
"""

__revision__ = "$Id: cache.py,v 1.7 2009/05/27 20:28:03 valya Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "Valentin Kuznetsov"

class NoResults(Exception):
    """
    Base class exception for cache modules, should be thrown when
    no results found in a cache.
    """
    pass

class Cache(object):
    """
    Base cache class used in DAS. It defines result, keys
    method used by DAS core. And cache/update_cache used by
    concrete cache implementations.
    """
    def __init__(self, config):
        self.config  = config
        self.logger  = config['logger']

    def incache(self, query):
        """
        Check if we have results in cache for given query
        """
        self.logger.info('Cache::incache(%s)' % query)
        return

    def get_from_cache(self, query, idx=0, limit=None):
        """
        Retreieve results from cache. Must be implemented by child class
        """
        self.logger.info('Cache::get_from_cache(%s,%s,%s)' \
                % (query, idx, limit))
        return

    def update_cache(self, query, results, expire):
        """
        Insert results into cache. Must be implemented by child class.
        """
        self.logger.info('Cache::update_cache(%s,%s,%s)' \
                % (query, results, expire))
        return

    def clean_cache(self, query, results, expire):
        """
        Clean expired results in cache. Must be implemented by child class.
        """
        self.logger.info('Cache::clean_cache(%s,%s,%s)' \
                % (query, results, expire))
        return

    def delete_cache(self, query, results, expire):
        """
        Delete results in cache. Must be implemented by child class.
        """
        self.logger.info('Cache::delete_cache(%s,%s,%s)' \
                % (query, results, expire))
        return
