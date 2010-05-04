#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Abstract cache class.
"""

__revision__ = "$Id: cache.py,v 1.1 2010/01/19 19:02:57 valya Exp $"
__version__ = "$Revision: 1.1 $"
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

    def is_expired(self, query):
        """
        Check if query result is expired in cache
        """
        self.logger.info('Cache::is_expired(%s)' % query)
        return

    def nresults(self, query):
        """
        Return number of reulsts in cache for given query.
        We implement this method in base class since all
        get_from_cache implementations are generators or lists.
        """
        res = 0
        for item in self.get_from_cache(query):
            res += 1
        return res

    def get_from_cache(self, query, idx=0, limit=0, skey=None, order='asc'):
        """
        Retreieve results from cache. Must be implemented by child class
        """
        self.logger.info('Cache::get_from_cache(%s,%s,%s,%s,%s)' \
                % (query, idx, limit, skey, order))
        return

    def update_cache(self, query, results, expire):
        """
        Insert results into cache. Must be implemented by child class.
        """
        self.logger.info('Cache::update_cache(%s,%s,%s)' \
                % (query, results, expire))
        return

    def remove_from_cache(self, query):
        """
        Remove query from cache. Must be implemented by child class
        """
        self.logger.info('Cache::remove_from_cache(%s)' \
                % (query, ))
        return

    def clean_cache(self):
        """
        Clean expired queries in cache. Must be implemented by child class.
        """
        self.logger.info('Cache::clean_cache')
        return

    def delete_cache(self, dbname=None, system=None):
        """
        Delete results in cache. Must be implemented by child class.
        """
        self.logger.info('Cache::delete_cache(%s,%s)' \
                % (dbname, system))
        return
