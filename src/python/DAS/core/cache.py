#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Abstract cache class.
"""

__revision__ = "$Id: cache.py,v 1.5 2009/05/19 12:43:10 valya Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "Valentin Kuznetsov"

class Cache(object):
    """
    Base cache class used in DAS. It defines result, keys
    method used by DAS core. And cache/update_cache used by
    concrete cache implementations.
    """
    def __init__(self, config):
        self.config  = config
        self.logger  = config['logger']

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

