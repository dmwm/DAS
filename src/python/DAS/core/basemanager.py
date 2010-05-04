#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Abstract manager class.
"""

__revision__ = "$Id: basemanager.py,v 1.3 2009/05/27 20:28:03 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

class BaseManager(object):
    """
    Base manager class used in DAS. It defines result, keys
    method used by DAS core.
    """
    def __init__(self, dasconfig):
        self.verbose        = dasconfig['verbose']
        self.logger         = dasconfig['logger']
        self.cache_servers  = dasconfig['cache_servers']
        self.cache_lifetime = dasconfig['cache_lifetime']
        self.couch_servers  = dasconfig['couch_servers']
        self.couch_lifetime = dasconfig['couch_lifetime']
        self.filecache_dir  = dasconfig['filecache_dir']
        self.filecache_lifetime = dasconfig['filecache_lifetime']
        self.service_keys   = {}

    def keys(self):
        """
        Base method to return service keys
        """
        return self.service_keys

    def result(self, query=None):
        """
        Base method to return results of the query
        """
        print query
        return
