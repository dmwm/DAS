#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
View manager class.
"""

__revision__ = "$Id: das_viewmanager.py,v 1.1 2009/04/30 20:47:37 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

class DASViewManager(object):
    """
    View manager class responsible of mapping user defined
    views into DAS queries
    """
    def __init__(self):
        self.map = {} # should be replaced with persistent storage

    def create(self, name, query):
        """
        create new view for given name and a query
        """
        # TODO: query validation via DAS QL parser
        self.map[name] = query.strip().split(' where ')[0]

    def delete(self, name):
        """
        delete existing view
        """
        if  self.map.has_key(name):
            del(self.map[name])

    def update(self, name, query):
        """
        update exising view with new given query
        """
        if  self.map.has_key(name):
            # TODO: we can add logic to keep track of updates here
            self.create(name, query)
        else:
            Exception("View '%s', doesn't exists")
        

    def get(self, name):
        """
        retrieve DAS query for given name
        """
        if  not self.map.has_key(name):
            Exception("View '%s', doesn't exists")
        return self.map[name]

    def all(self):
        """
        retrieve all views
        """
        return self.map

