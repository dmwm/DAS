#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
View manager class.
"""

__revision__ = "$Id: das_viewmanager.py,v 1.2 2009/04/30 20:54:11 valya Exp $"
__version__ = "$Revision: 1.2 $"
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
            raise Exception("View '%s', doesn't exists" % name)
        

    def get(self, name):
        """
        retrieve DAS query for given name
        """
        if  not self.map.has_key(name):
            raise Exception("View '%s', doesn't exists" % name)
        return self.map[name]

    def all(self):
        """
        retrieve all views
        """
        return self.map

