#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0613,W0622,W0702

"""
DAS DB utilities.
"""

__revision__ = "$Id: das_db.py,v 1.9 2010/05/04 21:12:19 valya Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "Valentin Kuznetsov"

import types

# monogo db modules
from pymongo.connection import Connection

def make_uri(pairs):
    """Return MongoDB URI for provided set of dbhost,dbport pairs"""
    uris = []
    for item in pairs:
        dbhost, dbport = item
        if  not dbport:
            uris.append(dbhost)
        else:
            if  not isinstance(dbport, int):
                msg = 'Invalid port="%s", type=%s' % (dbport, type(dbport))
                raise Exception(msg)
            if  dbport <= 1024:
                msg = 'Not enough privileges to use port=%s' % dbport
                raise Exception(msg)
            uris.append('%s:%s' % (dbhost, dbport))
    return 'mongodb://%s' % ','.join(uris)

class _DBConnectionSingleton(object):
    """
    DAS Connection Singleton class whose purpose is to get MongoDB
    connection once and forever.
    """
    def __init__(self):
        # just for the sake of information
        self.instance = "Instance at %d" % self.__hash__()
        self.conndict = {}

    def connection(self, dbhost, dbport):
        """Return MongoDB connection"""
        pair = (dbhost, dbport)
        uri  = make_uri([pair])
        if  not self.conndict.has_key(uri):
            self.conndict[uri] = Connection(uri)
        return self.conndict[uri]

    def connections(self, pairs):
        """Return MongoDB connection"""
        uri  = make_uri(pairs)
        if  not self.conndict.has_key(uri):
            self.conndict[uri] = Connection(uri)
        return self.conndict[uri]

DB_CONN_SINGLETON = _DBConnectionSingleton()

def db_connection(dbhost, dbport):
    """Return DB connection instance"""
    return DB_CONN_SINGLETON.connection(dbhost, dbport)

def db_connections(pairs):
    """
    Return DB connection instance for provided set of (dbhost, dbport)
    pairs
    """
    return DB_CONN_SINGLETON.connections(pairs)

