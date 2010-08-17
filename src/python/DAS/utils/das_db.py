#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0613,W0622,W0702

"""
DAS DB utilities.
"""

__revision__ = "$Id: das_db.py,v 1.9 2010/05/04 21:12:19 valya Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "Valentin Kuznetsov"

# monogo db modules
from pymongo.connection import Connection

# DAS modules
from DAS.utils.das_config import das_readconfig

class _DBConnectionSingleton(object):
    """
    DAS Connection Singleton class whose purpose is to get MongoDB
    connection once and forever.
    """
    def __init__(self):
        # just for the sake of information
        self.instance = "Instance at %d" % self.__hash__()
        das_config  = das_readconfig()
        self.dbhost = das_config['mongodb'].get('dbhost')
        self.dbport = das_config['mongodb'].get('dbport')
        self.conn   = Connection(self.dbhost, self.dbport)
    def connection(self):
        """Return MongoDB connection"""
        return self.conn

DB_CONN_SINGLETON = _DBConnectionSingleton()

def db_connection():
    """Return DB connection instance"""
    return DB_CONN_SINGLETON.connection()

