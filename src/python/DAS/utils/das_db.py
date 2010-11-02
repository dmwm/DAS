#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0613,W0622,W0702

"""
DAS DB utilities.
"""

__revision__ = "$Id: das_db.py,v 1.9 2010/05/04 21:12:19 valya Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "Valentin Kuznetsov"

import sys
import time
import types
import traceback

# monogo db modules
from pymongo.connection import Connection
import gridfs

# DAS modules
from DAS.utils.utils import DotDict, genkey

# MongoDB does not allow to store documents whose size more then 4MB
MONGODB_LIMIT = 4*1024*1024

def connection_monitor(uri, func, sleep=5):
    """
    Monitor connection to MongoDB and invoke provided function
    upon successfull connection. This function can be used in DAS server
    for monitoring MongoDB connections.
    """
    conn = db_connection(uri)
    while True:
        time.sleep(sleep)
        if  not conn:
            conn = db_connection(dbhost, dbport)
            if  conn:
                func()

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

    def connection(self, uri):
        """Return MongoDB connection"""
        key = genkey(str(uri))
        if  not self.conndict.has_key(key):
            try:
                dbinst = Connection(host=uri)
                gfs    = dbinst.gridfs
                fsinst = gridfs.GridFS(gfs)
                self.conndict[key] = (dbinst, fsinst)
            except:
                traceback.print_exc()
                return None
        return self.conndict[key]

DB_CONN_SINGLETON = _DBConnectionSingleton()

def db_connection(uri):
    """Return DB connection instance"""
    dbinst = None
    try:
        dbinst, _ = DB_CONN_SINGLETON.connection(uri)
    except:
        pass
    return dbinst

def db_gridfs(uri):
    """
    Return pointer to MongoDB GridFS
    """
    fsinst = None
    try:
        _, fsinst = DB_CONN_SINGLETON.connection(uri)
    except:
        pass
    return fsinst

def parse2gridfs(gfs, prim_key, genrows, logger=None):
    """
    Yield docs from provided generator with size < 4MB or store them into
    GridFS.
    """
    key = prim_key.split('.')[0]
    for row in genrows:
        if  not row:
            continue
        row_size = sys.getsizeof(str(row))
        if  row_size < MONGODB_LIMIT:
            yield row
        else:
            fid = gfs.put(str(row))
            gfs_rec = {key: {'gridfs_id': str(fid)}}
            ddict = DotDict(row)
            val = ddict._get(prim_key)
            if  logger:
                msg = 'parse2gridfs record size %s, replace with %s'\
                % (row_size, gfs_rec)
                logger.info(msg)
            if  val != row and sys.getsizeof(str(val)) < MONGODB_LIMIT:
                drec = DotDict(gfs_rec)
                drec._set(prim_key, val)
                yield drec
            else:
                yield gfs_rec

