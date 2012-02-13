#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0613,W0622,W0702,W0703

"""
DAS DB utilities.
"""

__author__ = "Valentin Kuznetsov"

import sys

# monogo db modules
from pymongo.connection import Connection
import gridfs

# DAS modules
from DAS.utils.utils import genkey, print_exc
from DAS.utils.ddict import DotDict

# MongoDB does not allow to store documents whose size more then 4MB
MONGODB_LIMIT = 4*1024*1024

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
            except Exception as exc:
                print_exc(exc)
                return None
        return self.conndict[key]

    def is_alive(self, uri):
        "Check if DB connection is alive"
        key = genkey(str(uri))
        try:
            conn, _ = self.connection(uri)
            if  conn:
                _dbnames = conn.database_names()
            else:
                return False
        except:
            if  self.conndict.has_key(key):
                del self.conndict[key]
            return False
        return True

DB_CONN_SINGLETON = _DBConnectionSingleton()

def db_connection(uri):
    """Return DB connection instance"""
    dbinst = None
    try:
        dbinst, _ = DB_CONN_SINGLETON.connection(uri)
    except:
        pass
    return dbinst

def is_db_alive(uri):
    "Check if DB is alive"
    return DB_CONN_SINGLETON.is_alive(uri)

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
            if  logger:
                msg = 'parse2gridfs record size %s, replace with %s'\
                % (row_size, gfs_rec)
                logger.info(msg)
            yield gfs_rec

def create_indexes(coll, index_list):
    """
    Create indexes for provided collection/index_list and
    ensure that they are in place
    """
    index_info = coll.index_information().values()
    for pair in index_list:
        index_exists = 0
        for item in index_info:
            if  item['key'] == [pair]:
                index_exists = 1
        if  not index_exists:
            try:
                coll.create_index([pair])
            except Exception as exp:
                print_exc(exp)
        try:
            coll.ensure_index([pair])
        except Exception as exp:
            print_exc(exp)

