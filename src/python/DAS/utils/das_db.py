#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0613,W0622,W0702,W0703

"""
DAS DB utilities.
"""
from __future__ import print_function

__author__ = "Valentin Kuznetsov"

# system modules
import sys
import time
import threading

if  sys.version.startswith('3.'):
    basestring = str

# monogo db modules
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure
import gridfs
import pymongo

# DAS modules
from DAS.utils.utils import genkey, print_exc, dastimestamp, Singleton
from DAS.utils.das_config import das_readconfig
from DAS.utils.ddict import DotDict
from DAS.utils.das_pymongo import PYMONGO_OPTS, MongoOpts

# MongoDB does not allow to store documents whose size more then 4MB
MONGODB_LIMIT = 4*1024*1024

def find_one(col, spec, fields=None):
    "Custom implementation of find_one function for given MongoDB parameters"
    return col.find_one(spec, fields)
#    res = None
#    try:
#        gen = (r for r in col.find(spec, fields, **PYMONGO_OPTS))
#        res = gen.next()
#        return res
#    except StopIteration:
#        return None
#    return None

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

class MongoConnection(object):
    __metaclass__ = Singleton # in python3 use MongoConnection(object, metaclass=Singleton)
    def __init__(self, uri, **args):
        self.mongo_client = MongoClient(uri, **args)
    def client(self):
        return self.mongo_client

# TODO: re-think Singleton usage in environment where DAS/MongoDB is out of order
# at start-up time. When MongoDB is not available DAS uses connection which is
# Singleton and can't recover. I commented out the __metaclass__ and MongoConnection
# usage in DBConnection class for time being.

class DBConnection(object):
#    __metaclass__ = Singleton # in python3 use DBConnection(object, metaclass=Singleton)
    """
    DB Connection class which hanldes MongoDB connections. Input parameters:

        - lifetime, controls connection lifetime
        - retry, controls number of retries to acquire MongoDB connection
    """
    def __init__(self, pool_size=300, lifetime=3600, retry=5):
        # just for the sake of information
        self.instance = "Instance at %d" % self.__hash__()
        self.conndict = {}
        self.timedict = {}
        self.thr      = lifetime
        self.retry    = retry
        self.psize    = pool_size
        self.mongo_opts = MongoOpts(w=1, psize=self.psize, fsync=True).opts()

    def genkey(self, uri):
        "Generate unique key"
        if  isinstance(uri, basestring):
            key = uri
        elif isinstance(uri, list) and len(uri) == 1:
            key = uri[0]
        else:
            key = genkey(str(uri))
        return key

    def connection(self, uri):
        """Return MongoDB connection"""
        key = self.genkey(uri)
        # check cache first
        try: # this block may fail in multi-threaded environment
            if  key in self.timedict:
                if  self.is_alive(uri) and (time.time()-self.timedict[key]) < self.thr:
                    return self.conndict[key]
                else: # otherwise clean-up
                    del self.timedict[key]
                    del self.conndict[key]
        except:
            pass
        return self.get_new_connection(uri)

    def get_new_connection(self, uri):
        "Get new MongoDB connection"
        key = self.genkey(uri)
        for idx in range(0, self.retry):
            try:
                dbinst = MongoClient(host=uri, **self.mongo_opts)
#                dbinst = MongoConnection(uri, **self.mongo_opts).client()
                gfs    = dbinst.gridfs
                fsinst = gridfs.GridFS(gfs)
                self.conndict[key] = (dbinst, fsinst)
                self.timedict[key] = time.time()
                return (dbinst, fsinst)
            except (ConnectionFailure, AutoReconnect) as exc:
                tstamp = dastimestamp('')
                thread = threading.current_thread()
                print("### MongoDB connection failure thread=%s, id=%s, time=%s" \
                        % (thread.name, thread.ident, tstamp))
                print_exc(exc)
            except Exception as exc:
                print_exc(exc)
            time.sleep(idx)
        return self.conndict.get(key, (None, None))

    def is_alive(self, uri):
        "Check if DB connection is alive"
        key = self.genkey(uri)
        if  key in self.conndict:
            conn, _ = self.conndict[key]
            return conn.alive()
        return False

DB_CONN_SINGLETON = DBConnection()

def db_connection(uri, singleton=True, verbose=True):
    """Return DB connection instance"""
    if  singleton:
        dbinst, _ = DB_CONN_SINGLETON.connection(uri)
    else:
        dbinst, _ = DBConnection(pool_size=10).connection(uri)
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
    if  not prim_key:
        return
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
                if  isinstance(pair, list):
                    coll.create_index(pair)
                else:
                    coll.create_index([pair])
            except Exception as exp:
                print_exc(exp)
        try:
            if  isinstance(pair, list):
                coll.ensure_index(pair)
            else:
                coll.ensure_index([pair])
        except Exception as exp:
            print_exc(exp)

def db_monitor(uri, func, sleep=5):
    """
    Check status of MongoDB connection. Invoke provided function upon
    successfull connection.
    """
    conn = db_connection(uri)
    while True:
        if  not conn or not is_db_alive(uri):
            try:
                conn = db_connection(uri)
                func()
                if  conn:
                    print("### db_monitor re-established connection %s" % conn)
                else:
                    print("### db_monitor, lost connection")
            except:
                pass
        time.sleep(sleep)


def get_db_uri():
    """ returns default dburi from config """
    config = das_readconfig()
    return config['mongodb']['dburi']


def query_db(dbname, dbcol, query,  idx=0, limit=10):
    """
    query a given db collection
    """

    conn = db_connection(get_db_uri())
    col = conn[dbname][dbcol]

    if col:
        try:
            if limit == -1:
                for row in col.find(query, **PYMONGO_OPTS):
                    yield row
            else:
                for row in col.find(query).skip(idx).limit(limit):
                    yield row
        except Exception as exc:  # we shall not catch GeneratorExit
            print_exc(exc)
