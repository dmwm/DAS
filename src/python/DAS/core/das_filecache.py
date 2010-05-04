#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS filecache wrapper.
"""

from __future__ import with_statement

__revision__ = "$Id: das_filecache.py,v 1.1 2009/05/18 01:17:16 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import os
import types
try:
    import cPickle as pickle
except:
    import pickle
    pass
import time

from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import create_engine, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# DAS modules
from DAS.utils.utils import genkey
from DAS.core.cache import Cache

Base = declarative_base()
class Query(Base):
    __tablename__ = 'query'

#    id = Column(Integer, primary_key=True)
    hash = Column(String, primary_key=True)
    name = Column(String)
    create = Column(String)
    expire = Column(String)

    def __init__(self, hash, name, create, expire):
        self.hash = hash
        self.name = name
        self.create = create
        self.expire = expire

    def __repr__(self):
       return "<Query('%s', '%s','%s', '%s')>" \
                % (self.hash, self.name, self.create, self.expire)

def yyyymmdd(itime=None):
    if  itime:
        return time.strftime("%Y%M%d",time.gmtime(itime))
    return time.strftime("%Y%M%d",time.gmtime())

class DASFilecache(Cache):
    """
    File system based DAS cache. Each query stored in cache as a single
    file whose name is hash of the query. We use simple SQLite DB to 
    bookkeep all queries, along with their hash, creation and expiration
    times.
    """
    def __init__(self, mgr, idir=None):
        Cache.__init__(self, mgr)
        if  idir:
            self.dir = idir
        else:
            self.dir = self.dasmgr.filecache_dir
        self.limit = self.dasmgr.filecache_lifetime
        self.logger.info("Init filecache %s" % self.dir)

#        self.dir = os.path.join(os.getcwd(), 'cache')
        try:
            os.makedirs(self.dir)
        except:
            pass

        if  self.verbose:
            verbose  = True
        else:
            verbose  = False
        dbfile       = os.path.join(self.dir, 'das_filecache.db')
        db_engine    = 'sqlite:///%s' % dbfile
        self.engine  = create_engine(db_engine, echo=verbose)
        self.session = sessionmaker(bind=self.engine)
        if  not os.path.isfile(dbfile):
            self.create_table()

    def create_table(self):
        metadata = MetaData()
        query_table = Query.__table__
        Base.metadata.create_all(self.engine)

    def get_from_cache(self, query):
        """
        Retreieve results from cache, otherwise return null.
        """
        key = genkey(query)

        session = self.session()
        res = session.query(Query).filter(Query.hash==key)
        for qobj in res:
            valid = eval(qobj.expire) - time.time()
            creationdate = yyyymmdd(eval(qobj.create))
            dir = os.path.join(self.dir, creationdate)
            filename = os.path.join(dir, key)
            self.logger.info("DASFilecache::get_from_cache %s" % filename)
            if  valid > 0:
                msg = "found valid query in cache, key=%s" % key
                self.logger.debug("DASFilecache::get_from_cache %s" % msg)
                if  os.path.isfile(filename):
                    fdr = open(filename, 'r')
                    res = pickle.load(fdr)
                    fdr.close()
                    return res
            else:
                msg = "found expired query in cache, key=%s" % key
                self.logger.debug("DASFilecache::get_from_cache %s" % msg)
                if  os.path.isfile(filename):
                    os.remove(filename)
                # if no more files in creationdate dir, remove the dir
                for root, dirs, files in os.walk(dir):
                    if  not len(files):
                        os.rmdir(dir)
                        break
                session.delete(qobj)
                session.commit()
        return

    def update_cache(self, query, results, expire):
        """
        Insert results into cache. Query provides a hash key which
        becomes a file name.
        """
        self.logger.info("DASFilecache::update_cache(%s) store to cache" % query)
        if  not results:
            return
        key = genkey(query)
        dir = os.path.join(self.dir, yyyymmdd())
        try:
            os.makedirs(dir)
        except:
            pass
        filename = os.path.join(dir, key)
        fdr = open(filename, 'w')
        pickle.dump(results, fdr)
        fdr.close()
        
        session = self.session()
        create  = time.time()
        expire  = create+expire
        qobj = Query(key, query, str(create), str(expire))
        session.add(qobj)
        session.commit()

    def clean_cache(self):
        """
        Clean expired docs in cache 
        """
        return

    def delete(self, dbname=None):
        """
        Delete all results in cache
        dbname is unused parameter to match behavior of couchdb cache
        """
        os.removedirs(self.dir)
