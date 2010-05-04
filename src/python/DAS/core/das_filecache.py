#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS filecache wrapper.
"""

from __future__ import with_statement

__revision__ = "$Id: das_filecache.py,v 1.9 2009/05/20 14:21:52 valya Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "Valentin Kuznetsov"

import os
import types
import traceback
#try:
#    import cPickle as pickle
#except:
#    import pickle
#    pass
import marshal

import time

from sqlalchemy import Table, Column, Integer, String, Text
from sqlalchemy import create_engine, MetaData, ForeignKey
from sqlalchemy.orm import relation, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# DAS modules
from DAS.utils.utils import genkey
from DAS.core.cache import Cache

Base = declarative_base()
class System(Base):
    __tablename__ = 'systems'
    id = Column(Integer, primary_key=True)
    name = Column(String(10), nullable=False, unique=True)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<System('%s')>" % self.name

class Query(Base):
    __tablename__ = 'queries'

    id = Column(Integer, primary_key=True)
    hash = Column(String(32))
    name = Column(Text)
    create = Column(String(16))
    expire = Column(String(16))
    system_id = Column(Integer, ForeignKey('systems.id'))

    system = relation(System, backref=backref('systems', order_by=id))

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
        return time.strftime("%Y%m%d",time.gmtime(itime))
    return time.strftime("%Y%m%d",time.gmtime())

def hour(itime=None):
    if  itime:
        return time.strftime("%H",time.gmtime(itime))
    return time.strftime("%H",time.gmtime())

def clean_dirs(hourdir, datedir):
    # if no more files in dir area (creationdatehour), remove dir
    for root, dirs, files in os.walk(hourdir):
        if  not len(files):
            os.rmdir(hourdir)
            break
    # if there are empty dirs in creationdate area, remove them
    for root, dirs, files in os.walk(datedir):
        for dirname in dirs:
            try:
                os.rmdir(dirname)
            except:
                pass

class DASFilecache(Cache):
    """
    File system based DAS cache. Each query stored in cache as a single
    file whose name is hash of the query. We use simple SQLite DB to 
    bookkeep all queries, along with their hash, creation and expiration
    times.

    The cache structure is the following:
    cache
    |
    |-- dbs (data-service area)
    |      |
    |      |-- 20090518 (current day area)
    |      |    |
    |      |    |-- 01
    |      |    |-- 02
    |      |    |-- ..
    |      |    |-- 24 (24th hour)

    This will allow to address a hit pattern we expect, > 1000 queries a day, but
    < 1000 queries/hour/data-service
    """
    def __init__(self, config):
        Cache.__init__(self, config)
        self.dir     = config['filecache_dir']
        self.limit   = config['filecache_lifetime']
        self.logger  = config['logger']
        self.verbose = config['verbose']
        self.logger.info("Init filecache %s" % self.dir)
        self.systemdict = {}
        for system in config['systems']:
            self.systemdict[system] = config[system]['url']

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
        key     = genkey(query)
        sysdir  = os.path.join(self.dir, self.get_system(query))
        session = self.session()
        try: # transactions
            res = session.query(Query).filter(Query.hash==key)
            session.commit()
        except:
            session.rollback()
            traceback.print_exc()
            pass
        for qobj in res:
            valid = eval(qobj.expire) - time.time()
            timestring   = eval(qobj.create)
            creationdate = yyyymmdd(timestring)
            creationhour = hour(timestring)
            datedir      = os.path.join(sysdir, creationdate)
            hourdir      = os.path.join(datedir, creationhour)
            dir          = hourdir
            filename     = os.path.join(dir, key)
            self.logger.info("DASFilecache::get_from_cache %s" % filename)
            if  valid > 0:
                msg = "found valid query in cache, key=%s" % key
                self.logger.debug("DASFilecache::get_from_cache %s" % msg)
                if  os.path.isfile(filename):
#                    fdr = open(filename, 'r')
#                    res = pickle.load(fdr)
#                    fdr.close()
                    fdr = open(filename, 'rb')
                    res = marshal.load(fdr)
                    fdr.close()
                    return res
            else:
                msg = "found expired query in cache, key=%s" % key
                self.logger.debug("DASFilecache::get_from_cache %s" % msg)
                if  os.path.isfile(filename):
                    os.remove(filename)
                clean_dirs(hourdir, datedir)
                try: # session transactions
                    session.delete(qobj)
                    session.commit()
                except:
                    session.rollback()
                    traceback.print_exc()
                    msg = "Unable to commit to DAS filecache DB"
                    raise Exception(msg)
        return

    def update_cache(self, query, results, expire):
        """
        Insert results into cache. Query provides a hash key which
        becomes a file name.
        """
        self.logger.info("DASFilecache::update_cache(%s) store to cache" % query)
        if  not results:
            return
        system = self.get_system(query)
        key = genkey(query)
        dir = os.path.join(self.dir, system)
        dir = os.path.join(dir, yyyymmdd())
        dir = os.path.join(dir, hour())
        try:
            os.makedirs(dir)
        except:
            pass
        filename = os.path.join(dir, key)
#        fdr = open(filename, 'w')
#        pickle.dump(results, fdr)
#        fdr.close()
        fdr = open(filename, 'wb')
        marshal.dump(results, fdr)
        fdr.close()
        
        session  = self.session()
        create   = time.time()
        expire   = create+expire
        qobj     = Query(key, query, str(create), str(expire))
        try: # session transactions
            try:
                sobj = session.query(System).filter(System.name==system).one()
            except:
                sobj = System(system)
                pass
            qobj.system = sobj
            session.add(qobj)
            session.commit()
        except:
            session.rollback()
            traceback.print_exc()
            msg = "Unable to commit DAS filecache DB"
            raise Exception(msg)

    def get_system(self, query):
        system = 'das'
        url = query.split(' ')[0]
        if  url.find('http') != -1:
            for sysname, val in self.systemdict.items():
                if  url.find(val) != -1:
                    system = sysname
                    break
        return system

    def clean_cache(self):
        """
        Clean expired docs in cache 
        """
        return

    def delete_cache(self, dbname=None):
        """
        Delete all results in cache
        dbname is unused parameter to match behavior of couchdb cache
        """
        os.removedirs(self.dir)
