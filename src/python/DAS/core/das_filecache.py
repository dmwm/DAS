#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS filecache wrapper.
"""

from __future__ import with_statement

__revision__ = "$Id: das_filecache.py,v 1.16 2009/06/30 19:35:42 valya Exp $"
__version__ = "$Revision: 1.16 $"
__author__ = "Valentin Kuznetsov"

import os
import types
import traceback
import marshal

import time

from sqlalchemy import Column, Integer, String, Text
from sqlalchemy import create_engine, MetaData, ForeignKey, and_
from sqlalchemy.orm import relation, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound

# DAS modules
from DAS.utils.utils import genkey
from DAS.core.cache import Cache

Base = declarative_base()
class System(Base):
    """System ORM"""
    __tablename__ = 'systems'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id = Column(Integer, primary_key=True)
    name = Column(String(10), nullable=False, unique=True)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        """String representation of System ORM object"""
        return "<System('%s')>" % self.name

class Query(Base):
    """Query ORM"""
    __tablename__ = 'queries'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id = Column(Integer, primary_key=True)
    hash = Column(String(32), unique=True)
    name = Column(Text)
    create = Column(String(16))
    expire = Column(String(16))
    system_id = Column(Integer, ForeignKey('systems.id'))

    system = relation(System, backref=backref('systems', order_by=id))

    def __init__(self, ihash, name, create, expire):
        self.hash = ihash
        self.name = name
        self.create = create
        self.expire = expire

    def __repr__(self):
        """String representation of Query ORM object"""
        return "<Query('%s', '%s','%s', '%s')>" \
                % (self.hash, self.name, self.create, self.expire)

def yyyymmdd(itime=None):
    """returns time in yyyymmdd format"""
    if  itime:
        return time.strftime("%Y%m%d", time.gmtime(itime))
    return time.strftime("%Y%m%d", time.gmtime())

def hour(itime=None):
    """returns current hour"""
    if  itime:
        return time.strftime("%H", time.gmtime(itime))
    return time.strftime("%H", time.gmtime())

def clean_dirs(hourdir, datedir):
    """clean directories"""
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
#        dbfile       = None
#        db_engine    = 'mysql://xxx:yyy@localhost/DAS'
        self.engine  = create_engine(db_engine, echo=False)
        self.session = sessionmaker(bind=self.engine)
        if  not dbfile:
            self.create_table()
        else: # sqlite case
            if  not os.path.isfile(dbfile):
                self.create_table()

    def create_table(self):
        """Create DB tables based on ORM objects"""
        metadata = MetaData()
        query_table = Query.__table__
        Base.metadata.create_all(self.engine)

    def is_expired(self, query):
        """
        Check if we have query result is expired in cache.
        """
        key     = genkey(query)
        curtime = time.time()
        session = self.session()
        sql_stm = session.query(Query).filter(and_(Query.hash==key, \
            Query.expire < '%s' % curtime ))
        try:
            res = sql_stm.one()
        except Exception, exp:
            msg = 'query=%s\n%s %s %s\n%s' % (query, sql_stm, key, curtime, exp)
            self.logger.debug(msg)
#            print "\n### das_filecache:is_expired msg=", msg
            return False
        return True

    def incache(self, query):
        """
        Check if we have query results in cache, otherwise return null.
        """
        if  self.is_expired(query):
            self.remove_from_cache(query)
            return False
        key     = genkey(query)
        curtime = time.time()
        session = self.session()
        sql_stm = session.query(Query).filter(and_(Query.hash==key, \
            Query.expire > '%s' % time.time() ))
        try:
            res = sql_stm.one()
        except Exception, exp:
            msg = 'query=%s\n%s %s %s\n%s' % (query, sql_stm, key, curtime, exp)
            self.logger.debug(msg)
#            print "\n### das_filecache:incache msg=", msg
            return False
        return True

    def get_from_cache(self, query, idx=0, limit=0):
        """
        Retreieve results from cache, otherwise return null.
        """
        id      = int(idx)
        idx     = int(idx)
        stop    = idx + long(limit)
        key     = genkey(query)
        sysdir  = os.path.join(self.dir, self.get_system(query))
        session = self.session()
        try: # transactions
            res = session.query(Query).filter(Query.hash==key)
            session.commit()
        except:
            session.rollback()
            self.logger.debug(traceback.format_exc())
            pass
        for qobj in res:
            valid = eval(qobj.expire) - time.time()
            timestring   = eval(qobj.create)
            creationdate = yyyymmdd(timestring)
            creationhour = hour(timestring)
            datedir      = os.path.join(sysdir, creationdate)
            hourdir      = os.path.join(datedir, creationhour)
            idir         = hourdir
            filename     = os.path.join(idir, key)
            self.logger.info("DASFilecache::get_from_cache %s" % filename)
            if  valid > 0:
                msg = "found valid query in cache, key=%s" % key
                self.logger.debug("DASFilecache::get_from_cache %s" % msg)
                if  os.path.isfile(filename):
                    fdr = open(filename, 'rb')
                    if  limit:
                        for i in range(0, stop):
                            try:
                                res = marshal.load(fdr)
                                if  i >= idx:
                                    res['id'] = id
                                    yield res
                                    id += 1
                            except EOFError, err:
                                break
                    else:
                        while 1:
                            try:
                                res = marshal.load(fdr)
                                res['id'] = id
                                yield res
                                id += 1
                            except EOFError, err:
                                break
                    fdr.close()
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
                    self.logger.debug(traceback.format_exc())
                    msg = "Unable to commit to DAS filecache DB"
                    raise Exception(msg)

    def update_cache(self, query, results, expire):
        """
        Insert results into cache. Query provides a hash key which
        becomes a file name.
        """
        if  not expire:
            raise Exception('Expire parameter is null')
        self.logger.info("DASFilecache::update_cache(%s) store to cache" \
                % query)
        if  not results:
            return
        system = self.get_system(query)
        key  = genkey(query)
        idir = os.path.join(self.dir, system)
        idir = os.path.join(idir, yyyymmdd())
        idir = os.path.join(idir, hour())
        try:
            os.makedirs(idir)
        except:
            pass
        filename = os.path.join(idir, key)
        fdr = open(filename, 'wb')
        if  type(results) is types.ListType or \
            type(results) is types.GeneratorType:
            for item in results:
                marshal.dump(item, fdr)
                yield item
        else:
            marshal.dump(results, fdr)
            yield results
        fdr.close()
        
        create   = time.time()
        expire   = create+expire
        qobj     = Query(key, query, str(create), str(expire))
        session  = self.session()
        try: # session transactions
            try:
                sobj = session.query(System).filter(System.name==system).one()
            except:
                sobj = System(system)
                session.add(sobj)
                pass
            qobj.system = sobj
            session.add(qobj)
            session.commit()
        except:
            session.rollback()
            self.logger.debug(traceback.format_exc())
            msg = "Unable to commit DAS filecache DB"
            raise Exception(msg)

    def remove_from_cache(self, query):
        """
        Remove query from cache
        """
        key     = genkey(query)
        session = self.session()
        try: # transactions
            res = session.query(Query).filter(Query.hash==key).one()
            session.delete(res)
            session.commit()
        except:
            session.rollback()
            self.logger.debug(traceback.format_exc())
            return False
        return True

    def get_system(self, query):
        """Look-up system used for given query"""
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

    def delete_cache(self, dbname=None, system=None):
        """
        Delete all results in cache
        dbname is unused parameter to match behavior of couchdb cache
        """
#        os.removedirs(self.dir)
        os.system('rm -rf %s' % self.dir)
