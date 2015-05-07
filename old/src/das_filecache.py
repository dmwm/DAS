#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS filecache wrapper.
"""

from __future__ import with_statement

__revision__ = "$Id: das_filecache.py,v 1.1 2010/01/19 19:02:57 valya Exp $"
__version__ = "$Revision: 1.1 $"
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
from DAS.utils.utils import genkey, getarg, sort_data
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

class Location(Base):
    """Location ORM"""
    __tablename__ = 'locations'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id  = Column(Integer, primary_key=True)
    dir = Column(Text, nullable=False, unique=True)

    def __init__(self, idir):
        self.dir = idir

    def __repr__(self):
        """String representation of Location ORM object"""
        return "<Location('%s')>" % self.dir

class Query(Base):
    """Query ORM"""
    __tablename__ = 'queries'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id        = Column(Integer, primary_key=True)
    hash      = Column(String(32), unique=True)
    name      = Column(Text)
    create    = Column(String(16))
    expire    = Column(String(16))
    system_id = Column(Integer, ForeignKey('systems.id'))
    dir_id    = Column(Integer, ForeignKey('locations.id'))

    system    = relation(System, backref=backref('systems', order_by=id))
    location  = relation(Location, backref=backref('locations', order_by=id))

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

def next_number(inumber, base='00'):
    """
    Create next number in a base format, e.g. 01, out of provided one.
    """
    format = '%%(n)0%sd' % len(base)
    number = format % {'n' : int(inumber) + 1}
    limit  = '9' * len(base)
    if  int(number) > int(limit):
        raise Exception('Run out of basedir space')
    return number
    
def create_dir(topdir, system, base='00', filesperdir=100):
    """
    Allocate new dir name for provided topdir and DAS system name.
    We use the following schema: YYYYMMDD/HOUR/base/base
    where HOUR is 00-24, and base provided as external parameter.
    """
    idir = os.path.join(topdir, system)
    idir = os.path.join(idir, yyyymmdd())
    idir = os.path.join(idir, hour())
    if  not os.path.isdir(idir):
        idir = os.path.join(os.path.join(idir, base), base)
    else:
        for root, dirs, files in os.walk(idir):
            dirs.sort()
            if  dirs:
                idir = os.path.join(root, dirs[-1])
                for _root, _dirs, _files in os.walk(idir):
                    if  _dirs:
                        _dirs.sort()
                        try:
                            newdir = next_number(_dirs[-1], base)
                        except:
                            try:
                                newdir = next_number(dirs[-1])
                                idir = os.path.join(os.path.join(root, newdir) , base)
                            except:
                                raise
                            break
                        last_dir = os.path.join(_root, _dirs[-1])
                        if  len(os.listdir(last_dir)) > filesperdir:
                            idir = os.path.join(_root, newdir)
                        else:
                            idir = os.path.join(_root, _dirs[-1])
                    else:
                        idir = os.path.join(idir, base)
                    break
                break
                if  not _dirs:
                    idir = os.path.join(idir, base)
            else:
                idir = os.path.join(os.path.join(root, base), base)
    try:
        os.makedirs(idir)
    except:
        pass
    return idir

def clean_dirs(topdir):
    """Scan provided topdir and remove empty dirs"""
    for root, dirs, files in os.walk(topdir):
        if  not dirs:
            os.rmdir(root)

class DASFilecache(Cache):
    """
    File system based DAS cache. Each query stored in cache as a single
    file whose name is hash of the query. We use simple SQLite DB to 
    bookkeep all queries, along with their hash, creation and expiration
    times.

    The cache structure is the following YYYYMMDD/HH/XXX/YYY, where
    YYYYMMDD is 4 digit for year, 2 digit for month, 2 digit for day;
    HH represents hours in 0-24 cycle;
    XXX and YYY are span from 0 to 999 using 3 digits. Here is an example
    of cache dir stucture: cache_top/dbs/20090706/02/001/998/
    We use XXX/YYY sub-dir structure to allocate 1M dirs within 1 hour.
    """
    def __init__(self, config):
        Cache.__init__(self, config)
        self.dir        = config['filecache_dir']
        self.limit      = config['filecache_lifetime']
        self.base_dir   = getarg(config, 'filecache_base_dir', '00')
        self.files_dir  = getarg(config, 'filecache_files_dir', 100)
        self.logger     = config['logger']
        self.verbose    = config['verbose']
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
        dbengine     = config['filecache_db_engine'] 
        dbfile       = None
        if  dbengine.find('sqlite:///') != -1:
            dbfile   = dbengine.replace('sqlite:///', '')
        self.engine  = create_engine(dbengine, echo=False)
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
        except Exception as exp:
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
        except Exception as exp:
            msg = 'query=%s\n%s %s %s\n%s' % (query, sql_stm, key, curtime, exp)
            self.logger.debug(msg)
#            print "\n### das_filecache:incache msg=", msg
            return False
        return True

    def get_from_cache(self, query, idx=0, limit=0, skey=None, order='asc'):
        """
        Retreieve results from cache, otherwise return null.
        """
#        id      = int(idx)
        idx     = int(idx)
        stop    = idx + long(limit)
        key     = genkey(query)
        sysdir  = os.path.join(self.dir, self.get_system(query))
        session = self.session()
        try: # transactions
            res = session.query(Query, Location).\
                        filter(Query.dir_id==Location.id).\
                        filter(Query.hash==key)
            session.commit()
        except:
            session.rollback()
            self.logger.debug(traceback.format_exc())
            pass
        for qobj, dobj in res:
            valid = eval(qobj.expire) - time.time()
            timestring   = eval(qobj.create)
            idir         = dobj.dir
            filename     = os.path.join(idir, key)
            self.logger.info("DASFilecache::get_from_cache %s" % filename)
            if  valid > 0:
                msg = "found valid query in cache, key=%s" % key
                self.logger.debug("DASFilecache::get_from_cache %s" % msg)
                if  os.path.isfile(filename):
                    fdr = open(filename, 'rb')
                    if  skey:
                        # first retrieve full list of results and sort it
                        data = []
                        id = 0
                        while 1:
                            try:
                                res = marshal.load(fdr)
                                if  type(res) is dict:
                                    res['id'] = id
                                data.append(res)
                                id += 1
                            except EOFError as err:
                                break
                        fdr.close()
                        sorted_data = sort_data(data, skey, order)
                        index = 0
                        for row in sorted_data:
                            if  limit:
                                if  index >= idx and index < stop:
                                    yield row
                            else:
                                if  index >= idx:
                                    yield row
                            index += 1
                    else:
                        id = 0
                        while 1:
                            try:
                                res = marshal.load(fdr)
                                if  type(res) is dict:
                                    res['id'] = id
                                if  limit:
                                    if  id >= idx and id < stop:
                                        yield res
                                    if  id == stop:
                                        break
                                else:
                                    yield res
                                id += 1
                            except EOFError as err:
                                break
                        fdr.close()
            else:
                msg = "found expired query in cache, key=%s" % key
                self.logger.debug("DASFilecache::get_from_cache %s" % msg)
                fdir = os.path.split(filename)[0]
                if  os.path.isfile(filename):
                    os.remove(filename)
                clean_dirs(fdir)
                try: # session transactions
                    session.delete(qobj)
                    session.commit()
                except:
                    session.rollback()
                    self.logger.debug(traceback.format_exc())
                    msg = "Unable to delete object from DAS filecache DB"
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
        idir = create_dir(self.dir, system, self.base_dir, self.files_dir)
        filename = os.path.join(idir, key)
        fdr = open(filename, 'wb')
        if  type(results) is list or \
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
            try:
                dobj = session.query(Location).filter(Location.dir==idir).one()
            except:
                dobj = Location(idir)
                session.add(dobj)
                pass
            qobj.system = sobj
            qobj.location = dobj
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
