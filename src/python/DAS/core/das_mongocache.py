#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mongocache wrapper.
"""

__revision__ = "$Id: das_mongocache.py,v 1.2 2009/07/23 19:57:35 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

import os
import types
import traceback
import time

# DAS modules
from DAS.utils.utils import genkey, getarg, sort_data
from DAS.core.cache import Cache

# monogo db modules
from pymongo.connection import Connection

def mongo_query(query):
    """Take das input query and convert it into mongo DB one"""
    cond = query.split(' where ')[-1].split('=')
    query_dict = {cond[0]: cond[1]}
    return query_dict

class DASMongocache(Cache):
    """
    DAS cache based MongoDB. 
    """
    def __init__(self, config):
        Cache.__init__(self, config)
        self.dir         = config['mongocache_dir']
        if  not os.path.isdir(self.dir):
            os.makedirs(self.dir)
        self.dbhost      = config['mongocache_dbhost']
        self.dbport      = config['mongocache_dbport']
        self.limit       = config['mongocache_lifetime']
        self.dbname      = getarg(config, 'mongocache_dbname', 'das')
        self.logger      = config['logger']
        self.verbose     = config['verbose']
        self.logger.info("Init mongocache %s" % self.dir)
        self.conn        = Connection(self.dbhost, self.dbport)
        self.collections = {} # dict of collections {srv_name: col_obj}
        self.config = config
        self.create_db()

    def create_db(self):
        """Create MongoDB and initialize collections"""
        if  self.dbname not in self.conn.database_names():
            # we need to create DB and set of collections
            self.db    = self.conn[self.dbname]
            for srv in self.config['systems']:
                url    = self.config[srv]['url']
                record = {srv: url}
                srv_collection = self.db[srv]
                srv_collection.insert(record)
                self.collections[srv] = srv_collection
            das_collection = self.db['das']
            das_collection.insert({'das': time.time()})
            self.collections['das'] = das_collection
        else:
            self.db = self.conn[self.dbname]
            srv_names = ['das'] + self.config['systems']
            srv_names.sort()
            col_names = [i for i in self.db.collection_names() \
                if  i.find('index') == -1]
            col_names.sort()
            if  col_names != srv_names:
                msg  = 'Number of collections not equal to expected services\n'
                msg += 'Collections: %s' % col_names
                msg += 'Services:    %s' % srv_names
                raise Exception(msg)
            for name in self.db.collection_names():
                self.collections[name] = self.db[name]
        msg = ""
        for srv in self.config['systems'] + ['das']:
            res = getattr(self.db, srv).find()
            msg += "records in %s\n" % srv
            for i in res:
                msg += "%s\n" % str(i)
        self.logger.info("MongoDB collections: %s" % msg)
        
    def is_expired(self, query):
        """
        Check if we have query result is expired in cache.
        """
        return True

    def incache(self, query):
        """
        Check if we have query results in cache, otherwise return null.
        """
        query = mongo_query(query)
        if  self.is_expired(query):
            self.remove_from_cache(query)
            return False
        for name in self.db.collection_names():
            col = self.coldict[name]
            res = col.find(query).count()
            if  res:
                return True
        return False

    def get_from_cache(self, query, idx=0, limit=0, skey=None, order='asc'):
        """
        Retreieve results from cache, otherwise return null.
        """
        query   = mongo_query(query)
        idx     = int(idx)
        for name in self.db.collection_names():
            if  name.find('index') != -1:
                continue
            col = self.collections[name]
            if  limit:
                res = col.find(query).skip(idx).limit(limit)
            else:
                res = col.find(query)
            for obj in res:
                row = obj.to_dict()
                del(row['_id']) # mongoDB add internal _id of the obj, we don't need it
                yield row

    def update_cache(self, query, results, expire):
        """
        Insert results into cache. Query provides a hash key which
        becomes a file name.
        """
        if  not expire:
            raise Exception('Expire parameter is null')
        self.logger.info("DASMongocache::update_cache(%s) store to cache" \
                % query)
        if  not results:
            return
        query   = mongo_query(query)
        if  type(results) is types.ListType or \
            type(results) is types.GeneratorType:
            for item in results:
                system = getarg(item, 'system', 'das')
                col = self.collections[system]
                col.insert(item)
                yield item
        else:
            system = 'das'
            if  type(results) is types.DictType:
                system = getarg(results, 'system', 'das')
            col = self.collections[system]
            col.insert(results)
            yield results

    def remove_from_cache(self, query):
        """
        Remove query from cache
        """
        query   = mongo_query(query)
        for name in self.db.collection_names():
            if  name.find('index') != -1:
                continue
            col = self.collections[name]
            col.remove(query)
        return

    def clean_cache(self):
        """
        Clean expired docs in cache 
        """
        current_time = time.time()
        query = {'expire': { '$lt':current_time} }
        for name in self.db.collection_names():
            if  name.find('index') != -1:
                continue
            col = self.collections[name]
            col.remove(query)
        return

    def delete_cache(self, dbname=None, system=None):
        """
        Delete all results in cache
        dbname is unused parameter to match behavior of couchdb cache
        """
#        self.conn.drop_database(self.dbname)
#        self.create_db()
        for name in self.db.collection_names():
            if  name.find('index') != -1:
                continue
            col = self.collections[name]
            col.remove({})
