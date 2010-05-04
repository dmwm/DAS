#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mongocache wrapper.
"""

__revision__ = "$Id: das_mongocache.py,v 1.16 2009/10/15 00:24:45 valya Exp $"
__version__ = "$Revision: 1.16 $"
__author__ = "Valentin Kuznetsov"

import time
import types
try:
    import json # since python 2.6
except:
    import simplejson as json # prior python 2.6

# DAS modules
from DAS.utils.utils import getarg, dict_value, merge_dict
from DAS.core.cache import Cache

# monogo db modules
from pymongo.connection import Connection
from pymongo import DESCENDING

def mongo_query(query):
    """Take das input query and convert it into mongo DB one"""
    cond = query.split(' where ')[-1].split('=')
    if  len(cond) > 1:
        query_dict = {cond[0]: cond[1]}
        return query_dict
    return {}

def update_item(item, key, val):
    """
    Update provided row with given key and a value. The key can be in
    form of x.y.z, etc. in this case it is composed key and associative
    dictionary must be build.
    The value here can be in form of MongoDB condition
    dictionary, e.g. {key : {'$gte':value}}
    """
    if  type(val) is not types.DictType:
        value = val
    else:
        value = val.values()

    keys = key.split('.')
    if  len(keys) == 1:
        if  not item.has_key(key):
            item[key] = value
    else: # we got composed key
        keys.reverse()
        for kkk in keys:
            if  kkk == keys[0]:
                newdict = {kkk : value}
            elif kkk == keys[-1]:
                continue
            else:
                newdict = {kkk : newdict}
        item[kkk] = newdict
    
class DASMongocache(Cache):
    """
    DAS cache based MongoDB. 
    """
    def __init__(self, config):
        Cache.__init__(self, config)
        self.dbhost  = config['mongocache_dbhost']
        self.dbport  = config['mongocache_dbport']
        self.limit   = config['mongocache_lifetime']
        self.dbname  = getarg(config, 'mongocache_dbname', 'das')
        self.colname = 'cache'
        self.logger  = config['logger']
        self.verbose = config['verbose']

        msg = "DASMongocache::__init__ %s:%s@%s" \
        % (self.dbhost, self.dbport, self.dbname)
        self.logger.info(msg)

        self.conn    = Connection(self.dbhost, self.dbport)
        self.db      = self.conn[self.dbname]
        self.col     = self.db[self.colname]
        self.buffer_size = 100 # TODO: I can pass it via config
        
    def is_expired(self, query):
        """
        Check if we have query result is expired in cache.
        """
        return True

    def incache(self, query):
        """
        Check if we have query results in cache, otherwise return null.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        self.logger.info("DASMongocache::incache(%s)" % query)
        # remove from cache all expire docs
        self.col.remove({'das.expire': {'$lt' : int(time.time())}})
        spec   = getarg(query, 'spec', {})
        fields = getarg(query, 'fields', None)
        res = self.col.find(spec=spec, fields=fields).count()
        if  res:
            return True
        return False

    def nresults(self, query):
        """
        Return number of results for given query.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        self.logger.info("DASMongocache::nresults(%s)" % query)
        spec   = getarg(query, 'spec', {})
        fields = getarg(query, 'fields', None)
        return self.col.find(spec=spec, fields=fields).count()

    def get_from_cache(self, query, idx=0, limit=0, skey=None, order='asc'):
        """
        Retreieve results from cache, otherwise return null.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        self.logger.info("DASMongocache::get_from_cache(%s)" \
                % query)
        idx    = int(idx)
        spec   = getarg(query, 'spec', {})
        fields = getarg(query, 'fields', None)
        if  limit:
            res = self.col.find(spec=spec, fields=fields)\
                .skip(idx).limit(limit)
        else:
            res = self.col.find(spec=spec, fields=fields)
        for row in res:
            del(row['_id']) #mongo add internal _id, we don't need it
            if  fields:
                fkeys = [k.split('.')[0] for k in fields]
                if  set(row.keys()) & set(fkeys) == set(fkeys):
                    yield row # only when row has all fields
            else:
                yield row

    def update_cache(self, query, results, header):
        """
        Insert results into cache. Query provides a hash key which
        becomes a file name.
        """
        self.logger.info("DASMongocache::update_cache(%s) store to cache" \
                % query)
        if  not results:
            return
        dasheader  = header['das']
        dasheader['selection_keys'] = header['selection_keys']
        lkeys      = header['lookup_keys']
        index_list = [(key, DESCENDING) for key in lkeys]
        prim_key   = lkeys[0] # TODO: what to do with multiple look-up keys
        trigger    = 0
        buffer     = [] # small buffer to be used for bulk updates
        if  type(results) is types.ListType or \
            type(results) is types.GeneratorType:
            for item in results:
                # TODO:
                # the exception/error records should not go to cache
                # instead we can place them elsewhere for further analysis
                if  item.has_key('exception') or item.has_key('error'):
                    continue
                if  not trigger:
                    trigger = 1
                item['das'] = dasheader
                try:
                    entry = dict_value(item, prim_key)
                    row = self.col.find_one({prim_key:entry})
                except:
                    row = None
                if  row:
                    value = dict_value(row, prim_key)
                    if  value == entry: # we found a match in cache
                        mdict = merge_dict(item, row)
                        del mdict['_id']
                        self.col.insert(mdict)
                        self.col.remove({'_id': row['_id']})
                    else:
                        if  len(buffer) < self.buffer_size:
                            buffer.append(item)
                        else:
                            self.col.insert(buffer)
                            buffer = []
#                        self.col.insert(item)
                else:
                    if  len(buffer) < self.buffer_size:
                        buffer.append(item)
                    else:
                        self.col.insert(buffer)
                        buffer = []
#                    self.col.insert(item)
                if  index_list:
                    try:
                        self.col.ensure_index(index_list)
                    except:
                        pass
            if  buffer:
                self.col.insert(buffer)
                buffer = []
            if  not trigger: # we got empty results
                # we will insert empty record to avoid consequentive
                # calls to service who doesn't have data
                self.col.insert(dict(das=dasheader))
        else:
            print "\n\n ### results = ", str(results)
            raise Exception('Provided results is not a list/generator type')

    def remove_from_cache(self, query):
        """
        Remove query from cache
        """
        self.col.remove(query)

    def clean_cache(self):
        """
        Clean expired docs in cache 
        """
        current_time = time.time()
        query = {'das.expire': { '$lt':current_time} }
        self.col.remove(query)

    def delete_cache(self, dbname=None, system=None):
        """
        Delete all results in cache
        dbname is unused parameter to match behavior of couchdb cache
        """
        self.col.remove({})
