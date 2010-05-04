#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mongocache wrapper.
"""

__revision__ = "$Id: das_mongocache.py,v 1.18 2009/10/15 21:00:09 valya Exp $"
__version__ = "$Revision: 1.18 $"
__author__ = "Valentin Kuznetsov"

import re
import time
import types
try:
    import json # since python 2.6
except:
    import simplejson as json # prior python 2.6
from itertools import groupby

# DAS modules
from DAS.utils.utils import getarg, dict_value, merge_dict, genkey
from DAS.core.cache import Cache

# monogo db modules
from pymongo.connection import Connection
from pymongo import DESCENDING

#def mongo_query(query):
#    """Take das input query and convert it into mongo DB one"""
#    cond = query.split(' where ')[-1].split('=')
#    if  len(cond) > 1:
#        query_dict = {cond[0]: cond[1]}
#        return query_dict
#    return {}

def convert2pattern(query):
    """
    In MongoDB patterns are specified via regular expression.
    Convert input query condition into regular expression patterns.
    Return new MongoDB compiled w/ regex query and query w/ debug info.
    """
    spec    = getarg(query, 'spec', {})
    fields  = getarg(query, 'fields', None)
    newspec = {}
    verspec = {}
    for key, val in spec.items():
        if  type(val) is types.StringType or type(val) is types.UnicodeType:
            if  val.find('*') != -1:
                val = re.compile(val.replace('*', '.*'))
                verspec[key] = val.pattern
            else:
                verspec[key] = val
            newspec[key] = val
        elif type(val) is types.DictType:
            cond  = {}
            vcond = {}
            for ckey, cval in val.items():
                if  type(cval) is types.StringType or \
                    type(cval) is types.UnicodeType:
                    if  cval.find('*') != -1:
                        cval = re.compile(cval.replace('*', '.*'))
                        vcond[ckey] = cval.pattern
                    else:
                        vcond[ckey] = cval
                    cond[ckey] = cval
                else:
                    cond[ckey] = cval
            newspec[key] = cond
            verspec[key] = vcond
    return dict(spec=newspec, fields=fields), dict(spec=verspec, fields=fields)

def compare_specs(query1, query2):
    """
    Function to compare set of fields and specs of two input mongo
    queries. We need to use it to identify if query2 is a subset of query1.
    """
    fields1 = getarg(query1, 'fields', None)
    if  not fields1:
        fields1 = []
    spec1   = getarg(query1, 'spec', {})

    fields2 = getarg(query2, 'fields', None)
    if  not fields2:
        fields2 = []
    spec2   = getarg(query2, 'spec', {})

    if  set(fields2) > set(fields1): # set2 is superset of set1
        return False

    if  set(spec2.keys()) > set(spec1.keys()):
        return False

    if  spec2.keys() != spec1.keys():
        return False

    for key, val1 in spec1.items():
        val2 = spec2[key]
        val1 = val1.replace('*', '')
        val2 = val2.replace('*', '')
        if  val2.find(val1) != -1:
            return True # only if val2 is sub-string of val1

    return False

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

    def similar_queries(self, system, query):
        """
        Check if we have query results in cache whose conditions are
        superset of provided query. For example, if cache contains records
        about T1 sites, then input query T1_CH_CERN is subset of results stored
        in cache.
        """
        self.logger.info("DASMongocache::similar_queries(%s)" % query)
        # remove from cache all expire docs
        self.col.remove({'das.expire': {'$lt' : int(time.time())}})
        spec    = getarg(query, 'spec', {})
        fields  = getarg(query, 'fields', None)
        newspec = {}
        verspec = {} # verbose spec
        # enable loose constraints, use LIKE pattern
        for key, val in spec.items():
            if  type(val) is types.StringType or type(val) is types.UnicodeType:
                if  val[-1] != '*':
                    val = val + '*' # add * to value
                val = re.compile(val.replace('*', '.*')) # replace value to regex
                verspec[key] = val.pattern
            else:
                verspec[key] = val
            newspec[key] = val
        newspec['das.system'] = system
        # now we need to look-up DAS queries and compare their conditions
        # with current one. To do so, I invoke group method with reduce
        # function to get DISTINCT list of queries for provided loose set 
        # of conditions
        reduce = "function(obj,prev){ return true;}"
        res  = self.col.group(['query'], newspec, 0, reduce=reduce)
        msg  = "DASMongocache::similar_queries, "
        msg += "loose query: %s" % verspec
        self.logger.info(msg)
        for row in res:
            if  type(row['query']) is types.StringType or \
                type(row['query']) is types.UnicodeType:
                cond = json.loads(row['query'])
                print "\n\n### Found cond", cond
            elif type(row['query']) is types.ListType:
                gen = (k for k, g in groupby(row['query']))
                for item in gen:
                    cond = json.loads(item)
                    print "\n\n### Found cond", cond
            else:
                raise TypeError('Unexpected type for query %s, %s' \
                % (row['query'], type(row['query'])))
            return True
        return False


    def version1(self):
        newspecdebug = {}
        msg     = 'Unable to loose condition: '
        for key, val in spec.items():
            # string-value case, w* is superset of word*
            if  type(val) is types.StringType or type(val) is types.UnicodeType:
                if  val[-1] == '*':
                    val = val[:-2] + '*'
                else:
                    val = val[:-1] + '*'
                val = re.compile(val.replace('*', '.*'))
                newspec[key] = val
                newspecdebug[key] = val.pattern
            elif type(val) is types.DictType: # e.g. run : {'$gt':10}
                # $gt-operator case, run > 1 is superset of run > 10
                # $lt-operator case, run < 11 is superset of run < 10
                cond = {}
                for ckey, cval in val.items():
                    if  ckey == '$gt':
                        if  type(cval) is types.IntType or \
                            type(cval) is types.FloatType:
                            cval -= 1
                            cond[ckey] = cval
                        else:
                            msg = "key %s, value %s" % (ckey, cval)
                            raise TypeError(msg)
                    elif ckey == '$lt':
                        if  type(cval) is types.IntType or \
                            type(cval) is types.FloatType:
                            cval += 1
                            cond[ckey] = cval
                        else:
                            msg = "key %s, value %s" % (ckey, cval)
                            raise TypeError(msg)
                    cond[ckey] = cval
                newspec[key] = cond
                newspecdebug[key] = cond
        newspec['das.system'] = system
#        res  = self.col.find(spec=newspec, fields=fields).count()
        res  = self.col.find(spec=newspec, fields=['das.query']).count()
        msg  = "DASMongocache::similar_queries, "
        msg += "loose query: %s, found results %s" % (newspecdebug, res)
        self.logger.info(msg)
        if  res:
            return True
        return False

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
        res    = self.col.find(spec=spec, fields=fields).count()
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
        self.logger.info("DASMongocache::get_from_cache(%s, %s, %s, %s, %s)"\
                % (query, idx, limit, skey, order))
        query, dquery = convert2pattern(query)
        self.logger.info("DASMongocache::get_from_cache, converted to %s" \
                % dquery)
        idx    = int(idx)
        spec   = getarg(query, 'spec', {})
        fields = getarg(query, 'fields', None)
        if  limit:
            res = self.col.find(spec=spec, fields=fields)\
                .skip(idx).limit(limit)
        else:
            res = self.col.find(spec=spec, fields=fields)
        for row in res:
            del(row['_id']) #mongo add internal _id, we don't need it (cannot JSON'ify)
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
        str_query  = json.dumps(query)
        buffer     = [] # small buffer to be used for bulk updates
# This optimization is premature, since it leads to another problem of
# cleaning expire records from DAS.
#        dashash    = genkey(str(dasheader))
#        if  not self.col.find_one({'dashash':dashash}):
#            self.col.insert(dict(dashash=dashash, das=dasheader))
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
                item['query'] = str_query
#                item['dashash'] = dashash # see above the dashash
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
