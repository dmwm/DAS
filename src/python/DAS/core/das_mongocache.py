#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mongocache wrapper.
"""

__revision__ = "$Id: das_mongocache.py,v 1.28 2009/11/08 19:17:29 valya Exp $"
__version__ = "$Revision: 1.28 $"
__author__ = "Valentin Kuznetsov"

import re
import time
import types
import itertools

# DAS modules
from DAS.utils.utils import getarg, dict_value, merge_dict, genkey
from DAS.core.cache import Cache

# monogo db modules
from pymongo.connection import Connection
from pymongo.objectid import ObjectId
from pymongo import DESCENDING

DOT = '.'
SEP = '___'

def adjust_id(query):
    """
    We need to adjust input query who has '_id' as a string to ObjectId
    used in MongoDB.
    """
    spec = query['spec']
    if  spec.has_key('_id'):
        val = spec['_id']
        if  type(val) is types.StringType:
            newval = ObjectId(val)
            spec['_id'] = newval
        elif type(val) is types.UnicodeType:
            newval = ObjectId(unicode.encode(val))
            spec['_id'] = newval
        elif type(val) is types.ListType:
            newval = []
            for item in val:
                if  type(item) is types.StringType:
                    newval.append(ObjectId(item))
                elif type(item) is types.UnicodeType:
                    newval.append(ObjectId(unicode.encode(item)))
                else:
                     raise Exception('Wrong type for id, %s=%s' % (item, type(item)))
            spec['_id'] = newval
        query['spec'] = spec
    return query

def loose(query):
    """
    Construct loose query out of provided one. That means add a pattern '*' to
    string type values for all conditions. We use this to look-up similar records
    in DB.
    """
    spec    = getarg(query, 'spec', {})
    fields  = getarg(query, 'fields', None)
    newspec = {}
    for key, val in spec.items():
        if  key != '_id' and \
        type(val) is types.StringType or type(val) is types.UnicodeType:
            if  val[-1] != '*':
                val += '*' # add pattern
#            val = re.compile(val.replace('*', '.*'))
        newspec[key] = val
    return dict(spec=newspec, fields=fields)

def transform_keys(query, from_pat, to_pat):
    """
    Tranform MongoDB query who has keys with pattern from_pat
    into spec with keys of to_pat.
    """
    spec    = getarg(query, 'spec', {})
    fields  = getarg(query, 'fields', None)
    newspec = dict(spec)
    for key in spec.keys():
        if  key.find(from_pat) != -1:
            newspec[key.replace(from_pat, to_pat)] = spec[key]
            del newspec[key]
    return dict(spec=newspec, fields=fields)

def encode_mongo_keys(query):
    """
    Mongo doesn't allow to store a dictionary w/ key having a dot '.'
    For instance we can't store mongo queries themselves.
    To avoid this limitation we will use encode_mongo_keys to convert
    key.attr into key:attr, where simecolon we use as an example
    of key attribute separator (SEP).
    """
    return transform_keys(query, DOT, SEP)

def decode_mongo_keys(query):
    """
    Perform opposite to encode_mongo_keys action.
    Restore key.attr from key:attr
    """
    return transform_keys(query, SEP, DOT)

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
        else:
            newspec[key] = val
            verspec[key] = val
    return dict(spec=newspec, fields=fields), dict(spec=verspec, fields=fields)

def compare_dicts(input_dict, exist_dict):
    """
    Helper function for compare_specs. It compares key/val pairs of
    Mongo dict conditions, e.g. {'$gt':10}. Return true if exist_dict
    is superset of input_dict
    """
    for key, val in input_dict.items():
        if  exist_dict.has_key(key):
            vvv = exist_dict[key]
        if  key == '$gt':
            if  (type(val) is types.IntType or type(val) is types.FloatType)\
                and \
                (type(vvv) is types.IntType or type(vvv) is types.FloatType):
                if  val > vvv:
                    return True
        elif key == '$lt':
            if  (type(val) is types.IntType or type(val) is types.FloatType)\
                and \
                (type(vvv) is types.IntType or type(vvv) is types.FloatType):
                if  val < vvv:
                    return True
        elif key == '$in':
            if  type(val) is types.ListType and type(vvv) is types.ListType:
                if  set(vvv) > set(val):
                    return True
        return False

def compare_specs(input_query, exist_query):
    """
    Function to compare set of fields and specs of two input mongo
    queries. Return True if results of exist_query are superset 
    of resulst for input_query.
    """
    # we use notation query2 is superset of query1
    query1  = input_query
    query2  = exist_query
    fields1 = getarg(query1, 'fields', None)
    if  not fields1:
        fields1 = []
    spec1   = getarg(query1, 'spec', {})

    fields2 = getarg(query2, 'fields', None)
    if  not fields2:
        fields2 = []
    spec2   = getarg(query2, 'spec', {})

    if  spec2 == {}: # empty conditions for existing query, look at sel. fields
        if  set(fields2) > set(fields1): # set2 is superset of set1
            return True

#    if  set(spec2.keys()) > set(spec1.keys()):
#        return False

#    if  spec2.keys() != spec1.keys():
#        return False

    for key, val1 in spec1.items():
        try:
            val2 = spec2[key]
        except:
            continue
        if  (type(val1) is types.StringType or \
                type(val1) is types.UnicodeType) and \
            (type(val2) is types.StringType or \
                type(val2) is types.UnicodeType):
            if  val2.find('*') != -1:
                val1 = val1.replace('*', '')
                val2 = val2.replace('*', '')
                if  val1.find(val2) != -1:
                    return True # only if val2 is sub-string of val1
        elif type(val1) is types.DictType and type(val2) is types.DictType:
            return compare_dicts(val1, val2)
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
        self.cache_size = config['mongocache_bulkupdate_size']
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
        fields  = getarg(query, 'fields', {})
        newspec = {}
        verspec = {} # verbose spec
        # enable loose constraints, use LIKE pattern
        for key, val in spec.items():
            nkey = 'query.spec.%s' % key.replace(DOT, SEP) # see transform_keys
            if  type(val) is types.StringType or type(val) is types.UnicodeType:
                val = val[0] + '*'
                val = re.compile(val.replace('*', '.*')) #replace value to regex
                verspec[nkey] = val.pattern
            else:
                val = {'$ne': None} # non null key
                verspec[nkey] = val
            newspec[nkey] = val
        if  not newspec: # empty spec
            newspec = {'query.fields': fields}
            verspec = newspec
        else:
            newspec['das.system'] = system
        msg  = "DASMongocache::similar_queries, "
        msg += "loose condition query: verspec=%s, newspec=%s" % (verspec, newspec)
        self.logger.info(msg)
        func = "function(obj,prev){ return true;}"
        res  = self.col.group(['query'], newspec, 0, reduce=func)
        msg  = "DASMongocache::similar_queries, "
        msg += "found query which cover this request: "
        for row in res:
            existing_query = decode_mongo_keys(row['query'])
            if  compare_specs(query, existing_query):
                msg += str(existing_query)
                self.logger.info(msg)
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
        query  = adjust_id(query)
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
        query  = adjust_id(query)
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
        query  = adjust_id(query)
        query, dquery = convert2pattern(query)
        self.logger.info("DASMongocache::get_from_cache, converted to %s" \
                % dquery)
        idx    = int(idx)
        spec   = getarg(query, 'spec', {})
        fields = getarg(query, 'fields', None)
#        spec.update({'query.spec':{'$exists':False}}) # exclude query records
        if  limit:
            res = self.col.find(spec=spec, fields=fields)\
                .skip(idx).limit(limit)
        else:
            res = self.col.find(spec=spec, fields=fields)
        for row in res:
            obj_id = row['_id']
            row['_id'] = obj_id.url_encode()
            if  fields:
                fkeys = [k.split('.')[0] for k in fields]
                if  set(row.keys()) & set(fkeys) == set(fkeys):
                    yield row # only when row has all fields
            else:
                yield row

    def update_cache(self, query, results, header):
        """
        Insert results into cache.
        """
# TODO: I introduced usage of generator, for that I renamed old
#       update_cache in update_records and added new method update_cache
#        gen = self.update_records(query, results, header)
#        idx = 0
#        while True:
#            if  not self.col.insert(itertools.islice(gen, self.cache_size)):
#                break
#        lkeys      = header['lookup_keys']
#        index_list = [(key, DESCENDING) for key in lkeys]
#        if  index_list:
#            try:
#                self.col.ensure_index(index_list)
#            except:
#                pass
#    def update_records(self, query, results, header):
#        """
#        Iterate over provided results, update records and yield them
#        for final update_cache.
#        """
        self.logger.info("DASMongocache::update_cache(%s) store to cache" \
                % query)
        if  not results:
            return
        dasheader  = header['das']
        dasheader['selection_keys'] = header['selection_keys']

        # check first if we have any results in cache for this query
        query_in_cache = False
        if  self.incache(query):
            query_in_cache = True

        # insert query
        record = dict(query=encode_mongo_keys(query),
                 das=dict(expire=dasheader['expire'], 
                        system=dasheader['system']))
        self.col.insert(dict(record))

        # insert DAS records
        lkeys      = header['lookup_keys']
        index_list = [(key, DESCENDING) for key in lkeys]
        prim_key   = lkeys[0] # TODO: what to do with multiple look-up keys
        trigger    = 0
        local_cache = [] # small cache to be used for bulk updates
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
#                item['query'] = str_query
#                item['dashash'] = dashash # see above the dashash
                row = None
                if  query_in_cache:
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
                        if  len(local_cache) < self.cache_size:
                            local_cache.append(item)
                        else:
                            self.col.insert(local_cache)
                            local_cache = []
#                        self.col.insert(item)
#                        yield item
                else:
                    if  len(local_cache) < self.cache_size:
                        local_cache.append(item)
                    else:
                        self.col.insert(local_cache)
                        local_cache = []
#                    self.col.insert(item)
#                    yield item
                if  index_list:
                    try:
                        self.col.ensure_index(index_list)
                    except:
                        pass
            if  local_cache:
                self.col.insert(local_cache)
                local_cache = []
            if  not trigger: # we got empty results
                # we will insert empty record to avoid consequentive
                # calls to service who doesn't have data
                self.col.insert(dict(das=dasheader))
#                yield dict(das=dasheader)
        else:
            print "\n\n ### results = ", str(results)
            raise Exception('Provided results is not a list/generator type')

    def remove_from_cache(self, query):
        """
        Remove query from cache
        """
        query  = adjust_id(query)
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
