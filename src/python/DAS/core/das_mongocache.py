#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mongocache wrapper.
"""

__revision__ = "$Id: das_mongocache.py,v 1.20 2009/10/19 02:28:03 valya Exp $"
__version__ = "$Revision: 1.20 $"
__author__ = "Valentin Kuznetsov"

import re
import time
import types
import DAS.utils.jsonwrapper as json
from itertools import groupby

# DAS modules
from DAS.utils.utils import getarg, dict_value, merge_dict, genkey
from DAS.core.cache import Cache

# monogo db modules
from pymongo.connection import Connection
from pymongo import DESCENDING

_dot = '.'
_sep = '___'

def loose(query):
    spec    = getarg(query, 'spec', {})
    fields  = getarg(query, 'fields', None)
    newspec = {}
    for key, val in spec.items():
        if  type(val) is types.StringType or type(val) is types.UnicodeType:
            val = val + '.*'
            val = re.compile(val.replace('*', '.*'))
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
    of key attribute separator (_sep).
    """
    return transform_keys(query, _dot, _sep)

def decode_mongo_keys(query):
    """
    Perform opposite to encode_mongo_keys action.
    Restore key.attr from key:attr
    """
    return transform_keys(query, _sep, _dot)

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
            if  (type(val) is types.IntType or types(val) is types.FloatType)\
                and \
                (type(vvv) is types.IntType or types(vvv) is types.FloatType):
                if  val > vvv:
                    return True
        elif key == '$lt':
            if  (type(val) is types.IntType or types(val) is types.FloatType)\
                and \
                (type(vvv) is types.IntType or types(vvv) is types.FloatType):
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
            nkey = 'query.spec.%s' % key.replace(_dot, _sep) # see transform_keys
            if  type(val) is types.StringType or type(val) is types.UnicodeType:
                val = val[0] + '*'
                val = re.compile(val.replace('*', '.*')) # replace value to regex
                verspec[nkey] = val.pattern
            else:
                verspec[nkey] = val
                val = {'$ne': None} # non null key
            newspec[nkey] = val
        newspec['das.system'] = system

# TODO: using loose conditions is probably wrong approach, since some data-services
# like SiteDB, doesn't store query parameters, so if I look-up SE's by providing
# CMSName SiteDB API doesn't store CMSName, I store what I get from the query, e.g. T1_
# But it is not sufficient to obtain queries with similary conditions.
# Instead I need to find out a way to query similar conditions. For that I need
# to store conditions as dicts, rather then string (for that I need to replace
# dot '.' in a keys, site.name) and apply query here on conditions.
# In this case I don't need  compare_specs since I will query on
# superset of conditions.
        msg  = "DASMongocache::similar_queries, "
        msg += "loose query: %s" % verspec
        self.logger.info(msg)
        reduce = "function(obj,prev){ return true;}"
        res  = self.col.group(['query'], newspec, 0, reduce=reduce)
        msg  = "DASMongocache::similar_queries, found query which cover this request: "
        for row in res:
            print "\n\n### Found similar query condition", row
            existing_query = decode_mongo_keys(row['query'])
            print "\nfrom string"
            print "input_query", query
            print "exist_query", existing_query
            print "comparisoin", compare_specs(query, existing_query)
            if  compare_specs(query, existing_query):
                msg += str(existing_query)
                self.logger.info(msg)
                return True
        return False

    def version_2(self):
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
        msg  = "DASMongocache::similar_queries, found query which cover this request: "
        for row in res:
            if  type(row['query']) is types.StringType or \
                type(row['query']) is types.UnicodeType:
                existing_query = json.loads(row['query'])
                print "\nfrom string"
                print "input_query", query
                print "exist_query", existing_query
                print "comparisoin", compare_specs(query, existing_query)
                if  compare_specs(query, existing_query):
                    msg += str(existing_query)
                    self.logger.info(msg)
                    return True
            elif type(row['query']) is types.ListType:
                gen = (k for k, g in groupby(row['query']))
                for item in gen:
                    existing_query = json.loads(item)
                    print "\nfrom list"
                    print "input_query", query
                    print "exist_query", existing_query
                    print "comparisoin", compare_specs(query, existing_query)
                    if  compare_specs(query, existing_query):
                        msg += str(existing_query)
                        self.logger.info(msg)
                        return True
            else:
                raise TypeError('Unexpected type for query %s, %s' \
                % (row['query'], type(row['query'])))
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
#        str_query  = json.dumps(query)
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
#                item['query'] = str_query
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
