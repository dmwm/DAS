#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mongocache wrapper.
"""

__revision__ = "$Id: das_mongocache.py,v 1.44 2009/12/07 21:21:57 valya Exp $"
__version__ = "$Revision: 1.44 $"
__author__ = "Valentin Kuznetsov"

import re
import time
import types
import itertools

# DAS modules
from DAS.utils.utils import getarg, dict_value, merge_dict, genkey
from DAS.core.cache import Cache
from DAS.core.das_son_manipulator import DAS_SONManipulator
import DAS.utils.jsonwrapper as json

# monogo db modules
from pymongo.connection import Connection
from pymongo.objectid import ObjectId
from pymongo import DESCENDING, ASCENDING

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
        newspec[key] = val
    return dict(spec=newspec, fields=fields)

def encode_mongo_query(query):
    """
    Mongo doesn't allow to store a dictionary w/ key having a dot '.', '$'
    notaions, therefor we will use string representation in DB for the query.
    """
    return json.dumps(query)

def decode_mongo_query(query):
    """
    Perform opposite to encode_mongo_query action.
    Restore query from a string.
    """
    return json.loads(query)

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

    if  spec2.keys() != spec1.keys():
        return False

    for key, val1 in spec1.items():
        val2 = spec2[key]
        if  (type(val1) is types.StringType or \
                type(val1) is types.UnicodeType) and \
            (type(val2) is types.StringType or \
                type(val2) is types.UnicodeType):
            if  val2.find('*') != -1:
                val1 = val1.replace('*', '')
                val2 = val2.replace('*', '')
                if  val1.find(val2) == -1:
                    return False
            else:
                if  val1 != val2:
                    return False
        elif type(val1) is types.DictType and type(val2) is types.DictType:
            if  not compare_dicts(val1, val2):
                return False
    return True

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

# Not ready yet
#        self.add_manipulator()
        
    def add_manipulator(self):
        """
        Add DAS-specific MongoDB SON manipulator to perform
        conversion of inserted data into DAS cache.
        """
        mapping_db  = self.conn['mapping']
        collection  = mapping_db['db']
        notationmap = {}
        spec = {'notations':{'$ne':None}}
        for item in collection.find(spec):
            notationmap[item['system']] = item['notations']
        das_son_manipulator = DAS_SONManipulator(notationmap)
        self.db.add_son_manipulator(das_son_manipulator)
        msg = "DASMongocache::__init__, DAS_SONManipulator %s" \
        % das_son_manipulator
        self.logger.info(msg)

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
        spec    = getarg(query, 'spec', {})
        fields  = getarg(query, 'fields', {})
        newspec = {}
        verspec = {} # verbose spec
        # enable loose constraints, use LIKE pattern
        for key, val in spec.items():
            nkey = 'query.spec.%s' % key.replace(DOT, SEP) 
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
            existing_query = decode_mongo_query(row['query'])
            if  compare_specs(query, existing_query):
                msg += str(existing_query)
                self.logger.info(msg)
                return True
        return False

    def remove_expired(self):
        """
        Remove expired records from DAS cache.
        """
        spec   = {'das.expire' : {'$lt' : int(time.time())}}
#        fields = ['_id']
#        for row in self.col.find(spec, fields):
#            objid = row['_id'].url_encode()
#            self.col.remove({'das_id':objid})
        self.col.remove(spec)

    def incache(self, query):
        """
        Check if we have query results in cache, otherwise return null.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        self.remove_expired()
        query  = adjust_id(query)
        spec   = getarg(query, 'spec', {})
        fields = getarg(query, 'fields', None)
        res    = self.col.find(spec=spec, fields=fields).count()
        self.logger.info("DASMongocache::incache(%s) found %s results" \
        % (query, res))
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
        if  fields:
            fields += ['das_id'] # always extract das_id's
        skeys  = []
        if  skey:
            if  order == 'asc':
                skeys = [(skey, ASCENDING)]
            else:
                skeys = [(skey, DESCENDING)]
        ### The date is special key in DAS, data-services doesn't provide
        ### it, so we must drop it.
        if  spec.has_key('date'):
            del spec['date']
        res = []
        try:
            if  limit:
                if  skeys:
                    res = self.col.find(spec=spec, fields=fields)\
                        .sort(skeys).skip(idx).limit(limit)
                else:
                    res = self.col.find(spec=spec, fields=fields)\
                        .skip(idx).limit(limit)
            else:
                if  skeys:
                    res = self.col.find(spec=spec, fields=fields).sort(skeys)
                else:
                    res = self.col.find(spec=spec, fields=fields)
        except Exception as exp:
            row = {'exception': exp}
            yield row
        for row in res:
            # TODO: use this if there is no das_son_manipulator
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
        Insert results into cache. Use bulk insert controller by
        self.cache_size. Upon completion ensure indexies.
        """
        gen = self.update_records(query, results, header)
        idx = 0
        while True:
            if  not self.col.insert(itertools.islice(gen, self.cache_size)):
                break
        lkeys      = header['lookup_keys']
        index_list = [(key, DESCENDING) for key in lkeys]
        if  index_list:
            try:
                self.col.ensure_index(index_list)
            except:
                pass

    def update_records(self, query, results, header):
        """
        Iterate over provided results, update records and yield them
        to next level (update_cache)
        """
        self.logger.info("DASMongocache::update_cache(%s) store to cache" \
                % query)
        if  not results:
            return
        dasheader  = header['das']
        dasheader['selection_keys'] = header['selection_keys']

        # check presence of query in a cache regardless of the system
        # and insert it for this system
#        record = dict(query=encode_mongo_query(query),
#                 das=dict(expire=dasheader['expire'], 
#                        system=dasheader['system']))
        query_in_cache = False
        spec = {'spec' : dict(query=encode_mongo_query(query))}
        if  self.incache(spec):
            query_in_cache = True
#        self.col.insert(dict(record))

        # insert das record for this set of results
        das_record = dict(das=dasheader, query=encode_mongo_query(query))
        objid = self.col.insert(das_record)

        # insert DAS records
        lkeys       = header['lookup_keys']
        prim_key    = lkeys[0] # TODO: what to do with multiple look-up keys
        counter     = 0
        merge_count = 0
        if  type(results) is types.ListType or \
            type(results) is types.GeneratorType:
            for item in results:
                # TODO:
                # the exception/error records should not go to cache
                # instead we can place them elsewhere for further analysis
                if  item.has_key('exception') or item.has_key('error'):
                    continue
                counter += 1
#                item['das'] = dasheader
                item['das'] = dict(expire=dasheader['expire'])
                item['das_id'] = objid.url_encode()
                row = None
                if  query_in_cache:
                    try:
                        entry = dict_value(item, prim_key)
                        if  entry:
                            row = self.col.find_one({prim_key:entry})
                    except:
                        row = None
                        pass
                if  row:
                    value = dict_value(row, prim_key)
                    if  value == entry: # we found a match in cache
#                        mdict = merge_dict(item, row)
#                        mdict.pop('_id')
#                        self.col.insert(mdict)
                        merge_dict(item, row)
                        item.pop('_id')
                        self.col.insert(item)
                        obj_id = ObjectId(row['_id'])
                        self.col.remove({'_id': obj_id})
                        merge_count += 1
                        item.clear()
                        row.clear()
                    else:
                        yield item
                else:
                    yield item
            if  not counter: # we got empty results
                # we will insert empty record to avoid consequentive
                # calls to service who doesn't have data
                yield dict(das=dasheader)
        else:
            print "\n\n ### results = ", str(results)
            raise Exception('Provided results is not a list/generator type')
        msg = "DASMongocache::update_cache, %s yield %s rows/%s merged" \
                % (dasheader['system'], counter, merge_count)
        self.logger.info(msg)

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
