#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mongocache manager.
The DAS consists of several sub-systems:

    - DAS cache contains data records (output from data-services)
      and API records
    - DAS merge contains merged data records
    - DAS mapreduce collection
"""

__revision__ = "$Id: das_mongocache.py,v 1.86 2010/05/03 19:14:06 valya Exp $"
__version__ = "$Revision: 1.86 $"
__author__ = "Valentin Kuznetsov"

import re
import time
import types
import itertools

# DAS modules
from DAS.utils.utils import genkey
from DAS.utils.utils import aggregator
from DAS.core.das_son_manipulator import DAS_SONManipulator
import DAS.utils.jsonwrapper as json

# monogo db modules
from pymongo.connection import Connection
from pymongo.objectid import ObjectId
from pymongo.code import Code
from pymongo import DESCENDING, ASCENDING
from pymongo.errors import InvalidOperation

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
                    raise Exception('Wrong type for id, %s=%s' \
                        % (item, type(item)))
            spec['_id'] = newval
        query['spec'] = spec
    return query

def loose(query):
    """
    Construct loose query out of provided one. That means add a pattern '*' to
    string type values for all conditions. We use this to look-up similar records
    in DB.
    """
    spec    = query.get('spec', {})
    fields  = query.get('fields', None)
    newspec = {}
    for key, val in spec.items():
        if  key != '_id' and \
        type(val) is types.StringType or type(val) is types.UnicodeType:
            if  val[-1] != '*':
                val += '*' # add pattern
        newspec[key] = val
    return dict(spec=newspec, fields=fields)

#def encode_mongo_query(query):
#    """
#    Mongo doesn't allow to store a dictionary w/ key having a dot '.', '$'
#    notaions, therefor we will use string representation in DB for the query.
#    """
#    return json.dumps(query)

#def decode_mongo_query(query):
#    """
#    Perform opposite to encode_mongo_query action.
#    Restore query from a string.
#    """
#    return json.loads(query)
def encode_mongo_query(query):
    """
    Encode mongo query into storage format. MongoDB does not allow storage of
    dict with keys containing "." or MongoDB operators, e.g. $lt. So we
    convert input mongo query spec into list of dicts whose "key"/"value"
    are mongo query spec key/values. For example

    .. doctest::

        spec:{"block.name":"aaa"}

        converted into

        spec:[{"key":"block.name", "value":'"aaa"'}]

    Conversion is done using JSON dumps method.
    """
    return_query = dict(query)
    speclist = []
    for key, val in return_query.pop('spec').items():
        val = json.dumps(val)
        speclist.append({"key":key, "value":val})
    return_query['spec'] = speclist
    return return_query

def decode_mongo_query(query):
    """
    Decode query from storage format into mongo format.
    """
    spec = {}
    for item in query.pop('spec'):
        val = json.loads(item['value'])
        spec.update({item['key'] : val})
    query['spec'] = spec
    return query

def convert2pattern(query):
    """
    In MongoDB patterns are specified via regular expression.
    Convert input query condition into regular expression patterns.
    Return new MongoDB compiled w/ regex query and query w/ debug info.
    """
    spec    = query.get('spec', {})
    fields  = query.get('fields', None)
    newspec = {}
    verspec = {}
    for key, val in spec.items():
        if  type(val) is types.StringType or type(val) is types.UnicodeType:
            if  val.find('*') != -1:
                if  val == '*':
                    val = {'$exists':True}
                    verspec[key] = val
                else:
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
                    vcond[ckey] = cval
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
        elif  key == '$gte':
            if  (type(val) is types.IntType or type(val) is types.FloatType)\
                and \
                (type(vvv) is types.IntType or type(vvv) is types.FloatType):
                if  val >= vvv:
                    return True
        elif key == '$lt':
            if  (type(val) is types.IntType or type(val) is types.FloatType)\
                and \
                (type(vvv) is types.IntType or type(vvv) is types.FloatType):
                if  val < vvv:
                    return True
        elif key == '$lte':
            if  (type(val) is types.IntType or type(val) is types.FloatType)\
                and \
                (type(vvv) is types.IntType or type(vvv) is types.FloatType):
                if  val <= vvv:
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
    fields1 = query1.get('fields', None)
    if  not fields1:
        fields1 = []
    spec1   = query1.get('spec', {})

    fields2 = query2.get('fields', None)
    if  not fields2:
        fields2 = []
    spec2   = query2.get('spec', {})

    if  spec2 == {}: # empty conditions for existing query, look at sel. fields
        if  set(fields2) > set(fields1): # set2 is superset of set1
            return True

    if  spec2.keys() != spec1.keys():
        return False

    for key, val1 in spec1.items():
        val2 = spec2[key]
        if  type(val1) != type(val2) and type(val1) is not types.DictType\
            and type(val2) is not types.DictType:
            return False
        elif  (type(val1) is types.StringType or \
                type(val1) is types.UnicodeType) and \
            (type(val2) is types.StringType or \
                type(val2) is types.UnicodeType):
            if  val2.find('*') != -1:
                val1 = val1.replace('*', '')
                val2 = val2.replace('*', '')
                if  val1.find(val2) == -1:
                    return False
            else:
                val1 = val1.replace('*', '')
                val2 = val2.replace('*', '')
                if  val1 != val2:
                    return False
        elif type(val1) is types.DictType and type(val2) is types.DictType:
            if  not compare_dicts(val1, val2):
                return False
        elif type(val2) is types.DictType and type(val1) is types.IntType:
            if  val1 in val2.values():
                return True
            return False
        else:
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

def make_connection(dbhost, dbport, attempt):
    """
    Safely make connection to MongoDB.
    """
    if  not attempt or attempt < 0:
        msg = 'Unable to make connection to MongoDB after %s attempts' % attempt
        raise Exception(msg)
    try:
        conn = Connection(dbhost, dbport)
    except pymongo.errors.AutoReconnect:
        attempt -= 1
        make_connection(dbhost, dbport, attempt)
    except pymongo.errors.ConnectionFailure:
        attempt -= 1
        make_connection(dbhost, dbport, attempt)
    return conn

class DASMongocache(object):
    """
    DAS cache based MongoDB. 
    """
    def __init__(self, config):
        self.dbhost  = config['mongodb']['dbhost']
        self.dbport  = config['mongodb']['dbport']
        self.limit   = config['mongodb']['lifetime']
        self.attempt = config['mongodb']['attempt']
        self.cache_size = config['mongodb']['bulkupdate_size']
        self.dbname  = config['mongodb'].get('dbname', 'das')
        self.logger  = config['logger']
        self.verbose = config['verbose']

        msg = "DASMongocache::__init__ %s:%s@%s" \
        % (self.dbhost, self.dbport, self.dbname)
        self.logger.info(msg)

        self.conn    = make_connection(self.dbhost, self.dbport, self.attempt)
        self.db      = self.conn[self.dbname]
        self.col     = self.db['cache']
        self.mrcol   = self.db['mapreduce']
        self.merge   = self.db['merge']

        self.add_manipulator()
        
    def add_manipulator(self):
        """
        Add DAS-specific MongoDB SON manipulator to perform
        conversion of inserted data into DAS cache.
        """
        das_son_manipulator = DAS_SONManipulator()
        self.db.add_son_manipulator(das_son_manipulator)
        msg = "DASMongocache::__init__, DAS_SONManipulator %s" \
        % das_son_manipulator
        self.logger.debug(msg)

    def is_expired(self, query):
        """
        Check if we have query result is expired in cache.
        """
        return True

    def similar_queries(self, query):
        """
        Check if we have query results in cache whose conditions are
        superset of provided query. The method only works for single
        key whose value is substring of value in input query.
        For example, if cache contains records about T1 sites, 
        then input query T1_CH_CERN is subset of results stored in cache.
        """
        msg = "DASMongocache::similar_queries %s" % query
        spec    = query.get('spec', {})
        fields  = query.get('fields', None)
#        if  len(spec.keys()) > 1:
#            msg = 'DASMongocache::similar_queries, too many keys'
#            self.logger.info(msg)
#            return False
#        key     = spec.keys()[0]
#        val     = spec[key]
#        cond    = {'query.spec.key': key}
        cond    = {'query.spec.key': {'$in' : spec.keys()}}
        for row in self.col.find(cond):
            mongo_query = decode_mongo_query(row['query'])
            if  mongo_query['spec'] == query['spec'] or \
                compare_specs(query, mongo_query):
                self.logger.info("%s, True" % msg)
                return True
        self.logger.info("%s, False" % msg)
        return False

    def similar_queries_v1(self, system, query):
        """
        Check if we have query results in cache whose conditions are
        superset of provided query. For example, if cache contains records
        about T1 sites, then input query T1_CH_CERN is subset of results stored
        in cache.
        """
        self.logger.info("DASMongocache::similar_queries(%s)" % query)
        spec    = query.get('spec', {})
        fields  = query.get('fields', {})
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
        msg += "loose condition query: verspec=%s, newspec=%s" \
                % (verspec, newspec)
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

    def remove_expired(self, collection):
        """
        Remove expired records from DAS cache.
        """
        col  = self.db[collection]
        spec = {'das.expire' : {'$lt' : int(time.time())}}
        col.remove(spec)

    def find_spec(self, query):
        """
        Find if cache has query whose specs are identical to provided query.
        """
        enc_query = encode_mongo_query(query)
        cond = {'das.qhash': {"$exists":True}, "query.spec": enc_query['spec']}
        return self.col.find_one(cond)

    def das_record(self, query):
        """
        Retrieve DAS record for given query.
        """
        enc_query = encode_mongo_query(query)
        return self.col.find_one({'das.qhash': genkey(enc_query)})

    def add_to_record(self, query, info):
        """
        Add to existing DAS record provided info
        """
        enc_query = encode_mongo_query(query)
        self.col.update({'query': enc_query}, {'$set': info}, upsert=True)

    def update_das_record(self, query, status, header=None):
        """
        Update DAS record for provided query.
        """
        enc_query = encode_mongo_query(query)
        if  header:
            record = self.col.find_one({'query': enc_query})
            if  header['das']['expire'] < record['das']['expire']:
                expire = header['das']['expire']
            else:
                expire = record['das']['expire']
            api = header['das']['api']
            url = header['das']['url']
            if  set(api) & set(record['das']['api']) == set(api) and \
                set(url) & set(record['das']['url']) == set(url):
                self.col.update({'_id':ObjectId(record['_id'])}, 
                     {'$set': {'das.expire':expire, 'das.status':status}})
            else:
                self.col.update({'_id':ObjectId(record['_id'])}, 
                    {'$pushAll':{'das.api':header['das']['api'], 
                                 'das.system':header['das']['system'], 
                                 'das.url':header['das']['url'],
                                 'das.ctime':header['das']['ctime'],
                                 'das.lookup_keys':header['lookup_keys'],
                                }, 
                     '$set': {'das.expire':expire, 'das.status':status}})
        else:
            self.col.update({'query': enc_query},
                    {'$set': {'das.status': status}})

    def incache(self, query, collection='merge'):
        """
        Check if we have query results in cache, otherwise return null.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        col    = self.db[collection]
        self.remove_expired(collection)
        query  = adjust_id(query)
        spec   = query.get('spec', {})
        fields = query.get('fields', None)
        res    = col.find(spec=spec, fields=fields).count()
        msg    = "DASMongocache::incache(%s, coll=%s) found %s results"\
                % (query, collection, res)
        self.logger.info(msg)
        if  res:
            return True
        return False

    def nresults(self, query, collection='merge'):
        """
        Return number of results for given query.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        col    = self.db[collection]
        query  = adjust_id(query)
        query, dquery = convert2pattern(query)
        spec   = query.get('spec', {})
        fields = query.get('fields', None)
        # loop over fields, since user interesting to see only results for
        # provided fields. Update spec to check that given field exists
        # in a query
        if  fields:
            for field in fields:
                if  not spec.has_key(field):
                    spec.update({field:{'$exists':True}})
        self.logger.info("DASMongocache::nresults(%s, coll=%s) spec=%s" \
                % (query, collection, spec))
        return col.find(spec=spec).count()

    def get_from_cache(self, query, idx=0, limit=0, skey=None, order='asc', 
                        collection='merge', adjust=True):
        """
        Retreieve results from cache, otherwise return null.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        col = self.db[collection]
        msg = "DASMongocache::get_from_cache(%s, %s, %s, %s, %s, coll=%s)"\
                % (query, idx, limit, skey, order, collection)
        self.logger.info(msg)
        # get aggregators and loose query for further processing
        aggregators = query.get('aggregators', None)
        if  query.has_key('spec') and query['spec'].has_key('_id'):
            # we got {'_id': {'$in': [ObjectId('4b912ee7e2194e35b8000010')]}}
            strquery = ""
        else:
            strquery = json.dumps(query)
            query = loose(query)
        # adjust query id if it's requested
        if  adjust:
            query  = adjust_id(query)
            query, dquery = convert2pattern(query)
            self.logger.info("DASMongocache::get_from_cache, converted to %s"\
                    % dquery)
        idx    = int(idx)
        spec   = query.get('spec', {})
        fields = query.get('fields', None)

        # look-up API query
        recapi = self.col.find_one({"query":strquery})

        # look-up raw record
        if  fields:
            fields += ['das_id', 'das', 'cache_id'] # be sure to extract those fields
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
                    res = col.find(spec=spec, fields=fields)\
                        .sort(skeys).skip(idx).limit(limit)
                else:
                    res = col.find(spec=spec, fields=fields)\
                        .skip(idx).limit(limit)
            else:
                if  skeys:
                    res = col.find(spec=spec, fields=fields).sort(skeys)
                else:
                    res = col.find(spec=spec, fields=fields)
        except Exception as exp:
            row = {'exception': exp}
            yield row
        counter = 0
        for row in res:
            # DAS info is stored via das_id, the records only contains
            # {'das':{'expire':123}} to consistently manage delete operation
#            if  row.has_key('das'):
#                das = row['das']
#                if  not (type(das) is types.DictType and das.has_key('api')):
#                    del row['das']
            if  fields:
                fkeys = [k.split('.')[0] for k in fields]
                if  set(row.keys()) & set(fkeys) == set(fkeys):
                    counter += 1
                    yield row # only when row has all fields
            else:
                counter += 1
                yield row

        # check if aggregator info is present in records
        if  recapi and recapi.has_key('_id'):
            spec = {'das_id':recapi['_id'], 'aggregator':{'$exists':True}}
            res = self.col.find(spec)
            for row in res:
                counter += 1
                yield row

        if  counter:
            msg = "DASMongocache::get_from_cache, yield %s record(s)"\
                    % counter
            self.logger.info(msg)

        # if no raw records were yield we look-up possible error records
        counter = 0
        if  recapi and recapi.has_key('_id'):
            for key in spec.keys():
                qkey = '%s.error' % key.split(".")[0]
                spec = {'das_id':recapi['_id'], qkey:{'$exists':True}}
                res = self.col.find(spec)
                for row in res:
                    counter += 1
                    yield row
        if  counter:
            msg = "DASMongocache::get_from_cache, found %s error record(s)"\
                    % counter
            self.logger.info(msg)

    def map_reduce(self, mr_input, spec=None, collection='merge'):
        """
        Wrapper around _map_reduce to allow sequential map/reduce
        operations, e.g. map/reduce out of map/reduce. 

        mr_input is either alias name or list of alias names for
        map/reduce functions.
        spec is optional MongoDB query which is applied to first
        iteration of map/reduce functions.
        """
        if  type(mr_input) is not types.ListType:
            mrlist = [mr_input]
        else:
            mrlist = mr_input
        coll = self.db[collection]
        for mapreduce in mrlist:
            if  mapreduce == mrlist[0]:
                cond = spec
            else:
                cond = None
            coll = self._map_reduce(coll, mapreduce, cond)
        for row in coll.find():
            yield row

    def _map_reduce(self, coll, mapreduce, spec=None):
        """
        Perform map/reduce operation over DAS cache using provided
        collection, mapreduce name and optional conditions.
        """
        self.logger.info("DASMongocache::map_reduce(%s, %s)" \
                % (mapreduce, spec))
        record = self.mrcol.find_one({'name':mapreduce})
        if  not record:
            raise Exception("Map/reduce function '%s' not found" % mapreduce)
        fmap = record['map']
        freduce = record['reduce']
        if  spec:
            result = coll.map_reduce(Code(fmap), Code(freduce), query=spec)
        else:
            result = coll.map_reduce(Code(fmap), Code(freduce))
        msg = "DASMongocache::map_reduce found %s records in %s" \
                % (result.count(), result.name)
        self.logger.info(msg)
        self.logger.debug(fmap)
        self.logger.debug(freduce)
        return result

    def get_map_reduce(self, name=None):
        """
        Return definition of map/reduce functions for provided name
        or gives full list.
        """
        spec = {}
        if  name:
            spec = {'name':name}
        result = self.mrcol.find(spec)
        for row in result:
            yield row

    def merge_records(self, query):
        """
        Merge DAS records for provided query. We perform the following
        steps: 
        1. get all queries from das.cache by ordering them by primary key
        2. run aggregtor function to merge neighbors
        3. insert records into das.merge
        """
        self.logger.info("DASMongocache::merge_records(%s)" % query)
        lookup_keys = []
        id_list = []
        expire  = 9999999999 # future
        skey    = None # sort key
        # get all API records for given DAS query
        records = self.col.find({'query':encode_mongo_query(query)})
        for row in records:
#            lkeys = row['das']['lookup_keys']
            rec   = [k for i in row['das']['lookup_keys'] for k in i.values()]
            lkeys = list(set(k for i in rec for k in i))
            for key in lkeys:
                if  key not in lookup_keys:
                    lookup_keys.append(key)
            if  row['das']['expire'] < expire:
                expire = row['das']['expire']
            if  row['_id'] not in id_list:
                id_list.append(row['_id'])
        for pkey in lookup_keys:
            skey = [(pkey, DESCENDING)]
            # lookup all service records
            spec = {'das_id': {'$in': id_list}, 'das.primary_key': pkey}
            if  self.verbose:
                nrec = self.col.find(spec).sort(skey).count()
                msg  = "DASMongocache::merge_records, merging %s records"\
                        % nrec 
                msg += ", for %s key" % pkey
            else:
                msg  = "DASMongocache::merge_records, merging records"
                msg += ", for %s key" % pkey
            self.logger.info(msg)
            records = self.col.find(spec).sort(skey)
            # aggregate all records
            gen = aggregator(records, expire)
            # create index on all lookup keys
            try:
                self.merge.ensure_index(skey)
            except:
                pass
            # insert all records into das.merge using bulk insert
            size = self.cache_size
            try:
                while True:
                    if  not self.merge.insert(itertools.islice(gen, size)):
                        break
            except InvalidOperation:
                pass

    def update_cache(self, query, results, header):
        """
        Insert results into cache. Use bulk insert controller by
        self.cache_size. Upon completion ensure indexies.
        """
        # update query record in DAS cache
        self.update_query(query, header)

        # update results records in DAS cache
#        lkeys      = header['lookup_keys']
        rec   = [k for i in header['lookup_keys'] for k in i.values()]
        lkeys = list(set(k for i in rec for k in i))
        index_list = [(key, DESCENDING) for key in lkeys]
        if  index_list:
            try:
                self.col.ensure_index(index_list)
            except:
                pass
        gen = self.update_records(query, results, header)
        # bulk insert
        try:
            while True:
                if  not self.col.insert(itertools.islice(gen, self.cache_size)):
                    break
        except InvalidOperation:
            pass

    def update_query(self, query, header):
        """
        Update query in DAS cache.
        """
        msg = "DASMongocache::update_query, query=%s, header=%s" \
                % (query, header)
        self.logger.info(msg)
        dasheader  = header['das']
        lkeys      = header['lookup_keys']

        # check presence of API record in a cache
        enc_query = encode_mongo_query(query)
        spec = {'spec' : dict(query=enc_query)}
        if  self.incache(spec, collection='cache'):
            record = self.col.find_one({'query':enc_query}, fields=['_id'])
            objid  = record['_id']
        else:
            # insert query record for this set of results
            q_record = dict(das=dasheader, query=encode_mongo_query(query))
            q_record['das']['lookup_keys'] = lkeys
            objid  = self.col.insert(q_record)
            index_list = [('das.qhash', DESCENDING), 
                          ('query', DESCENDING),
                          ('query.spec', DESCENDING),
                         ]
            self.col.ensure_index(index_list)

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
#        lkeys      = header['lookup_keys']
        rec        = [k for i in header['lookup_keys'] for k in i.values()]
        lkeys      = list(set(k for i in rec for k in i))
        # get API record id
        enc_query  = encode_mongo_query(query)
        spec       = {'spec' : dict(query=enc_query)}
        record     = self.col.find_one({'query':enc_query}, fields=['_id'])
        objid      = record['_id']
        # insert DAS records
        prim_key   = lkeys[0] # what to do with multiple look-up keys
        counter    = 0
        if  type(results) is types.ListType or \
            type(results) is types.GeneratorType:
            for item in results:
                # TODO:
                # the exception/error records should not go to cache
                # instead we can place them elsewhere for further analysis
                if  item.has_key('exception') or item.has_key('error'):
                    continue
                counter += 1
                item['das'] = dict(expire=dasheader['expire'], primary_key=prim_key)
                item['das_id'] = str(objid)
                yield item
        else:
            print "\n\n ### results = ", str(results)
            raise Exception('Provided results is not a list/generator type')
        self.logger.info("\n")
        msg = "DASMongocache::update_cache, %s yield %s rows" \
                % (dasheader['system'], counter)
        self.logger.info(msg)

        # update das record with new status
        status = 'Update DAS cache, %s API' % header['das']['api'][0]
        self.update_das_record(query, status, header)

    def remove_from_cache(self, query):
        """
        Remove query from DAS cache. To do so, we retrieve API record
        and remove all data records from das.cache and das.merge
        """
        records = self.col.find({'query':encode_mongo_query(query)})
        id_list = []
        for row in records:
            if  row['_id'] not in id_list:
                id_list.append(row['_id'])
        spec = {'das_id':{'$in':id_list}}
        self.merge.remove(spec)
        self.col.remove(spec)
        self.col.remove({'query':encode_mongo_query(query)})

    def clean_cache(self):
        """
        Clean expired docs in das.cache and das.merge. 
        """
        current_time = time.time()
        query = {'das.expire': { '$lt':current_time} }
        self.merge.remove(query)
        self.col.remove(query)

    def delete_cache(self):
        """
        Delete all results in DAS cache/merge collection, including
        internal indexes.
        """
        self.col.remove({})
        try: # in pymongo 1.5.1 drop of indexes with non-existsing DB doesn't work
            self.col.drop_indexes()
        except:
            pass
        self.merge.remove({})
        try:
            self.merge.drop_indexes()
        except:
            pass

