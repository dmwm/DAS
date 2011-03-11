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
from   types import GeneratorType
import random
import itertools
import traceback
import fnmatch

# DAS modules
from DAS.utils.utils import genkey, convert_dot_notation, aggregator
from DAS.utils.utils import adjust_mongo_keyvalue
from DAS.core.das_son_manipulator import DAS_SONManipulator
from DAS.utils.das_db import db_connection
from DAS.utils.utils import parse_filters
from DAS.utils.das_db import db_gridfs, parse2gridfs
import DAS.utils.jsonwrapper as json

# monogo db modules
from pymongo.objectid import ObjectId
from pymongo.code import Code
from pymongo import DESCENDING, ASCENDING
from pymongo.errors import InvalidOperation
from bson.errors import InvalidDocument

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
        if  isinstance(val, str):
            newval = ObjectId(val)
            spec['_id'] = newval
        elif isinstance(val, unicode):
            newval = ObjectId(unicode.encode(val))
            spec['_id'] = newval
        elif isinstance(val, list):
            newval = []
            for item in val:
                if  isinstance(item, str):
                    newval.append(ObjectId(item))
                elif isinstance(item, unicode):
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
        isinstance(val, str) or isinstance(val, unicode):
            if  val[-1] != '*':
                val += '*' # add pattern
        newspec[key] = val
    return dict(spec=newspec, fields=fields)

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
    if  not query:
        msg = 'Cannot decode, input query=%s' % query
        raise Exception(msg)
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
        if  key == 'records':
            continue # skip special records keyword
        if  isinstance(val, str) or isinstance(val, unicode):
            if  val.find('*') != -1:
                if  val == '*':
                    val = {'$exists':True}
                    verspec[key] = val
                else:
                    val = re.compile("^%s" % val.replace('*', '.*'))
                    verspec[key] = val.pattern
            else:
                verspec[key] = val
            newspec[key] = val
        elif isinstance(val, dict):
            cond  = {}
            vcond = {}
            for ckey, cval in val.items():
                if  isinstance(cval, str) or isinstance(cval, unicode):
                    if  cval.find('*') != -1:
                        cval = re.compile("^%s" % cval.replace('*', '.*'))
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
        signal = False
        vvv = None
        if  exist_dict.has_key(key):
            vvv = exist_dict[key]
        cond = (isinstance(val, int) or isinstance(val, float)) and \
               (isinstance(vvv, int) or isinstance(vvv, float))
        if  key == '$gt':
            if  cond and val > vvv:
                signal = True
        elif  key == '$gte':
            if  cond and val >= vvv:
                signal = True
        elif key == '$lt':
            if  cond and val < vvv:
                signal = True
        elif key == '$lte':
            if  cond and val <= vvv:
                signal = True
        elif key == '$in':
            if  isinstance(val, list) and isinstance(vvv, list):
                if  set(vvv) > set(val):
                    signal = True
        if signal == False:
            return False
    return True

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

    if  fields1 != fields2:
        if  fields1 and fields2 and not set(fields2) > set(fields1):
            return False
        elif fields1 and not fields2:
            return False
        elif fields2 and not fields1:
            return False

    spec2   = query2.get('spec', {})

    if  spec2 == {}: # empty conditions for existing query, look at sel. fields
        if  set(fields2) > set(fields1): # set2 is superset of set1
            return True

    if  spec2.keys() != spec1.keys():
        return False

    for key, val1 in spec1.items():
        val2 = spec2[key]
        if (isinstance(val1, str) or isinstance(val1, unicode)) and \
             (isinstance(val2, str) or isinstance(val2, unicode)):
            if  val2.find('*') != -1:
                val1 = val1.replace('*', '')
                val2 = val2.replace('*', '')
                if  val1.find(val2) == -1:
                    return False
            else:
                orig = val1
                val1 = val1.replace('*', '')
                val2 = val2.replace('*', '')
                if  val1 != val2:
                    return False
                elif orig.find('*') != -1 and val1 == val2:
                    return False # compare A* with existing A
        elif  type(val1) != type(val2) and not isinstance(val1, dict)\
            and not isinstance(val2, dict):
            return False
        elif isinstance(val1, dict) and isinstance(val2, dict):
            if  not compare_dicts(val1, val2):
                return False
        elif isinstance(val2, dict) and isinstance(val1, int):
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
    if  not isinstance(val, dict):
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

class DASMongocache(object):
    """
    DAS cache based MongoDB. 
    """
    def __init__(self, config):
        self.dburi   = config['mongodb']['dburi']
        self.cache_size = config['mongodb']['bulkupdate_size']
        self.dbname  = config['dasdb']['dbname']
        self.logger  = config['logger']
        self.verbose = config['verbose']

        self.conn    = db_connection(self.dburi)
        self.mdb     = self.conn[self.dbname]
        self.col     = self.mdb[config['dasdb']['cachecollection']]
        self.mrcol   = self.mdb[config['dasdb']['mrcollection']]
        self.merge   = self.mdb[config['dasdb']['mergecollection']]
        self.gfs     = db_gridfs(self.dburi)

        msg = "DASMongocache::__init__ %s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)

        self.add_manipulator()

        # ensure that we have the following indexes
        index_list = [('das.expire', ASCENDING), ('query.spec.key', ASCENDING),
                      ('das.qhash', DESCENDING), ('das.empty_record', ASCENDING),
                      ('query', DESCENDING), ('query.spec', DESCENDING)]
        self.indexes(self.col, index_list)
        index_list = [('das.expire', ASCENDING), ('das_id', ASCENDING),
                      ('das.empty_record', ASCENDING)]
        self.indexes(self.merge, index_list)
        
    def indexes(self, coll, index_list):
        """
        Invoke ensure_index for provided collection and index_list
        """
        for pair in index_list:
            coll.ensure_index([pair])

    def add_manipulator(self):
        """
        Add DAS-specific MongoDB SON manipulator to perform
        conversion of inserted data into DAS cache.
        """
        das_son_manipulator = DAS_SONManipulator()
        self.mdb.add_son_manipulator(das_son_manipulator)
        msg = "DASMongocache::__init__, DAS_SONManipulator %s" \
        % das_son_manipulator
        self.logger.debug(msg)

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
        cond    = {'query.spec.key': {'$in' : spec.keys()}}
        for row in self.col.find(cond):
            mongo_query = decode_mongo_query(row['query'])
            if  compare_specs(query, mongo_query):
                self.logger.info("%s, True" % msg)
                self.logger.info("similar to %s" % mongo_query)
                return mongo_query
        self.logger.info("%s, False" % msg)
        return False
    
    def get_superset_keys(self, key, value):
        """
        This is a special-case version of similar_keys,
        intended for analysers that want to quickly
        find possible superset queries of a simple
        query of the form key=value.
        """
        
        msg = "DASMongocache::get_superset_keys %s=%s" % (key, value)
        self.logger.debug(msg)
        cond = {'query.spec.key': key}
        found_keys = []
        for row in self.col.find(cond):
            mongo_query = decode_mongo_query(row['query'])
            for thiskey, thisvalue in mongo_query.items():
                if thiskey == key:
                    if fnmatch.fnmatch(value, thisvalue):
                        yield thisvalue
        
    def remove_expired(self, collection):
        """
        Remove expired records from DAS cache.
        """
        timestamp = int(time.time())
        col  = self.mdb[collection]
        spec = {'das.expire' : {'$lt' : timestamp}}
        if  self.verbose:
            nrec = col.find(spec).count()
            msg  = "DASMongocache::remove_expired, will remove %s records"\
                 % nrec
            msg += ", localtime=%s" % timestamp
            self.logger.debug(msg)
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
    
    def find_records(self, das_id):
        """
        Return all the records matching a given das_id
        """
        return self.col.find({'das_id': das_id})

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
                                 'das.urn':header['das']['api'],
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
        self.remove_expired(collection)
        col    = self.mdb[collection]
        query  = adjust_id(query)
        spec   = query.get('spec', {})
        fields = query.get('fields', None)
        if spec.has_key('date') and isinstance(spec['date'], dict)\
             and spec['date'].has_key('$lte') \
             and spec['date'].has_key('$gte'):
             spec['date'] = [spec['date']['$gte'], spec['date']['$lte']]

        res    = col.find(spec=spec, fields=fields).count()
        msg    = "DASMongocache::incache(%s, coll=%s) found %s results"\
                % (query, collection, res)
        self.logger.info(msg)
        if  res:
            return True
        return False

    def nresults(self, query, collection='merge', filters=None):
        """
        Return number of results for given query.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        col    = self.mdb[collection]
        query  = adjust_id(query)
        query, dquery = convert2pattern(query)
        spec   = query.get('spec', {})
        fields = query.get('fields', None)
        # to cover the case of one key=value pair, e.g. site=T1,
        # to return only records associated with requested key (site and not
        # blocks) loop over fields and update spec with primary key constructed
        # from a field key
        if  fields:
            for field in fields:
                if  not spec.has_key(field):
                    prim_key = re.compile("^%s" % field) 
                    spec.update({'das.primary_key': prim_key})

        # always look-up non-empty records
        spec.update({"das.empty_record":0})

        if  filters:
            new_query = dict(query)
            new_query.update({'filters': filters})
            filter_dict = parse_filters(new_query)
            if  filter_dict:
                spec.update(filter_dict)
        msg = 'DASMongocache::nresults(%s, %s, %s) spec=%s' \
                % (query, collection, filters, spec)
        self.logger.debug(msg)
        for key, val in spec.items():
            if  val == '*':
                del spec[key]
        # usage of fields in find doesn't account for counting, since it
        # is a view over records found with spec, so we don't need to use it.
        res = col.find(spec=spec).count()
        msg = "DASMongocache::nresults=%s" % res
        self.logger.info(msg)
        return res

    def get_from_cache(self, query, idx=0, limit=0, skey=None, order='asc', 
                        collection='merge', adjust=True):
        """
        Retreieve results from cache, otherwise return null.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        col = self.mdb[collection]
        msg = "DASMongocache::get_from_cache(%s, %s, %s, %s, %s, coll=%s)"\
                % (query, idx, limit, skey, order, collection)
        self.logger.debug(msg)
        # get aggregators and loose query for further processing
        aggregators = query.get('aggregators', None)
        if  query.has_key('spec') and query['spec'].has_key('_id'):
            # we got {'_id': {'$in': [ObjectId('4b912ee7e2194e35b8000010')]}}
            strquery = ""
        else:
            strquery = json.dumps(query)
        if  query.has_key('spec') and query['spec'].has_key('date'):
            if isinstance(query['spec']['date'], dict)\
                and query['spec']['date'].has_key('$lte') \
                and query['spec']['date'].has_key('$gte'):
                query['spec']['date'] = [query['spec']['date']['$gte'],
                                         query['spec']['date']['$lte']]

# TODO: investigate why do I need to loose the query????
# 20110120 (ticket #960)
#            query = loose(query)
        # adjust query id if it's requested
        if  adjust:
            query  = adjust_id(query)
            query, dquery = convert2pattern(query)
            self.logger.debug("DASMongocache::get_from_cache, converted to %s"\
                    % dquery)
        idx    = int(idx)
        spec   = query.get('spec', {})
        fields = query.get('fields', None)

        # always look-up non-empty records
        if  spec:
            spec.update({'das.empty_record':0})

        # look-up API query
        recapi = self.col.find_one({"query":strquery})

        # look-up raw record
        if  fields: # be sure to extract those fields
            fields += ['das_id', 'das', 'cache_id']
        skeys  = []
        if  skey:
            if  order == 'asc':
                skeys = [(skey, ASCENDING)]
            else:
                skeys = [(skey, DESCENDING)]
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
        if  not isinstance(mr_input, list):
            mrlist = [mr_input]
        else:
            mrlist = mr_input
        coll = self.mdb[collection]
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
        self.logger.debug("DASMongocache::map_reduce(%s, %s)" \
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
        self.logger.debug("DASMongocache::merge_records(%s)" % query)
        lookup_keys = []
        id_list = []
        expire  = 9999999999 # future
        skey    = None # sort key
        # get all API records for given DAS query
        records = self.col.find({'query':encode_mongo_query(query)})
        for row in records:
            try:
                rec   = \
                [k for i in row['das']['lookup_keys'] for k in i.values()]
            except:
                traceback.print_exc()
                print "Fail with record:", row
                continue
            lkeys = list(set(k for i in rec for k in i))
            for key in lkeys:
                if  key not in lookup_keys:
                    lookup_keys.append(key)
            # find smallest expire timestamp to be used by aggregator
            if  row['das']['expire'] < expire:
                expire = row['das']['expire']
            if  row['_id'] not in id_list:
                id_list.append(row['_id'])
        inserted = 0
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
            self.logger.debug(msg)
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
                        inserted = 1
                        break
            except InvalidDocument as exp:
                msg = "Caught bson error: " + str(exp)
                self.logger.info(msg)
                records = self.col.find(spec).sort(skey)
                gen = aggregator(records, expire)
                genrows = parse2gridfs(self.gfs, pkey, gen, self.logger)
                das_dict = {'das':{'expire':expire, 'primary_key':lookup_keys,
                        'empty_record': 0}, 'cache_id':[], 'das_id': id_list}
                for row in genrows:
                    row.update(das_dict)
                    self.merge.insert(row)
            except InvalidOperation:
                pass
        if  not inserted: # we didn't merge anything
            empty_record = {'das':{'expire':expire, 'primary_key':lookup_keys,
                                   'empty_record': 1}, 
                            'cache_id':[], 'das_id': id_list}
            spec = query['spec']
            if  isinstance(spec, dict):
                spec = [spec]
            for key, val in query['spec'].items():
                if  key.find('.') == -1:
                    empty_record[key] = []
                else: # it is compound key, e.g. site.name
                    newkey, newval = convert_dot_notation(key, val)
                    empty_record[newkey] = adjust_mongo_keyvalue(newval)
            self.merge.insert(empty_record)

    def update_cache(self, query, results, header):
        """
        Insert results into cache. Use bulk insert controller by
        self.cache_size. Upon completion ensure indexies.
        """
        # insert/check query record in DAS cache
        self.insert_query_record(query, header)

        # update results records in DAS cache
        rec   = [k for i in header['lookup_keys'] for k in i.values()]
        lkeys = list(set(k for i in rec for k in i))
        index_list = [(key, DESCENDING) for key in lkeys]
        self.indexes(self.col, index_list)
        gen = self.update_records(query, results, header)
        # bulk insert
        try:
            while True:
                if  not self.col.insert(itertools.islice(gen, self.cache_size)):
                    break
        except InvalidOperation:
            pass

    def insert_query_record(self, query, header):
        """
        Insert query record into DAS cache.
        """
        dasheader  = header['das']
        lkeys      = header['lookup_keys']
        # check presence of API record in a cache
        enc_query = encode_mongo_query(query)
        spec = {'spec' : dict(query=enc_query)}
        if  not self.incache(spec, collection='cache'):
            msg = "DASMongocache::insert_query_record, query=%s, header=%s" \
                    % (query, header)
            self.logger.debug(msg)
            q_record = dict(das=dasheader, query=enc_query)
            q_record['das']['lookup_keys'] = lkeys
            q_record['das']['empty_record'] = 0
            q_record['das']['status'] = "requested"
            q_record['das']['qhash'] = genkey(enc_query)
            objid  = self.col.insert(q_record)

    def update_records(self, query, results, header):
        """
        Iterate over provided results, update records and yield them
        to next level (update_cache)
        """
        self.logger.debug("DASMongocache::update_cache(%s) store to cache" \
                % query)
        if  not results:
            return
        dasheader  = header['das']
        expire     = dasheader['expire']
        system     = dasheader['system']
        rec        = [k for i in header['lookup_keys'] for k in i.values()]
        lkeys      = list(set(k for i in rec for k in i))
        # get API record id
        enc_query  = encode_mongo_query(query)
        qhash      = genkey(enc_query)
        spec       = {'spec' : dict(query=enc_query)}
        record     = self.col.find_one({'query':enc_query}, fields=['_id'])
        objid      = record['_id']
        # insert DAS records
        prim_key   = rec[0][0]#use rec instead of lkeys[0] which re-order items
        counter    = 0
        if  isinstance(results, list) or isinstance(results, GeneratorType):
            for item in results:
                # TODO:
                # the exception/error records should not go to cache
                # instead we can place them elsewhere for further analysis
                if  item.has_key('exception') or item.has_key('error'):
                    continue
                counter += 1
                item['das'] = dict(expire=expire, primary_key=prim_key, 
                                        system=system, empty_record=0)
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
        try: 
            self.col.drop_indexes()
        except:
            pass
        self.merge.remove({})
        try:
            self.merge.drop_indexes()
        except:
            pass

