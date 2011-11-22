#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0703

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
import itertools
import fnmatch

# DAS modules
from DAS.core.das_son_manipulator import DAS_SONManipulator
from DAS.utils.ddict import convert_dot_notation
from DAS.utils.utils import genkey, aggregator, unique_filter
from DAS.utils.utils import adjust_mongo_keyvalue, print_exc, das_diff
from DAS.utils.utils import parse_filters, expire_timestamp
from DAS.utils.utils import dastimestamp, deepcopy
from DAS.utils.das_db import db_connection
from DAS.utils.das_db import db_gridfs, parse2gridfs, create_indexes
from DAS.utils.logger import PrintManager
import DAS.utils.jsonwrapper as json

# monogo db modules
from pymongo.objectid import ObjectId
from pymongo.code import Code
from pymongo import DESCENDING, ASCENDING
from pymongo.errors import InvalidOperation
from bson.errors import InvalidDocument

DOT = '.'
SEP = '___'

def check_unique_filter(query):
    """Check if query carries unique filter"""
    filters     = query.get('filters', None)
    if  filters:
        unique  = bool('unique' in filters)
    else:
        unique  = False
    return unique

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
    system  = query.get('system', None)
    inst    = query.get('instance', None)
    newspec = {}
    for key, val in spec.items():
        if  key != '_id' and \
        isinstance(val, str) or isinstance(val, unicode):
            if  val[-1] != '*':
                val += '*' # add pattern
        newspec[key] = val
    lquery = dict(spec=newspec, fields=fields)
    if  system:
        lquery.update({'system': system})
    if  inst:
        lquery.update({'instance': inst})
    return lquery

def encode_mongo_query(query, pattern=False):
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
        if  str(type(val)) == "<type '_sre.SRE_Pattern'>":
            val = json.dumps(val.pattern)
            speclist.append({"key":key, "value":val, "pattern":1})
        else:
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
        if  item.has_key('pattern'):
            val = re.compile(val)
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
    filters = query.get('filters', None)
    aggs    = query.get('aggregators', None)
    inst    = query.get('instance', None)
    system  = query.get('system', None)
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
    newquery = dict(spec=newspec, fields=fields)
    newdquery = dict(spec=verspec, fields=fields)
    if  filters:
        newquery.update({'filters': filters})
        newdquery.update({'filters': filters})
    if  aggs:
        newquery.update({'aggregators': aggs})
        newdquery.update({'aggregators': aggs})
    if  inst:
        newquery.update({'instance': inst})
        newdquery.update({'instance': inst})
    if  system:
        newquery.update({'system': system})
        newdquery.update({'system': system})
    return newquery, newdquery

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

def compare_str(query1, query2):
    """
    Function to compare string from specs of query.
    Return True if query2 is supperset of query1.
    query1&query2 is the string in the pattern::

         ([a-zA-Z0-9_\-\#/*\.])*

    \* is the sign indicates that a sub string of \*::

        ([a-zA-Z0-9_\-\#/*\.])*

    case 1. if query2 is flat query(w/out \*) then query1 must be the same flat one

    case 2. if query1 is start/end w/ \* then query2 must start/end with \*

    case 3. if query2 is start/end w/out \* then query1 must start/end with query2[0]/query[-1]

    case 4. query1&query2 both include \*

        Way to perform a comparision is spliting:

            query1 into X0*X1*X2*X3
            query2 into Xa*Xb*Xc

        foreach X in (Xa, Xb, Xc):

            case 5. X is '':

                continue

                special case:

                    when X0 & Xa are '' or when X3 & Xc are ''
                    we already cover it in case 2

            case 6. X not in query1 then return False

            case 7. X in query1 begin at index:

                case 7-1. X is the first X not '' we looked up in query1.(Xa)
                    last_idx = index ;
                    continue

                case 7-2. X is not the first:

                    try to find the smallest Xb > Xa
                    if and Only if we could find a sequence:

                        satisfy Xc > Xb > Xa, otherwise return False
                        '=' will happen when X0 = Xa
                        then we could return True
    """
    if query2.find('*') == -1:
        if query1.find('*') != -1:
            return False
        elif query1 != query2:
            return False
        return True
    else:
        if query1.endswith('*') and not query2.endswith('*'):
            return False
        if query1.startswith('*') and not query2.startswith('*'):
            return False

        # last_idx to save where we find last X
        last_idx = -1
        dict2 = query2.split('*')
        if dict2[0] != '' and not query1.startswith(dict2[0]):
            return False
        if dict2[-1] != '' and not query1.endswith(dict2[-1]):
            return False
        for x_item in dict2:
            if x_item == '':
                continue
            # cur_idx to save where we find cur X
            cur_idx = query1.find(x_item)
            if cur_idx == -1:
                return False
            else :
                if last_idx == -1:# first X not ''
                    last_idx = cur_idx
                    continue
                else:
                    while cur_idx <= last_idx:
                        mov = query1[cur_idx+1:].find(x_item)
                        if mov == -1:
                            break
                        else:
                            cur_idx += mov +1
                    if cur_idx > last_idx:
                        last_idx = cur_idx
                        continue
                    else:# mov != -1 or cur_idx <= last_idx
                        return False
        # if reach this then:
        return True


def compare_specs(input_query, exist_query):
    """
    Function to compare set of fields and specs of two input mongo
    queries. Return True if results of exist_query are superset 
    of resulst for input_query.
    """
    # check that instance remain the same
    inst1 = input_query.get('instance', None)
    inst2 = exist_query.get('instance', None)
    if  inst1 != inst2:
        return False

    # we use notation query2 is superset of query1
    query1  = dict(input_query)
    query2  = dict(exist_query)

    # delete aggregators during comparision
    for query in [query1, query2]:
        if  query.has_key('aggregators'):
            del query['aggregators']
        if  query.has_key('filters'):
            del query['filters']
        if  query.has_key('instance'):
            del query['instance']

    if  query1 == query2:
        return True

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

    # check spec keys, since they applied to data-srv APIs do not
    # allow their comparison for different set of keys.
    if  spec2.keys() != spec1.keys():
        return False

    for key, val1 in spec1.items():
        if  spec2.has_key(key):
            val2 = spec2[key]
            if  (isinstance(val1, str) or isinstance(val1, unicode)) and \
                (isinstance(val2, str) or isinstance(val2, unicode)):
                if  not compare_str(val1, val2):
                    return False
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
                if  val1 != val2:
                    return False
    return True

class DASMongocache(object):
    """
    DAS cache based MongoDB. 
    """
    def __init__(self, config):
        self.emptyset_expire = expire_timestamp(\
            config['das'].get('emptyset_expire', 5))
        self.dburi   = config['mongodb']['dburi']
        self.cache_size = config['mongodb']['bulkupdate_size']
        self.dbname  = config['dasdb']['dbname']
        self.verbose = config['verbose']
        self.logger  = PrintManager('DASMongocache', self.verbose)
        self.mapping = config['dasmapping']

        self.conn    = db_connection(self.dburi)
        self.mdb     = self.conn[self.dbname]
        self.col     = self.mdb[config['dasdb']['cachecollection']]
        self.mrcol   = self.mdb[config['dasdb']['mrcollection']]
        self.merge   = self.mdb[config['dasdb']['mergecollection']]
        self.gfs     = db_gridfs(self.dburi)

        self.das_internal_keys = ['das_id', 'das', 'cache_id']

        msg = "%s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)

        self.add_manipulator()

        # ensure that we have the following indexes
        index_list = [('das.expire', ASCENDING), ('das_id', ASCENDING),
                      ('query.spec.key', ASCENDING), ('das.ts', ASCENDING),
                      ('das.qhash', DESCENDING),
                      ('das.empty_record', ASCENDING),
                      ('query', DESCENDING), ('query.spec', DESCENDING)]
        create_indexes(self.col, index_list)
        index_list = [('das.expire', ASCENDING), ('das_id', ASCENDING),
                      ('das.empty_record', ASCENDING), ('das.ts', ASCENDING)]
        create_indexes(self.merge, index_list)
        
    def add_manipulator(self):
        """
        Add DAS-specific MongoDB SON manipulator to perform
        conversion of inserted data into DAS cache.
        """
        das_son_manipulator = DAS_SONManipulator()
        self.mdb.add_son_manipulator(das_son_manipulator)
        msg = "DAS_SONManipulator %s" \
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
        msg = "query=%s" % query
        spec    = query.get('spec', {})
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
        
        msg = "%s=%s" % (key, value)
        self.logger.debug(msg)
        cond = {'query.spec.key': key}
        for row in self.col.find(cond):
            mongo_query = decode_mongo_query(row['query'])
            for thiskey, thisvalue in mongo_query.items():
                if thiskey == key:
                    if fnmatch.fnmatch(value, thisvalue):
                        yield thisvalue

    def execution_query(self, orig_query):
        """
        Prepare execution query to be run for records retrieval
        """
        # do deep copy of original query to avoid dictionary
        # overwrites tricks
        query = deepcopy(orig_query)

        # setup properly _id type
        query  = adjust_id(query)

        # convert query spec to use patterns
        query, dquery = convert2pattern(query)
        msg = 'adjusted query %s' % dquery
        self.logger.debug(msg)

        # add das_ids look-up to remove duplicates
        das_ids = self.get_das_ids(query)
        if  das_ids:
            query['spec'].update({'das_id':{'$in':das_ids}})
        fields = query.get('fields', None)
        if  fields == ['records']:
            fields = None # look-up all records
            query['fields'] = None # reset query field part
        spec = query.get('spec', {})
        if  spec == dict(records='*'):
            spec  = {} # we got request to get everything
            query['spec'] = spec
        else: # add look-up of condition keys
            ckeys = [k for k in spec.keys() if k != 'das_id']
            if  len(ckeys) == 1:
                query['spec'].update({'das.condition_keys': ckeys})
            elif len(ckeys):
                query['spec'].update({'das.condition_keys': {'$all':ckeys}})
        # add proper date condition
        if  query.has_key('spec') and query['spec'].has_key('date'):
            if isinstance(query['spec']['date'], dict)\
                and query['spec']['date'].has_key('$lte') \
                and query['spec']['date'].has_key('$gte'):
                query['spec']['date'] = [query['spec']['date']['$gte'],
                                         query['spec']['date']['$lte']]
        if  fields:
            prim_keys = []
            for key in fields:
                for srv in self.mapping.list_systems():
                    prim_key = self.mapping.find_mapkey(srv, key)
                    if  prim_key and prim_key not in prim_keys:
                        prim_keys.append(prim_key)
            if  prim_keys:
                query['spec'].update({"das.primary_key": {"$in":prim_keys}})
        aggregators = query.get('aggregators', None)
        mapreduce   = query.get('mapreduce', None)
        filters     = query.get('filters', None)
        if  filters:
            fields  = query['fields']
            if  not fields or not isinstance(fields, list):
                fields = []
            new_fields = []
            for dasfilter in filters:
                if  dasfilter == 'unique':
                    continue
                if  dasfilter not in fields and \
                    dasfilter not in new_fields:
                    if  dasfilter.find('=') == -1 and dasfilter.find('<') == -1\
                    and dasfilter.find('>') == -1:
                        new_fields.append(dasfilter)
            if  new_fields:
                query['fields'] = new_fields
            else:
                if  fields:
                    query['fields'] = fields
                else:
                    query['fields'] = None
            # adjust query if we got a filter
            if  query.has_key('filters'):
                filter_dict = parse_filters(query)
                if  filter_dict:
                    query['spec'].update(filter_dict)

        if  query.has_key('system'):
            spec.update({'das.system' : query['system']})
        msg = 'execution query %s' % query
        self.logger.info(msg)
        return query

    def remove_expired(self, collection):
        """
        Remove expired records from DAS cache.
        """
        timestamp = int(time.time())
        col  = self.mdb[collection]
        spec = {'das.expire' : {'$lt' : timestamp}}
        if  self.verbose:
            nrec = col.find(spec).count()
            msg  = "will remove %s records" % nrec
            msg += ", localtime=%s" % timestamp
            self.logger.debug(msg)
        col.remove(spec)

    def find(self, query):
        """
        Find provided query in DAS cache.
        """
        enc_query = encode_mongo_query(query)
        cond = {'query': enc_query, 'das.system':'das'}
        return self.col.find_one(cond)

    def find_specs(self, query, system='das'):
        """
        Check if cache has query whose specs are identical to provided query.
        Return all matches.
        """
        enc_query = encode_mongo_query(query)
        inst = query.get('instance', None)
        if  system:
            cond = {'query.spec': enc_query['spec'],
                    'query.fields': enc_query['fields'],
                    'das.system':'das'}
        else:
            cond = {'query.spec': enc_query['spec'],
                    'query.fields': enc_query['fields']}
        if  inst:
            cond.update({'query.instance':inst})
        return self.col.find(cond)

    def get_das_ids(self, query):
        """
        Return list of DAS ids associated with given query
        """
        das_ids = []
        try:
            das_ids = \
                [r['_id'] for r in self.col.find_specs(query, system='')]
        except:
            pass
        return das_ids

    def update_das_expire(self, query, timestamp):
        """Update timestamp of all DAS data records for given query"""
        newval  = {'$set': {'das.expire':timestamp}}
        specs   = self.find_specs(query, system='')
        das_ids = [r['_id'] for r in specs]
        # update DAS records
        spec = {'_id' : {'$in': [ObjectId(i) for i in das_ids]}}
        self.col.update(spec, newval, multi=True)
        self.merge.update(spec, newval, multi=True)
        # update data records
        spec = {'das_id' : {'$in': das_ids}}
        self.col.update(spec, newval, multi=True)
        self.merge.update(spec, newval, multi=True)

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

    def add_to_record(self, query, info, system=None):
        """
        Add to existing DAS record provided info
        """
        enc_query = encode_mongo_query(query)
        if  system:
            self.col.update({'query': enc_query, 'das.system':system}, 
                            {'$set': info}, upsert=True)
        else:
            self.col.update({'query': enc_query}, 
                            {'$set': info}, upsert=True)

    def update_query_record(self, query, status, header=None):
        """
        Update DAS record for provided query.
        """
        enc_query = encode_mongo_query(query)
        if  header:
            system = header['das']['system']
            spec1  = {'query': enc_query, 'das.system': 'das'}
            dasrecord = self.col.find_one(spec1)
            spec2  = {'query': enc_query, 'das.system': system}
            sysrecord = self.col.find_one(spec2)
            if  dasrecord and sysrecord:
                if  header['das']['expire'] < dasrecord['das']['expire']:
                    expire = header['das']['expire']
                else:
                    expire = dasrecord['das']['expire']
                api = header['das']['api']
                url = header['das']['url']
                if  set(api) & set(sysrecord['das']['api']) == set(api) and \
                    set(url) & set(sysrecord['das']['url']) == set(url):
                    self.col.update({'_id':ObjectId(sysrecord['_id'])},
                         {'$set': {'das.expire':expire, 'das.status':status}})
                else:
                    self.col.update({'_id':ObjectId(sysrecord['_id'])},
                        {'$pushAll':{'das.api':header['das']['api'],
                                     'das.urn':header['das']['api'],
                                     'das.url':header['das']['url'],
                                     'das.ctime':header['das']['ctime'],
                                     'das.lookup_keys':header['lookup_keys'],
                                    },
                         '$set': {'das.expire':expire, 'das.status':status}})
                self.col.update({'_id':ObjectId(dasrecord['_id'])},
                     {'$set': {'das.expire':expire, 'das.status':status}})
        else:
            self.col.update({'query': enc_query, 'das.system':'das'},
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
        if  spec.has_key('date') and isinstance(spec['date'], dict) \
            and spec['date'].has_key('$lte') \
            and spec['date'].has_key('$gte'):
            spec['date'] = [spec['date']['$gte'], spec['date']['$lte']]
        if  fields:
            prim_keys = []
            for field in fields:
                if  not spec.has_key(field):
                    prim_key = re.compile("^%s" % field)
                    prim_keys.append(prim_key)
            if  prim_keys:
                spec.update({'das.primary_key': {'$in': prim_keys}})
        system = query.get('system', None)
        if  system:
            spec.update({'das.system': system})
        res    = col.find(spec=spec, fields=fields).count()
        msg    = "(%s, coll=%s) found %s results" % (query, collection, res)
        self.logger.info(msg)
        if  res:
            return True
        return False

    def nresults(self, query, collection='merge'):
        """Return number of results for given query."""
        if  query.has_key('aggregators'):
            return len(query['aggregators'])
        # Distinguish 2 use cases, unique filter and general query
        # in first one we should count only unique records, in later
        # we can rely on DB count() method. Pleas keep in mind that
        # usage of fields in find doesn't account for counting, since it
        # is a view over records found with spec, so we don't need to use it.
        col  = self.mdb[collection]
        spec = query['spec']
        if  check_unique_filter(query):
            skeys = self.find_sort_keys(collection, query)
            if  skeys:
                gen = col.find(spec=spec).sort(skeys)
            else:
                gen = col.find(spec=spec)
            res = len([r for r in unique_filter(gen)])
        else:
            res = col.find(spec=spec).count()
        msg = "%s" % res
        self.logger.info(msg)
        return res

    def find_sort_keys(self, collection, query, skey=None):
        """
        Find list of sort keys for a given DAS query. Check existing
        indexes and either use fields or spec keys to find them out.
        """
        # try to get sort keys all the time to get ordered list of
        # docs which allow unique_filter to apply afterwards
        fields = query.get('fields')
        spec   = query.get('spec')
        skeys  = []
        if  skey:
            if  order == 'asc':
                skeys = [(skey, ASCENDING)]
            else:
                skeys = [(skey, DESCENDING)]
        else:
            existing_idx = [i for i in self.existing_indexes(collection)]
            if  fields:
                lkeys = []
                for key in fields:
                    for mkey in self.mapping.mapkeys(key):
                        if  mkey not in lkeys:
                            lkeys.append(mkey)
            else:
                lkeys = spec.keys()
            keys = [k for k in lkeys \
                if k.find('das') == -1 and k.find('_id') == -1 and \
                        k in existing_idx]
            skeys = [(k, ASCENDING) for k in keys]
        return skeys

    def existing_indexes(self, collection='merge'):
        """
        Get list of existing indexes in DB. They are returned by index_information
        API in the following for:

        .. doctest::

            {u'_id_': {u'key': [(u'_id', 1)], u'v': 0},
             u'das.expire_1': {u'key': [(u'das.expire', 1)], u'v': 0},
             ...
             u'tier.name_-1': {u'key': [(u'tier.name', -1)], u'v': 0}}
        """
        col = self.mdb[collection]
        for val in col.index_information().values():
            for idx in val['key']:
                yield idx[0] # index name

    def get_records(self, col, spec, fields, skeys, idx, limit, unique=False):
        """
        Get DAS records out of MongoDB and correctly apply
        unique filter. If it is present in request, we skip
        idx/limit options and apply then at application level.
        """
        # ensure that all fields keys will be presented in DAS record
        if  fields:
            for key in fields:
                if  key not in self.das_internal_keys and \
                    not spec.has_key(key):
                    spec.update({key: {'$exists':True}})
        res = []
        try:
            res = col.find(spec=spec, fields=fields)
            if  skeys:
                res = res.sort(skeys)
            if  not unique:
                if  idx:
                    res = res.skip(idx)
                if  limit:
                    res = res.limit(limit)
        except Exception as exp:
            print_exc(exp)
            row = {'exception': str(exp)}
            yield row
        if  unique:
            if  limit:
                gen = itertools.islice(unique_filter(res), idx, idx+limit)
            else:
                gen = unique_filter(res)
            for row in gen:
                yield row
        else:
            for row in res:
                yield row

    def get_from_cache(self, query, idx=0, limit=0, skey=None, order='asc', 
                        collection='merge'):
        """
        Retrieve results from cache, otherwise return null.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        col = self.mdb[collection]
        msg = "(%s, %s, %s, %s, %s, coll=%s)"\
                % (query, idx, limit, skey, order, collection)
        self.logger.info(msg)

        # keep original MongoDB query without DAS additions
        orig_query = deepcopy(query)
        for key in orig_query['spec'].keys():
            if  key.find('das') != -1:
                del orig_query['spec'][key]

        idx    = int(idx)
        spec   = query.get('spec', {})
        fields = query.get('fields', None)
        # always look-up non-empty records
        if  spec:
            spec.update({'das.empty_record':0})

        # look-up raw record
        if  fields: # be sure to extract those fields
            fields = list(fields) + self.das_internal_keys
        # try to get sort keys all the time to get ordered list of
        # docs which allow unique_filter to apply afterwards
        skeys  = self.find_sort_keys(collection, query, skey)
        res = self.get_records(col, spec, fields, skeys, \
                        idx, limit, check_unique_filter(query))
        counter = 0
        for row in res:
            counter += 1
            yield row

        if  counter:
            msg = "yield %s record(s)" % counter
            self.logger.info(msg)

        # if no raw records were yield we look-up possible error records
        if  not counter:
            err = 0
            for row in self.col.find({'query':encode_mongo_query(orig_query)}):
                spec = {'das_id': str(row['_id'])}
                for row in self.col.find(spec, fields):
                    err += 1
                    msg = '\nfor query %s\nfound %s' % (orig_query, row)
                    prf = 'DAS WARNING, monogocache:get_from_cache '
                    print dastimestamp(prf), msg
#                    yield row

            if  err:
                msg = "found %s error record(s)" % counter
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
        self.logger.debug("(%s, %s)" % (mapreduce, spec))
        record = self.mrcol.find_one({'name':mapreduce})
        if  not record:
            raise Exception("Map/reduce function '%s' not found" % mapreduce)
        fmap = record['map']
        freduce = record['reduce']
        if  spec:
            result = coll.map_reduce(Code(fmap), Code(freduce), query=spec)
        else:
            result = coll.map_reduce(Code(fmap), Code(freduce))
        msg = "found %s records in %s" % (result.count(), result.name)
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
        self.logger.debug("(%s)" % query)
        lookup_keys = []
        id_list = []
        expire  = 9999999999 # future
        skey    = None # sort key
        # get all API records for given DAS query
        records = self.col.find({'query':encode_mongo_query(query)})
        for row in records:
            if  not row.has_key('das'):
                continue
            if  not row['das'].has_key('lookup_keys'):
                continue
            try:
                rec   = \
                [k for i in row['das']['lookup_keys'] for k in i.values()]
            except Exception as exc:
                print_exc(exc)
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
                msg  = "merging %s records, for %s key" % (nrec, pkey) 
            else:
                msg  = "merging records, for %s key" % pkey
            self.logger.debug(msg)
            records = self.col.find(spec).sort(skey)
            # aggregate all records
            agen = aggregator(records, expire)
            # diff aggregated records
            gen = das_diff(agen, self.mapping.diff_keys(pkey.split('.')[0]))
            # create index on all lookup keys
            create_indexes(self.merge, skey)
            # insert all records into das.merge using bulk insert
            size = self.cache_size
            try:
                while True:
                    if  self.merge.insert(itertools.islice(gen, size)):
                        inserted = 1
                    else:
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
        if  not inserted: # we didn't merge anything, it DB look-up failure
            empty_expire = 20 # secs, short enough to expire
            empty_record = {'das':{'expire':empty_expire,
                                   'primary_key':lookup_keys,
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
        create_indexes(self.col, index_list)
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
        spec = {'spec' : {'query':enc_query, 'das.system':dasheader['system']}}
        if  not self.incache(spec, collection='cache'):
            msg = "query=%s, header=%s" % (query, header)
            self.logger.debug(msg)
            q_record = dict(das=dasheader, query=enc_query)
            q_record['das']['lookup_keys'] = lkeys
            q_record['das']['empty_record'] = 0
            q_record['das']['status'] = "requested"
            q_record['das']['qhash'] = genkey(enc_query)
            self.col.insert(q_record)

    def update_records(self, query, results, header):
        """
        Iterate over provided results, update records and yield them
        to next level (update_cache)
        """
        self.logger.debug("(%s) store to cache" % query)
        if  not results:
            return
        dasheader  = header['das']
        expire     = dasheader['expire']
        system     = dasheader['system']
        rec        = [k for i in header['lookup_keys'] for k in i.values()]
        cond_keys  = query['spec'].keys()
        daststamp  = time.time()
        # get API record id
        enc_query  = encode_mongo_query(query)
        spec       = {'query':enc_query, 'das.system':system}
        record     = self.col.find_one(spec, fields=['_id'])
        objid      = record['_id']
        # insert DAS records
        prim_key   = rec[0][0]#use rec instead of lkeys[0] which re-order items
        counter    = 0
        if  isinstance(results, list) or isinstance(results, GeneratorType):
            for item in results:
                counter += 1
                item['das'] = dict(expire=expire, primary_key=prim_key, 
                                        condition_keys=cond_keys, ts=daststamp,
                                        system=system, empty_record=0)
                item['das_id'] = str(objid)
                yield item
        else:
            print "\n\n ### results = ", str(results)
            raise Exception('Provided results is not a list/generator type')
        self.logger.info("\n")
        msg = "%s yield %s rows" % (dasheader['system'], counter)
        self.logger.info(msg)

        # update das record with new status
        status = 'Update DAS cache, %s API' % header['das']['api'][0]
        self.update_query_record(query, status, header)

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

