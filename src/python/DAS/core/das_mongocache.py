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
import datetime
import itertools
import fnmatch

# DAS modules
from DAS.core.das_son_manipulator import DAS_SONManipulator
from DAS.core.das_query import DASQuery
from DAS.utils.query_utils import compare_specs, convert2pattern
from DAS.utils.query_utils import encode_mongo_query, decode_mongo_query
from DAS.utils.ddict import convert_dot_notation
from DAS.utils.utils import genkey, aggregator, unique_filter
from DAS.utils.utils import adjust_mongo_keyvalue, print_exc, das_diff
from DAS.utils.utils import parse_filters, expire_timestamp
from DAS.utils.utils import dastimestamp, deepcopy, gen_counter
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

def update_query_spec(spec, fdict):
    """
    Update spec dict with given dictionary. If fdict
    keys overlap with spec dict, we construct and $and condition.
    Please note we update given query spec!!!
    """
    keys = spec.keys()
    and_list = spec.get('$and', None)
    if  set(keys) & set(fdict.keys()):
        for key, val in fdict.iteritems():
            if  key in keys:
                qvalue = spec[key]
                if  isinstance(qvalue, list):
                    qvalue.add({key:val})
                    cond = {'$and' : qvalue}
                else:
                    cond = {'$and' : [{key:qvalue}, {key:val}]}
                del spec[key]
                spec.update(cond)
            else:
                spec.update({key:val})
    elif and_list:
        for key, val in fdict.iteritems():
            if  key in [k for d in and_list for k in d.keys()]:
                and_list.append({key:val})
            else:
                spec.update({key:val})
    else: # set of keys do not overlap
        spec.update(fdict)

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

def logdb_record(coll, doc):
    "Return logdb record"
    timestamp = time.time()
    date = int(str(datetime.date.fromtimestamp(time.time())).replace('-', ''))
    rec = {'type':coll, 'date':date, 'ts':timestamp}
    rec.update(doc)
    return rec

class DASLogdb(object):
    """DASLogdb"""
    def __init__(self, config):
        super(DASLogdb, self).__init__()
        capped_size = config['loggingdb']['capped_size']
        logdbname   = config['loggingdb']['dbname']
        logdbcoll   = config['loggingdb']['collname']
        dburi       = config['mongodb']['dburi']
        try:
            conn    = db_connection(dburi)
            if  logdbname not in conn.database_names():
                dbname      = conn[logdbname]
                options     = {'capped':True, 'size': capped_size}
                dbname.create_collection('db', **options)
                self.warning('Created %s.%s, size=%s' \
                % (logdbname, logdbcoll, capped_size))
            self.logcol     = conn[logdbname][logdbcoll]
        except Exception as exc:
            print_exc(exc)
            self.logcol     = None

    def insert(self, coll, doc):
        "Insert record to logdb"
        if  self.logcol:
            rec = logdb_record(coll, doc)
            self.logcol.insert(rec)

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

        self.logdb   = DASLogdb(config)

        self.das_internal_keys = ['das_id', 'das', 'cache_id']

        msg = "%s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)

        self.add_manipulator()

        # ensure that we have the following indexes
        index_list = [('das.expire', ASCENDING), ('das_id', ASCENDING),
                      ('das.ts', ASCENDING), ('das.system', ASCENDING),
                      ('qhash', DESCENDING),
                      ('das.empty_record', ASCENDING)]
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

    def similar_queries(self, dasquery):
        """
        Check if we have query results in cache whose conditions are
        superset of provided query. The method only works for single
        key whose value is substring of value in input query.
        For example, if cache contains records about T1 sites, 
        then input query T1_CH_CERN is subset of results stored in cache.
        """
        spec = dasquery.mongo_query.get('spec', {})
        cond = {'query.spec.key': {'$in' : spec.keys()}}
        for row in self.col.find(cond):
            found_query = DASQuery(row['query'])
            if  compare_specs(dasquery.mongo_query, found_query.mongo_query):
                msg = "%s similar to %s" % (dasquery, found_query)
                self.logger.info(msg)
                return found_query
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
            for thiskey, thisvalue in mongo_query.iteritems():
                if thiskey == key:
                    if fnmatch.fnmatch(value, thisvalue):
                        yield thisvalue

    def execution_query(self, dasquery):
        """
        Prepare execution query to be run for records retrieval
        """
        # do deep copy of original query to avoid dictionary
        # overwrites tricks
        query = deepcopy(dasquery.mongo_query)

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
                    update_query_spec(query['spec'], filter_dict)

        if  query.has_key('system'):
            spec.update({'das.system' : query['system']})
        if  dasquery.instance:
            spec.update({'das.instance' : dasquery.instance})
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
        self.logdb.insert(collection, {'delete': self.col.find(spec).count()})
        col.remove(spec)

    def find(self, dasquery):
        """
        Find provided query in DAS cache.
        """
        cond = {'qhash': dasquery.qhash, 'das.system':'das'}
        return self.col.find_one(cond)

    def find_specs(self, dasquery, system='das'):
        """
        Check if cache has query whose specs are identical to provided query.
        Return all matches.
        """
        cond = {'qhash': dasquery.qhash}
        if  system:
            cond.update({'das.system': system})
        return self.col.find(cond)

    def get_das_ids(self, dasquery):
        """
        Return list of DAS ids associated with given query
        """
        das_ids = []
        try:
            das_ids = \
                [r['_id'] for r in self.col.find_specs(dasquery, system='')]
        except:
            pass
        return das_ids

    def update_das_expire(self, dasquery, timestamp):
        """Update timestamp of all DAS data records for given query"""
        newval  = {'$set': {'das.expire':timestamp}}
        specs   = self.find_specs(dasquery, system='')
        das_ids = [r['_id'] for r in specs]
        # update DAS records
        spec = {'_id' : {'$in': [ObjectId(i) for i in das_ids]}}
        self.col.update(spec, newval, multi=True)
        self.merge.update(spec, newval, multi=True)
        # update data records
        spec = {'das_id' : {'$in': das_ids}}
        self.col.update(spec, newval, multi=True)
        self.merge.update(spec, newval, multi=True)

    def das_record(self, dasquery):
        """
        Retrieve DAS record for given query.
        """
        return self.col.find_one({'qhash': dasquery.qhash})
    
    def find_records(self, das_id):
        """
        Return all the records matching a given das_id
        """
        return self.col.find({'das_id': das_id})

    def add_to_record(self, dasquery, info, system=None):
        "Add to existing DAS record provided info"
        if  system:
            self.col.update({'query': dasquery.storage_query,
                             'das.system':system},
                            {'$set': info}, upsert=True)
        else:
            self.col.update({'query': dasquery.storage_query},
                            {'$set': info}, upsert=True)

    def update_query_record(self, dasquery, status, header=None):
        "Update DAS record for provided query"
        if  header:
            system = header['das']['system']
            spec1  = {'qhash': dasquery.qhash, 'das.system': 'das'}
            dasrecord = self.col.find_one(spec1)
            spec2  = {'qhash': dasquery.qhash, 'das.system': system}
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
            self.col.update({'query': dasquery.storage_query,
                             'das.system':'das'},
                            {'$set': {'das.status': status}})

    def incache(self, dasquery, collection='merge'):
        """
        Check if we have query results in cache, otherwise return null.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        query  = deepcopy(dasquery.pattern_query)
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
        msg    = "(%s, coll=%s) found %s results" \
                % (dasquery, collection, res)
        self.logger.info(msg)
        if  res:
            return True
        return False

    def nresults(self, dasquery, collection='merge'):
        """Return number of results for given query."""
        if  dasquery.aggregators:
            return len(dasquery.aggregators)
        # Distinguish 2 use cases, unique filter and general query
        # in first one we should count only unique records, in later
        # we can rely on DB count() method. Pleas keep in mind that
        # usage of fields in find doesn't account for counting, since it
        # is a view over records found with spec, so we don't need to use it.
        col  = self.mdb[collection]
        exec_query = self.execution_query(dasquery)
        spec = exec_query['spec']
        if  dasquery.unique_filter:
            skeys = self.find_sort_keys(collection, dasquery)
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

    def find_sort_keys(self, collection, dasquery, skey=None, order='asc'):
        """
        Find list of sort keys for a given DAS query. Check existing
        indexes and either use fields or spec keys to find them out.
        """
        # try to get sort keys all the time to get ordered list of
        # docs which allow unique_filter to apply afterwards
        fields = dasquery.mongo_query.get('fields')
        spec   = dasquery.mongo_query.get('spec')
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
        "Generator to get records from MongoDB. It correctly applies"
        if  fields:
            for key in fields: # ensure that fields keys will be presented
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

    def get_from_cache(self, dasquery, idx=0, limit=0, skey=None, order='asc',
                        collection='merge'):
        "Generator which retrieves results from the cache"
        if  dasquery.service_apis_map(): # valid DAS query
            result = self.get_das_records\
                        (dasquery, idx, limit, skey, order, collection)
        else: # pure MongoDB query
            coll    = self.mdb[collection]
            fields  = dasquery.mongo_query.get('fields', None)
            spec    = dasquery.mongo_query.get('spec', {})
            if  dasquery.filters:
                if  fields == None:
                    fields = dasquery.filters
                else:
                    fields += dasquery.filters
            result  = self.get_records(coll, spec, fields, skey, \
                            idx, limit, dasquery.unique_filter)
        for row in result:
            yield row

    def get_das_records(self, dasquery, idx=0, limit=0, skey=None, order='asc',
                        collection='merge'):
        "Generator which retrieves DAS records from the cache"
        col = self.mdb[collection]
        msg = "(%s, %s, %s, %s, %s, coll=%s)"\
                % (dasquery, idx, limit, skey, order, collection)
        self.logger.info(msg)

        # keep original MongoDB query without DAS additions
        orig_query = deepcopy(dasquery.mongo_query)
        exec_query = self.execution_query(dasquery)
#        orig_query = deepcopy(exec_query)
        for key in orig_query['spec'].keys():
            if  key.find('das') != -1:
                del orig_query['spec'][key]

        idx    = int(idx)
        spec   = exec_query.get('spec', {})
        fields = exec_query.get('fields', None)
        # always look-up non-empty records
        if  spec:
            spec.update({'das.empty_record':0})

        # look-up raw record
        if  fields: # be sure to extract those fields
            fields = list(fields) + self.das_internal_keys
        # try to get sort keys all the time to get ordered list of
        # docs which allow unique_filter to apply afterwards
        skeys  = self.find_sort_keys(collection, dasquery, skey, order)
        res = self.get_records(col, spec, fields, skeys, \
                        idx, limit, dasquery.unique_filter)
        counter = 0
        for row in res:
            counter += 1
            yield row

        if  counter:
            msg = "yield %s record(s)" % counter
            self.logger.info(msg)

        # if no raw records were yield we look-up possible error records
        if  not counter and not orig_query['spec'].has_key('_id'):
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

    def map_reduce(self, mr_input, dasquery, collection='merge'):
        """
        Wrapper around _map_reduce to allow sequential map/reduce
        operations, e.g. map/reduce out of map/reduce. 

        mr_input is either alias name or list of alias names for
        map/reduce functions.

        Input dasquery which is applied to first
        iteration of map/reduce functions.
        """
        # NOTE: I probably need to convert dasquery to
        # execution query, but since this method is not used so
        # often I need to revisit mapreduce.
        spec = dasquery.mongo_query['spec']
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

    def merge_records(self, dasquery):
        """
        Merge DAS records for provided query. We perform the following
        steps: 
        1. get all queries from das.cache by ordering them by primary key
        2. run aggregtor function to merge neighbors
        3. insert records into das.merge
        """
        self.logger.debug(dasquery)
        lookup_keys = []
        id_list = []
        expire  = 9999999999 # future
        skey    = None # sort key
        # get all API records for given DAS query
        records = self.col.find({'qhash':dasquery.qhash})
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
        cdict = {'counter':0}
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
            gen  = gen_counter(gen, cdict)
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
        if  inserted:
            self.logdb.insert('merge', {'insert': cdict['counter']})
        else: # we didn't merge anything, it is DB look-up failure
            empty_expire = 20 # secs, short enough to expire
            empty_record = {'das':{'expire':empty_expire,
                                   'primary_key':lookup_keys,
                                   'empty_record': 1}, 
                            'cache_id':[], 'das_id': id_list}
            for key, val in dasquery.mongo_query['spec'].iteritems():
                if  key.find('.') == -1:
                    empty_record[key] = []
                else: # it is compound key, e.g. site.name
                    newkey, newval = convert_dot_notation(key, val)
                    empty_record[newkey] = adjust_mongo_keyvalue(newval)
            self.merge.insert(empty_record)
            # update DAS records
            newval  = {'$set': {'das.expire':empty_expire}}
            spec = {'_id' : {'$in': [ObjectId(i) for i in id_list]}}
            self.col.update(spec, newval, multi=True)

    def update_cache(self, dasquery, results, header):
        """
        Insert results into cache. Use bulk insert controller by
        self.cache_size. Upon completion ensure indexies.
        """
        # insert/check query record in DAS cache
        self.insert_query_record(dasquery, header)

        # update results records in DAS cache
        rec   = [k for i in header['lookup_keys'] for k in i.values()]
        lkeys = list(set(k for i in rec for k in i))
        index_list = [(key, DESCENDING) for key in lkeys]
        create_indexes(self.col, index_list)
        gen  = self.update_records(dasquery, results, header)
        cdict = {'counter':0}
        gen  = gen_counter(gen, cdict)
        inserted = 0
        # bulk insert
        try:
            while True:
                if  self.col.insert(itertools.islice(gen, self.cache_size)):
                    inserted = 1
                else:
                    break
        except InvalidOperation:
            pass
        if  inserted:
            self.logdb.insert('cache', {'insert': cdict['counter']})

    def insert_query_record(self, dasquery, header):
        """
        Insert query record into DAS cache.
        """
        dasheader  = header['das']
        lkeys      = header['lookup_keys']
        # check presence of API record in a cache
        spec = {'spec' : {'qhash':dasquery.qhash,
                          'das.system':dasheader['system']}}
        if  not self.incache(DASQuery(spec), collection='cache'):
            msg = "query=%s, header=%s" % (dasquery, header)
            self.logger.debug(msg)
            q_record = dict(das=dasheader, query=dasquery.storage_query)
            q_record['das']['lookup_keys'] = lkeys
            q_record['das']['empty_record'] = 0
            q_record['das']['status'] = "requested"
            q_record['das']['qhash'] = dasquery.qhash
            q_record['qhash'] = dasquery.qhash
            self.col.insert(q_record)

    def update_records(self, dasquery, results, header):
        """
        Iterate over provided results, update records and yield them
        to next level (update_cache)
        """
        self.logger.debug("(%s) store to cache" % dasquery)
        if  not results:
            return
        dasheader  = header['das']
        expire     = dasheader['expire']
        system     = dasheader['system']
        rec        = [k for i in header['lookup_keys'] for k in i.values()]
        cond_keys  = dasquery.mongo_query['spec'].keys()
        daststamp  = time.time()
        # get API record id
        spec       = {'qhash':dasquery.qhash, 'das.system':system}
        record     = self.col.find_one(spec, fields=['_id'])
        counter    = 0
        prim_key   = rec[0][0]#use rec instead of lkeys[0] which re-order items
        if  record:
            objid  = record['_id']
            if  isinstance(results, list) or isinstance(results, GeneratorType):
                for item in results:
                    counter += 1
                    item['das'] = dict(expire=expire, primary_key=prim_key,
                                       condition_keys=cond_keys, ts=daststamp,
                                       instance=dasquery.instance,
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
        self.update_query_record(dasquery, status, header)

    def remove_from_cache(self, dasquery):
        """
        Remove query from DAS cache. To do so, we retrieve API record
        and remove all data records from das.cache and das.merge
        """
        records = self.col.find({'qhash':dasquery.qhash})
        id_list = []
        for row in records:
            if  row['_id'] not in id_list:
                id_list.append(row['_id'])
        spec = {'das_id':{'$in':id_list}}
        self.logdb.insert('merge', {'delete': self.col.find(spec).count()})
        self.merge.remove(spec)
        self.logdb.insert('cache', {'delete': self.col.find(spec).count()})
        self.col.remove(spec)
        self.col.remove({'qhash':dasquery.qhash})

    def clean_cache(self):
        """
        Clean expired docs in das.cache and das.merge. 
        """
        current_time = time.time()
        query = {'das.expire': { '$lt':current_time} }
        self.logdb.insert('merge', {'delete': self.merge.find(query).count()})
        self.merge.remove(query)
        self.logdb.insert('cache', {'delete': self.col.find(query).count()})
        self.col.remove(query)

    def delete_cache(self):
        """
        Delete all results in DAS cache/merge collection, including
        internal indexes.
        """
        self.logdb.insert('cache', {'delete': self.col.count()})
        self.col.remove({})
        try: 
            self.col.drop_indexes()
        except:
            pass
        self.logdb.insert('merge', {'delete': self.merge.count()})
        self.merge.remove({})
        try:
            self.merge.drop_indexes()
        except:
            pass
