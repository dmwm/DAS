#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703,R0913,R0912,R0914,R0915,R0902,R0903,R0904

"""
DAS mongocache manager.
The DAS consists of several sub-systems:

    - DAS cache contains data records (output from data-services)
      and API records
    - DAS merge contains merged data records
    - DAS mapreduce collection
"""

__author__ = "Valentin Kuznetsov"

# system modules
import re
import time
from   types import GeneratorType
import datetime
import itertools
import fnmatch

# DAS modules
from DAS.core.das_ql import das_record_keys
from DAS.core.das_son_manipulator import DAS_SONManipulator
from DAS.utils.query_utils import decode_mongo_query
from DAS.utils.ddict import convert_dot_notation
from DAS.utils.utils import aggregator, unique_filter
from DAS.utils.utils import adjust_mongo_keyvalue, print_exc, das_diff
from DAS.utils.utils import parse_filters, expire_timestamp
from DAS.utils.utils import dastimestamp, record_codes
from DAS.utils.das_db import db_connection, find_one
from DAS.utils.das_db import db_gridfs, parse2gridfs, create_indexes
from DAS.utils.logger import PrintManager
from DAS.utils.thread import start_new_thread
from DAS.utils.das_pymongo import PYMONGO_OPTS, PYMONGO_NOEXHAUST

# monogo db modules
from bson.objectid import ObjectId
from bson.code import Code
from pymongo import DESCENDING, ASCENDING
from bson.errors import InvalidDocument
from pymongo.errors import ConnectionFailure, InvalidOperation, DuplicateKeyError

def adjust_expire(expire):
    "Adjust expire timestamp"
    if  expire < time.time(): # it means we got plain expire tstamp
        expire = time.time()+expire
    return expire

def spec4data_records():
    "Return spec part for data_records"
    data_record = record_codes('data_record')
    empty_record = record_codes('empty_record')
    gridfs_record = record_codes('gridfs_record')
    spec = {'$in': [data_record, empty_record, gridfs_record]}
    return spec

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
    if  '_id' in spec:
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

def etstamp(delta=20):
    """
    Return expire timestamp which can be used for empty records
    It should be short enough to allow quick expire of the records.
    The delta input parameter can be used to control thta.
    """
    return time.time() + delta

def cleanup_worker(dburi, dbname, collections, del_ttl, sleep):
    """DAS cache cleanup worker"""
    while True:
        conn = db_connection(dburi)
        spec = {'das.expire': { '$lt':time.time()-del_ttl}}
        for col in collections:
            conn[dbname][col].remove(spec)
        time.sleep(sleep)

class DASMongocache(object):
    """
    DAS cache based MongoDB.
    """
    def __init__(self, config):
        self.config  = config
        self.emptyset_expire = \
                expire_timestamp(config['das'].get('emptyset_expire', 5))
        self.dburi   = config['mongodb']['dburi']
        self.cache_size = config['mongodb']['bulkupdate_size']
        self.dbname  = config['dasdb']['dbname']
        self.verbose = config['verbose']
        self.logger  = PrintManager('DASMongocache', self.verbose)
        self.mapping = config['dasmapping']
        self.logging = config['dasdb'].get('logging', False)
        self.rec_ttl = config['dasdb'].get('record_ttl', 24*60*60)
        self.del_ttl = config['dasdb'].get('delta_ttl', 60)
        self.cleanup_del_ttl = config['dasdb'].get('cleanup_delta_ttl', 3600)
        self.retry   = config['dasdb'].get('retry', 3)
        self.das_son_manipulator = DAS_SONManipulator()

        # Initialize MongoDB connection
        self.col_    = self.config['dasdb']['cachecollection']
        self.mrcol_  = self.config['dasdb']['mrcollection']
        self.merge_  = self.config['dasdb']['mergecollection']
        self.gfs     = db_gridfs(self.dburi)

        msg = "%s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)

        # ensure that we have the following indexes
        common_idx = [
                      ('file.name', DESCENDING),
                      ('dataset.name', DESCENDING),
                      ('block.name', DESCENDING),
                      ('run.run_number', DESCENDING),
                      ]
        index_list = [('das.expire', ASCENDING), ('das_id', ASCENDING),
                      ('das.system', ASCENDING),
                      ('qhash', DESCENDING),
                      ('das.record', ASCENDING)]
        create_indexes(self.col, index_list + common_idx)
        index_list = [('das.expire', ASCENDING), ('das_id', ASCENDING),
                      ('qhash', DESCENDING),
                      ('das.record', ASCENDING),
                      ('das.ts', ASCENDING)]
        create_indexes(self.merge, index_list)
        # NOTE: I found that creating index in merge collection leads to
        # MongoDB error when records contains multiple arrays on indexed
        # keys. For example, when we query file,run,lumi both file and run
        # are arrays in MongoDB. In this case the final sort in MongoDB
        # bark with the following message:
        # cannot sort with keys that are parallel arrays
        # it looks like that there is no fix for that yet
        # see
        # http://stackoverflow.com/questions/6516725/how-do-i-index-two-arrays-in-mongodb
        # therefore I temporary disabled create_indexes call on merge
        # collection which was used to have index to ease final sort,
        # especially in a case when a lot of records correspond to inital
        # query, e.g. file records.
        # On another hand, the most common use case where sort fails is
        # getting file records, and I can add one compound key to ease sort
        # but I can't add another compound key on array field, e.g. run
        common_idx = [[('qhash', DESCENDING), ('file.name', DESCENDING)]]
        create_indexes(self.merge, index_list + common_idx)

        # thread which clean-up DAS collections
        thname = 'mongocache_cleanup'
        cols   = [config['dasdb']['cachecollection'],
                  config['dasdb']['mrcollection'],
                  config['dasdb']['mergecollection']]
        sleep  = config['dasdb'].get('cleanup_interval', 600)
        if  config['dasdb'].get('cleanup_worker', True):
            args = (self.dburi, self.dbname, cols, self.cleanup_del_ttl, sleep)
            start_new_thread(thname, cleanup_worker, args, unique=True)

    @property
    def col(self):
        "col property provides access to DAS cache collection"
        conn = db_connection(self.dburi)
        mdb  = conn[self.dbname]
        colnames = mdb.collection_names()
        if  not colnames or self.col_ not in colnames:
            print "Create", mdb, self.col_
            mdb.create_collection(self.col_)
        mdb.add_son_manipulator(self.das_son_manipulator)
        return mdb[self.col_]

    @property
    def merge(self):
        "merge property provides access to DAS merge collection"
        conn = db_connection(self.dburi)
        mdb  = conn[self.dbname]
        colnames = mdb.collection_names()
        if  not colnames or self.merge_ not in colnames:
            print "Create", mdb, self.merge_
            mdb.create_collection(self.merge_)
        mdb.add_son_manipulator(self.das_son_manipulator)
        return mdb[self.merge_]

    @property
    def mrcol(self):
        "mrcol property provides access to DAS map-reduce collection"
        conn = db_connection(self.dburi)
        mdb  = conn[self.dbname]
        mdb.add_son_manipulator(self.das_son_manipulator)
        return mdb[self.mrcol_]

    def get_dataset_hashes(self, dasquery):
        "Get dataset hashes from DBS database"
        spec = dasquery.mongo_query.get('spec', {})
        inst = dasquery.instance
        conn = db_connection(self.dburi)
        if  spec and inst:
            dataset = spec.get('dataset.name', None)
            if  dataset:
                if  dataset.find('*') != -1:
                    cond = {'dataset':re.compile(dataset.replace('*', '.*'))}
                else:
                    cond = {'dataset': dataset}
                for row in conn['dbs'][inst].find(cond):
                    if  'qhash' in row:
                        yield row['qhash']

    def check_datasets(self, dasquery):
        "Check dataset presence in DAS cache for given das query"
        hashes = [r for r in self.get_dataset_hashes(dasquery)]
        if  hashes:
            spec = {'qhash': {'$in': hashes}}
            if  len(hashes) == self.merge.find(spec, **PYMONGO_OPTS).count():
                dasquery._hashes = hashes

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
        for row in self.col.find(cond, **PYMONGO_OPTS):
            mongo_query = decode_mongo_query(row['query'])
            for thiskey, thisvalue in mongo_query.iteritems():
                if thiskey == key:
                    if fnmatch.fnmatch(value, thisvalue):
                        yield thisvalue

    def get_fields(self, dasquery):
        "Prepare fields to extract from MongoDB"
        fields     = dasquery.mongo_query.get('fields', [])
        if  fields and 'records' in fields:
            fields = None # look-up all records
        filters    = dasquery.filters
        cond       = {}
        if  filters:
            new_fields = []
            for dasfilter in filters:
                if  dasfilter == 'unique':
                    continue
                if  fields and dasfilter not in fields and \
                    dasfilter not in new_fields:
                    if  dasfilter.find('=') == -1 and dasfilter.find('<') == -1\
                    and dasfilter.find('>') == -1:
                        new_fields.append(dasfilter)
                    else:
                        cond = parse_filters(dasquery.mongo_query)
            if  not new_fields and fields:
                new_fields = list(fields)
            return new_fields, cond
        return fields, cond

    def remove_expired(self, dasquery, collection):
        """
        Remove expired records from DAS cache. We need to perform this
        operation very carefullly since we don't use transation and on-going
        commits can invoke this method (see das_core.py).  Therefore we use
        MongoDB $or operator to wipe out queries which match DASQuery hash and
        already expired or queries which lived in cache more then rec_ttl
        config parameter. The later operation just prevent DAS cache from
        growing.
        """
        conn   = db_connection(self.dburi)
        mdb    = conn[self.dbname]
        mdb.add_son_manipulator(self.das_son_manipulator)
        col    = mdb[collection]
        # use additional delta to check data record expiration
        # we add this delta to ensure that there is no records close to
        # current timestamp which may expire during request processing
        spec = {'qhash':dasquery.qhash,
                'das.expire':{'$lt':time.time()+self.del_ttl}}
        col.remove(spec)

    def check_services(self, dasquery):
        """
        Check if DAS cache contains DAS records with service response for
        given query.
        """
        das_rec  = self.find(dasquery)
        if  not das_rec:
            return False
        if  'das' not in das_rec:
            return False
        if  'services' not in das_rec['das']:
            return False
        spec = {'qhash':dasquery.qhash, 'das.system':{'$ne':'das'},
                'das.expire':{'$gt':time.time()}}
        nres = self.col.find(spec, **PYMONGO_OPTS).count()
        if  nres:
            return True
        return False

    def find(self, dasquery):
        """
        Find provided query in DAS cache.
        """
        cond = {'qhash': dasquery.qhash, 'das.system':'das',
                'das.expire': {'$gt':time.time()}}
        return find_one(self.col, cond)

    def find_specs(self, dasquery, system='das'):
        """
        Check if cache has query whose specs are identical to provided query.
        Return all matches.
        """
        if dasquery.hashes:
            cond = {'qhash':{'$in':dasquery.hashes}}
        else:
            cond = {'qhash': dasquery.qhash}
        if  system:
            cond.update({'das.system': system})
        cond.update({'das.expire':{'$gt':time.time()}})
        return self.col.find(cond, **PYMONGO_OPTS)

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
        "Update timestamp of all DAS data records for given query"
        nval = {'$set': {'das.expire':timestamp}}
        spec = {'qhash' : dasquery.qhash}
        self.col.update(spec, nval, multi=True)
        self.merge.update(spec, nval, multi=True)

    def das_record(self, dasquery):
        "Retrieve DAS record for given query"
        cond = {'qhash': dasquery.qhash, 'das.expire':{'$gt':time.time()}}
        return find_one(self.col, cond)

    def find_records(self, das_id):
        " Return all the records matching a given das_id"
        return self.col.find({'das_id': das_id}, **PYMONGO_OPTS)

    def is_error_in_records(self, dasquery, collection='cache'):
        "Scan DAS cache for error records and return true or not"
        if  collection == 'cache':
            results = self.col.find({'qhash':dasquery.qhash}, **PYMONGO_OPTS)
        else:
            results = self.merge.find({'qhash':dasquery.qhash}, **PYMONGO_OPTS)
        error  = None
        reason = None
        for row in results:
            if 'error' in row:
                error  = row.get('error')
                reason = row.get('reason', '')
                break
        return error, reason

    def add_to_record(self, dasquery, info, system=None):
        "Add to existing DAS record provided info"
        if  system:
            self.col.update({'query': dasquery.storage_query,
                             'das.system':system},
                            {'$set': info}, upsert=True)
        else:
            self.col.update({'query': dasquery.storage_query},
                            {'$set': info}, upsert=True)

    def find_min_expire(self, dasquery):
        """Find minimal expire timestamp across all records for given DAS query"""
        spec   = {'qhash': dasquery.qhash}
        min_expire = 2*time.time() # upper bound, will update
        for rec in self.col.find(spec, **PYMONGO_OPTS):
            if  'das' in rec and 'expire' in rec['das']:
                estamp = rec['das']['expire']
                if  min_expire > estamp:
                    min_expire = estamp
        return long(min_expire)

    def update_query_record(self, dasquery, status, header=None, reason=None):
        "Update DAS record for provided query"
        ctime = time.time()
        das_spec = {'qhash': dasquery.qhash, 'das.system':'das'}
        min_expire = self.find_min_expire(dasquery)
        if  header:
            system = header['das']['system']
            sts    = header['das']['status']
            expire = header['das']['expire']
            spec   = {'qhash': dasquery.qhash, 'das.system': system}
            new_expire = None
            for rec in self.col.find(spec, **PYMONGO_OPTS):
                if  'das' in rec and 'expire' in rec['das']:
                    if  rec['das']['expire'] > expire:
                        new_expire = expire
                        ndict = {'das.expire':expire, 'das.status':status}
                        cdict = {'das.ctime':ctime}
                        udict = {'$set':ndict, '$push':cdict}
                        oid   = ObjectId(rec['_id'])
                        self.col.update({'_id':oid}, udict)
            if  new_expire:
                udict = {'$set': {'das.expire': new_expire},
                         '$push': {'das.ctime':ctime}}
                self.col.update(das_spec, udict)
        else:
            udict = {'$set': {'das.status':status, 'das.expire': min_expire},
                     '$push': {'das.ctime':ctime}}
            self.col.update(das_spec, udict)
        if  reason:
            udict = {'$set': {'das.reason':reason}}
            self.col.update(das_spec, udict)
        # align all expire timestamps when we recieve ok status
        if  status == 'ok':
            udict = {'$set': {'das.expire': min_expire}}
            self.col.update(das_spec, udict, fsync=True)

    def apilist(self, dasquery):
        "Return list of apis for given dasquery"
        spec = {'qhash':dasquery.qhash,
                'das.record':record_codes('query_record')}
        apis = []
        for row in self.col.find(spec, ['das.api'], **PYMONGO_OPTS):
            try:
                apis += row['das']['api']
            except Exception as _err:
                pass
        return apis

    def incache(self, dasquery, collection='merge', system=None, api=None,
            query_record=False):
        """
        Check if we have query results in cache, otherwise return null.
        Please note, input parameter query means MongoDB query, please
        consult MongoDB API for more details,
        http://api.mongodb.org/python/
        """
        if  query_record:
            record = record_codes('query_record')
        else:
            record = spec4data_records()
        spec = {'qhash':dasquery.qhash, 'das.record':record,
                'das.expire':{'$gt':time.time()}}
        if  system:
            spec.update({'das.system': system})
        if  api:
            spec.update({'das.api': api})
        conn = db_connection(self.dburi)
        mdb  = conn[self.dbname]
        mdb.add_son_manipulator(self.das_son_manipulator)
        col  = mdb[collection]
        res  = col.find(spec, **PYMONGO_OPTS).count()
        msg  = "(%s, coll=%s) found %s results" % (dasquery, collection, res)
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
        fields, filter_cond = self.get_fields(dasquery)
        if  not fields:
            spec = dasquery.mongo_query.get('spec', {})
        elif dasquery.hashes:
            spec = {'qhash':{'$in':dasquery.hashes},
                    'das.record': spec4data_records()}
        else:
            spec = {'qhash':dasquery.qhash,
                    'das.record': spec4data_records()}
        if  filter_cond:
            spec.update(filter_cond)
        conn = db_connection(self.dburi)
        mdb  = conn[self.dbname]
        mdb.add_son_manipulator(self.das_son_manipulator)
        col  = mdb[collection]
        if  dasquery.unique_filter:
            skeys = self.mongo_sort_keys(collection, dasquery)
            if  skeys:
                gen = col.find(spec=spec, **PYMONGO_OPTS).sort(skeys)
            else:
                gen = col.find(spec=spec, **PYMONGO_OPTS)
            res = len([r for r in unique_filter(gen)])
        else:
            res = col.find(spec=spec, **PYMONGO_OPTS).count()
            if  not res: # double check that this is really the case
                time.sleep(1)
                res = col.find(spec=spec, **PYMONGO_OPTS).count()
        msg = "%s" % res
        self.logger.info(msg)
        return res

    def mongo_sort_keys(self, collection, dasquery):
        """
        Find list of sort keys for a given DAS query. Check existing
        indexes and either use fields or spec keys to find them out.
        Return list of mongo sort keys in a form of (key, order).
        """
        # try to get sort keys all the time to get ordered list of
        # docs which allow unique_filter to apply afterwards
        fields = dasquery.mongo_query.get('fields')
        spec   = dasquery.mongo_query.get('spec')
        skeys  = dasquery.sortkeys
        mongo_skeys = []
        if  skeys:
            for key in skeys:
                if  key.find('-') != -1: # reverse order, e.g. desc
                    mongo_skeys.append((key.replace('-', ''), DESCENDING))
                else:
                    mongo_skeys.append((key, ASCENDING))
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
            mongo_skeys = [(k, ASCENDING) for k in keys]
        return mongo_skeys

    def existing_indexes(self, collection='merge'):
        """
        Get list of existing indexes in DB. They are returned by
        index_information API in the following for:

        .. doctest::

            {u'_id_': {u'key': [(u'_id', 1)], u'v': 0},
             u'das.expire_1': {u'key': [(u'das.expire', 1)], u'v': 0},
             ...
             u'tier.name_-1': {u'key': [(u'tier.name', -1)], u'v': 0}}
        """
        conn = db_connection(self.dburi)
        mdb  = conn[self.dbname]
        mdb.add_son_manipulator(self.das_son_manipulator)
        col = mdb[collection]
        for val in col.index_information().values():
            for idx in val['key']:
                yield idx[0] # index name

    def get_records(self, coll, spec, fields, skeys, idx, limit, unique=False):
        "Generator to get records from MongoDB."
        try:
            conn = db_connection(self.dburi)
            mdb  = conn[self.dbname]
            mdb.add_son_manipulator(self.das_son_manipulator)
            col = mdb[coll]
            nres = col.find(spec, **PYMONGO_OPTS).count()
            if  nres == 1 or nres <= limit:
                limit = 0
            if  limit:
                res = col.find(spec, fields, sort=skeys, skip=idx, limit=limit)
            else:
                res = col.find(spec, fields, sort=skeys, **PYMONGO_OPTS)
            if  unique:
                res = unique_filter(res)
            for row in res:
                yield row
        except Exception as exp:
            print_exc(exp)
            row = {'exception': str(exp)}
            res = []
            yield row

    def get_from_cache(self, dasquery, idx=0, limit=0, collection='merge'):
        "Generator which retrieves results from the cache"
        if  dasquery.service_apis_map(): # valid DAS query
            result = self.get_das_records(dasquery, idx, limit, collection)
            for row in result:
                yield row
        else: # pure MongoDB query
            fields  = dasquery.mongo_query.get('fields', [])
            if  fields == None:
                fields = []
            spec    = dasquery.mongo_query.get('spec', {})
            if  dasquery.filters:
                if  not fields:
                    fields = []
                fields += dasquery.filters
                pkeys   = [k.split('.')[0] for k in fields]
            fields += das_record_keys()
            if  'records' in dasquery.query:
                fields = None # special case for DAS 'records' keyword
            skeys   = self.mongo_sort_keys(collection, dasquery)
            result  = self.get_records(collection, spec, fields, skeys, \
                            idx, limit, dasquery.unique_filter)
            for row in result:
                if  dasquery.filters:
                    if  pkeys and set(pkeys) & set(row.keys()):
                        yield row
                else:
                    yield row

    def get_das_records(self, dasquery, idx=0, limit=0, collection='merge'):
        "Generator which retrieves DAS records from the cache"
        msg = "(%s, %s, %s, coll=%s)" % (dasquery, idx, limit, collection)
        self.logger.info(msg)

        idx = int(idx)
        fields, filter_cond = self.get_fields(dasquery)
        if  fields == None:
            fields = []
        if  not fields:
            spec = dasquery.mongo_query.get('spec', {})
        elif dasquery.hashes:
            spec = {'qhash':{'$in':dasquery.hashes},
                    'das.record': spec4data_records()}
        else:
            spec = {'qhash':dasquery.qhash,
                    'das.record': spec4data_records()}
        if  filter_cond:
            spec.update(filter_cond)
        if  'records' in dasquery.query:
            fields  = None # retrieve all fields for records DAS query
        else:
            # be sure to extract das internal keys
            fields += das_record_keys()
        # try to get sort keys all the time to get ordered list of
        # docs which allow unique_filter to apply afterwards
        skeys   = self.mongo_sort_keys(collection, dasquery)
        res     = self.get_records(collection, spec, fields, skeys, \
                        idx, limit, dasquery.unique_filter)
        counter = 0
        for row in res:
            counter += 1
            yield row
        msg = 'qhash %s, found %s record(s) in %s collection' \
                % (dasquery.qhash, counter, collection)
        print dastimestamp('DAS INFO '), msg

        if  counter:
            msg = "yield %s record(s)" % counter
            self.logger.info(msg)

        # if no raw records were yield we look-up possible error records
        # and reset timestamp for record with system:['das']
        if  not counter:
            spec = {'qhash':dasquery.qhash}
            nrec = self.col.find(spec, **PYMONGO_OPTS).count()
            if  nrec:
                msg = "for query %s, found %s non-result record(s)" \
                        % (dasquery, nrec)
                print dastimestamp('DAS WARNING'), msg
                for rec in self.col.find(spec, **PYMONGO_OPTS):
                    if  'query' in rec:
                        print dastimestamp('DAS das record'), rec
            self.update_das_expire(dasquery, etstamp())

    def map_reduce(self, mr_input, dasquery, collection='merge'):
        """
        Wrapper around _map_reduce to allow sequential map/reduce
        operations, e.g. map/reduce out of map/reduce.

        mr_input is either alias name or list of alias names for
        map/reduce functions.

        Input dasquery which is applied to first
        iteration of map/reduce functions.
        """
        # NOTE: I need to revisit mapreduce.
        spec = dasquery.mongo_query['spec']
        if  not isinstance(mr_input, list):
            mrlist = [mr_input]
        else:
            mrlist = mr_input
        conn = db_connection(self.dburi)
        mdb  = conn[self.dbname]
        mdb.add_son_manipulator(self.das_son_manipulator)
        coll = mdb[collection]
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
        record = find_one(self.mrcol, {'name':mapreduce})
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
        result = self.mrcol.find(spec, **PYMONGO_OPTS)
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
        id_list = []
        expire  = 9999999999 # future
        # get all API records for given DAS query
        spec    = {'qhash':dasquery.qhash,
                   'das.expire':{'$gt':time.time()},
                   'das.record':record_codes('query_record')}
        records = self.col.find(spec, **PYMONGO_OPTS)
        for row in records:
            # find smallest expire timestamp to be used by aggregator
            rexpire = row.get('das', {}).get('expire', expire)
            if  rexpire < expire:
                expire = rexpire
            if  row['_id'] not in id_list:
                id_list.append(row['_id'])
        inserted = 0
        lookup_keys = set()
        fields = dasquery.mongo_query.get('fields')
        if  not fields: # Mongo
            fields = []
        for key in fields:
            for pkey in self.mapping.mapkeys(key):
                lookup_keys.add(pkey)
        for pkey in lookup_keys:
            skey = [(pkey, DESCENDING)]
            # lookup all service records
            spec = {'das_id': {'$in': id_list}, 'das.primary_key': pkey}
            if  self.verbose:
                nrec = self.col.find(spec, **PYMONGO_OPTS).sort(skey).count()
                msg  = "merging %s records, for %s key" % (nrec, pkey)
            else:
                msg  = "merging records, for %s key" % pkey
            self.logger.debug(msg)
            # use exhaust=False since we process all records in aggregator
            # and it can be delay in processing
            records = self.col.find(spec, **PYMONGO_NOEXHAUST).sort(skey)
            # aggregate all records
            agen = aggregator(dasquery, records, expire)
            # diff aggregated records
            gen  = das_diff(agen, self.mapping.diff_keys(pkey.split('.')[0]))
            # insert all records into das.merge using bulk insert
            size = self.cache_size
            try:
                while True:
                    nres = self.merge.insert(itertools.islice(gen, size))
                    if  nres and isinstance(nres, list):
                        inserted += len(nres)
                    else:
                        break
            except InvalidDocument as exp:
                msg = "Caught bson error: " + str(exp)
                self.logger.info(msg)
                records = self.col.find(spec, **PYMONGO_OPTS).sort(skey)
                gen = aggregator(dasquery, records, expire)
                genrows = parse2gridfs(self.gfs, pkey, gen, self.logger)
                das_dict = {'das':{'expire':expire,
                        'das.record': record_codes('gridfs_record'),
                        'primary_key':[k for k in lookup_keys],
                        'system': ['gridfs']}, 'qhash':dasquery.qhash,
                        'cache_id':[], 'das_id': id_list}
                for row in genrows:
                    row.update(das_dict)
                    self.merge.insert(row)
            except InvalidOperation:
                pass
            except DuplicateKeyError as err:
                if  not isinstance(gen, list):
                    raise err
        if  inserted:
            # use explicit if statement, due to inserted condition
            # with outside scope meaning
            pass
        elif  not lookup_keys: # we get query w/o fields
            msg = 'qhash %s, no lookup_keys' % dasquery.qhash
            print dastimestamp('DAS WARNING'), msg
            pass
        else: # we didn't merge anything, it is DB look-up failure
            msg  = 'qhash %s, did not insert into das.merge' % dasquery.qhash
            print dastimestamp('DAS WARNING'), msg
            empty_expire = etstamp()
            lkeys = list(lookup_keys)
            das = dict(expire=empty_expire, primary_key=lkeys[0],
                       condition_keys=lkeys,
                       instance=dasquery.instance,
                       system=['das'], services=dasquery.services,
                       record=record_codes('empty_record'),
                       ts=time.time(), api=[])
            empty_record = {'das':das, 'qhash': dasquery.qhash,
                            'cache_id':[], 'das_id': id_list}
            for key in lkeys:
                empty_record.update({key.split('.')[0]:[]})
            for key, val in dasquery.mongo_query['spec'].iteritems():
                if  key.find('.') == -1:
                    empty_record[key] = []
                else: # it is compound key, e.g. site.name
                    newkey, newval = convert_dot_notation(key, val)
                    empty_record[newkey] = adjust_mongo_keyvalue(newval)
            self.merge.insert(empty_record)
            # update DAS records (both meta and data ones, by using qhash)
            nval = {'$set': {'das.expire':empty_expire}}
            spec = {'qhash':dasquery.qhash}
            self.col.update(spec, nval, multi=True)

    def update_cache(self, dasquery, results, header):
        """
        Insert results into cache. Use bulk insert controller by
        self.cache_size. Upon completion ensure indexies.
        """
        # insert/check query record in DAS cache
        self.insert_query_record(dasquery, header)

        # update results records in DAS cache
        gen  = self.generate_records(dasquery, results, header)
        inserted = 0
        # bulk insert
        try:
            while True:
                nres = self.col.insert(itertools.islice(gen, self.cache_size))
                if  nres and isinstance(nres, list):
                    inserted += len(nres)
                else:
                    break
        except InvalidOperation:
            pass

    def insert_query_record(self, dasquery, header):
        """
        Insert query record into DAS cache.
        """
        # check presence of API record in a cache
        dasheader   = header['das']
        system      = dasheader['system']
        api         = dasheader['api']
        collection  = 'cache'
        check_query = True
        expire = dasheader.get('expire', None)
        if  expire:
            dasheader['expire'] = adjust_expire(expire)
        if  not self.incache(dasquery, collection, system, api, check_query):
            msg = "query=%s, header=%s" % (dasquery, header)
            self.logger.debug(msg)
            q_record = dict(das=dasheader, query=dasquery.storage_query)
            q_record['das']['record'] = record_codes('query_record')
            q_record['das']['status'] = "requested"
            q_record['qhash'] = dasquery.qhash
            q_record['das']['ctime'] = [time.time()]
            res = self.col.insert(q_record)
            if  not res:
                msg = 'unable to insert query record'
                print dastimestamp('DAS ERROR '), dasquery, msg, ', will retry'
                time.sleep(1)
                res = self.col.insert(q_record)
                if  not res:
                    print dastimestamp('DAS ERROR '), dasquery, msg

    def generate_records(self, dasquery, results, header):
        """
        Iterate over provided results, update records and yield them
        to next level (update_cache)
        """
        self.logger.debug("(%s) store to cache" % dasquery)
        if  not results:
            return

        dasheader  = header['das']
        expire     = adjust_expire(dasheader['expire'])
        system     = dasheader['system'] # DAS service names, e.g. combined
        services   = dasheader['services'] # CMS services used to get data
        api        = dasheader['api']
        prim_key   = header.get('prim_key', None)
        if  not prim_key:
            # get primary key from a list of lookup keys which has the
            # following structure [{'api':[keys]}, {...}]
            lup_keys = header['lookup_keys']
            lkeys    = [l for i in lup_keys for k in i.values() for l in k]
            prim_key = lkeys[0] if 'summary' not in lkeys else 'summary'
        cond_keys  = dasquery.mongo_query['spec'].keys()
        # get API record id
        spec       = {'qhash':dasquery.qhash, 'das.system':system,
                      'das.expire': {'$gt':time.time()},
                      'das.record': record_codes('query_record')}
        counter    = 0
        rids = [str(r['_id']) for r in \
                self.col.find(spec, ['_id'], **PYMONGO_OPTS)]
        if  rids:
            if  isinstance(results, list) or isinstance(results, GeneratorType):
                for item in results:
                    counter += 1
                    if  'das' in item:
                        expire = item.get('das').get('expire', expire)
                        dasheader['expire'] = expire
                    item['das'] = dict(expire=expire, primary_key=prim_key,
                                       condition_keys=cond_keys,
                                       instance=dasquery.instance,
                                       system=system, services=services,
                                       record=record_codes('data_record'),
                                       ts=time.time(), api=api)
                    item['das_id'] = rids
                    item['qhash'] = dasquery.qhash
                    yield item
            else:
                print "\n\n ### results = ", str(results)
                raise Exception('Provided results is not a list/generator type')
        if  expire != dasheader['expire']: # update DAS records
            header['das']['expire'] = expire
        # update das record with new status
        status = 'Update DAS cache, %s API' % header['das']['api'][0]
        self.update_query_record(dasquery, status, header)

        msg = "\n%s yield %s rows" % (dasheader['system'], counter)
        self.logger.info(msg)

    def remove_from_cache(self, dasquery):
        """
        Remove query from DAS cache. To do so, we retrieve API record
        and remove all data records from das.cache and das.merge
        """
        records = self.col.find({'qhash':dasquery.qhash}, **PYMONGO_OPTS)
        id_list = []
        for row in records:
            if  row['_id'] not in id_list:
                id_list.append(row['_id'])
        spec = {'das_id':{'$in':id_list}}
        self.merge.remove(spec)
        self.merge.remove({'qhash':dasquery.qhash})
        self.col.remove(spec)
        self.col.remove({'qhash':dasquery.qhash})

    def clean_cache(self, collection=None):
        """
        Clean expired docs in das.cache and das.merge.
        """
        current_time = time.time()
        query = {'das.expire': { '$lt':current_time} }
        if  not collection or collection == 'merge':
            self.merge.remove(query)
        if  not collection or collection == 'cache':
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
