#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS analytics DB module.
"""

__revision__ = "$Id: das_analytics_db.py,v 1.19 2010/04/14 16:56:28 valya Exp $"
__version__ = "$Revision: 1.19 $"
__author__ = "Valentin Kuznetsov"

# system modules
import time

# monogo db modules
from pymongo import DESCENDING
from pymongo.objectid import ObjectId

# DAS modules
from DAS.utils.utils import gen2list, genkey, expire_timestamp
from DAS.core.das_mongocache import encode_mongo_query
from DAS.utils.das_db import db_connection

class DASAnalytics(object):
    """
    DAS analytics DB manager.
    """
    def __init__(self, config):
        self.logger = config['logger']
        self.verbose = config['verbose']
        self.dburi   = config['mongodb']['dburi']
        self.dbname  = config['analyticsdb']['dbname']        
        self.colname = config['analyticsdb']['collname']
        self.history = config['analyticsdb']['history']

        msg = "DASAnalytics::__init__ %s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)
        self.create_db()

    def create_db(self):
        """
        Create analytics DB in MongoDB back-end.
        """
        self.conn = db_connection(self.dburi)
        database  = self.conn[self.dbname]
        self.col  = database[self.colname]
#        if  self.dbname not in self.conn.database_names():
#            capped_size = 104857600
#            options   = {'capped':True, 'size': capped_size}
#            database  = self.conn[self.dbname]
#            database.create_collection('self.colname', **options)
#            print "####CREATE CAPPED ANALYTICS"
#        self.col  = self.conn[self.dbname][self.colname] 

    def delete_db(self):
        """
        Delete analytics DB in MongoDB back-end.
        """
        self.conn.drop_database(self.dbname)

    def delete_db_collection(self):
        """
        Delete analytics DB collection in MongoDB.
        """
        self.conn.drop_collection(self.colname)

    def add_query(self, dasquery, mongoquery):
        """
        Add DAS-QL/MongoDB-QL queries into analytics.
        
        A unique record is contained for each (qhash, dhash) pair.
        For each an array of call-times is contained.
        """
        if  isinstance(mongoquery, dict):
            mongoquery = encode_mongo_query(mongoquery)
        msg = 'DASAnalytics::add_query("%s", %s)' % (dasquery, mongoquery)
        self.logger.debug(msg)
        dhash = genkey(dasquery)
        qhash = genkey(mongoquery)

        now = time.time()

        existing = self.col.find_one({'qhash': qhash, 'dhash': dhash})
        if existing:
            #self.col.update({'_id': existing['_id']},
            #                {'$push': {'times': now},
            #                 '$pull': {'times': {'$lt': now - self.history}}})
            # the above is not possible - mongo disallows push and pull
            # on same field in one update operation (as of 1.7.1)
            self.col.update({'_id': existing['_id']},
                            {'$push': {'times': now}})
        else:
            record = dict(dasquery=dasquery, mongoquery=mongoquery,
                        qhash=qhash, dhash=dhash, times=[now])
            self.col.insert(record)

        index = [('qhash', DESCENDING),
                 ('dhash', DESCENDING)]
        self.col.ensure_index(index)
        
    def clean_queries(self):
        """
        Standalone method to clean up expired call-times from query records,
        since otherwise only the active record is cleaned.
        
        This is too expensive to do with every operation, and mongodb
        does not allow multiple modifications to a single field in a single
        update operation (ie, we can't do $push and $pull in one update),
        so it should probably be done asynchronously at fixed intervals.
        """
        
        msg = 'DASAnalytics::clean_queries()'
        self.logger.debug(msg)
        
        now = time.time()
        
        #clean out the times array
        self.col.update({'times': {'$exists': True}},
                        {'$pull': {'times': {'$lt': now - self.history}}})
        #now delete any with no times
        self.col.remove({'times': {'$size': 0}})
        #and should maybe delete anything with the same qhash here?

    def remove_expired(self):
        "Moved from AbstractService -  remove old apicall records"
        spec = {'apicall.expire':{'$lt' : int(time.time())}}
        self.col.remove(spec)

    def add_summary(self, identifier, start, finish, **payload):
        """
        Add an analyzer summary, with given analyzer identifier,
        start and finish times and payload.
        
        It is intended that a summary document is deposited on
        each run of an analyzer (if desirable) and is thereafter
        immutable.
        """
        msg = 'DASAnalytics::add_summary(%s, %s->%s, %s)'
        self.logger.debug(msg, identifier, start, finish, payload)
        
        record = {'analyzer':identifier,
                  'start': start,
                  'finish': finish}
        payload.update(record) #ensure key fields are set correctly
        self.col.insert(payload)
        # ensure summary items are indexed for quick extract
        self.col.ensure_index([('analyzer', DESCENDING)])
        

    def get_summary(self, identifier, after=None, before=None, **query):
        """
        Retrieve a summary document for a given analyzer-identifier,
        optionally specifying a time range.
        """
        cond = {'analyzer': identifier}
        if after:
            cond['start'] = {'$gte': after}
        if before:
            cond['finish'] = {'$lte': before}
        if query:
            cond.update(query)
        return list(self.col.find(cond))

    def add_api(self, system, query, api, args):
        """
        Add API info to analytics DB. 
        Here args is a dict of API parameters.
        """
        orig_query = query
        if  isinstance(query, dict):
            query = encode_mongo_query(query)
        msg = 'DASAnalytics::add_api(%s, %s, %s, %s)' \
        % (system, query, api, args)
        self.logger.debug(msg)
        # find query record
        qhash = genkey(query)
        record = self.col.find_one({'qhash':qhash}, fields=['dasquery'])
        if  not record:
            self.add_query("", orig_query)
        # find api record
        record = self.col.find_one({'qhash':qhash, 'system':system,
                        'api.name':api, 'api.params':args}) 
        apidict = dict(name=api, params=args)
        if  record:
            self.col.update({'_id':record['_id']}, {'$inc':{'counter':1}})
        else:
            record = dict(system=system, api=apidict, qhash=qhash, counter=1)
            self.col.insert(record)
        index = [('system', DESCENDING), ('dasquery', DESCENDING),
                 ('api.name', DESCENDING), ('qhash', DESCENDING) ]
        self.col.ensure_index(index)
        
    def insert_apicall(self, system, query, url, api, api_params, expire):
        """
        Remove obsolete apicall records and
        insert into Analytics DB provided information about API call.
        Moved from AbstractService.
        
        Updated so that we do not have multiple records when performing
        forced updates (ie, the old record is not yet expired) - now
        look for an existing record with the same parameters (I'm hoping
        the fact that some of the variables are indexed will make this
        fast even though not all are), and if it exists just update
        the expiry. Otherwise insert a new record.
        """
        msg = 'DASAnalytics::insert_apicall, query=%s, url=%s,'\
                % (query, url)
        msg += 'api=%s, args=%s, expire=%s' % (api, api_params, expire)
        self.logger.debug(msg)
        expire = expire_timestamp(expire)
        query = encode_mongo_query(query)
        qhash = genkey(query)
        self.remove_expired()
        existing = self.col.find_one({'apicall.system':     system,
                                      'apicall.url':        url,
                                      'apicall.api':        api,
                                      'apicall.api_params': api_params,
                                      'apicall.qhash':      qhash})
        if existing:
            self.logger.debug("DASAnalytics::insert_apicall updating")
            self.col.update({'_id': existing['_id']},
                            {'$set':{'apicall.expire': expire}})
        else:
            self.col.insert({'apicall':{'api_params':   api_params,
                                        'url':          url,
                                        'api':          api,
                                        'system':       system,
                                        'expire':       expire,
                                        'qhash':        qhash}})
        index_list = [('apicall.url', DESCENDING),
                      ('apicall.api', DESCENDING),
                      ('qhash', DESCENDING)]
        self.col.ensure_index(index_list)
        
    def update_apicall(self, query, das_dict):
        """
        Update apicall record with provided DAS dict.
        Moved from AbstractService
        """
        msg = 'DBSAnalytics::update_apicall, query=%s, das_dict=%s'\
                % (query, das_dict)
        self.logger.debug(msg)
        spec = {'apicall.qhash':genkey(encode_mongo_query(query))} 
        record = self.col.find_one(spec)
        self.col.update({'_id':ObjectId(record['_id'])},
            {'$set':{'dasapi':das_dict,
                     'apicall.expire':das_dict['response_expires']}})

    def update(self, system, query):
        """
        Update records for given system/query.
        """
        if  isinstance(query, dict):
            query = encode_mongo_query(query)
        msg = 'DASAnalytics::update(%s, %s)' % (system, query)
        self.logger.debug(msg)
        qhash = genkey(query)
        cond = {'qhash':qhash, 'system':system}
        self.col.update(cond, {'$inc' : {'counter':1}})

    def list_systems(self):
        """
        List all DAS systems.
        """
        cond = { 'system' : { '$ne' : None } }
        gen = (row['system'] for row in self.col.find(cond, ['system']))
        return gen2list(gen)

    def list_queries(self, qhash=None, dhash=None, query_regex=None,
                     key=None, after=None, before=None):
        """
        List inserted queries based on many criteria.
        """
        cond = {'mongoquery': {'$exists': True}}
        if qhash:
            cond['qhash'] = qhash
        if dhash:
            cond['dhash'] = dhash
        if query_regex:
            cond['dasquery'] = {'$regex':query_regex}
        if key:
            cond['mongoquery.spec.key'] = key
        # in this case we need a specific element to be within the range,
        # so we need to use elemMatch
        if before and after:
            cond['times'] = {'$elemMatch': {'$gt': after, '$lt': before}}
        # in these cases we only need to match any element
        elif after:
            cond['times'] = {'$gt': after}
        elif before:
            cond['times'] = {'$lt': before}
        
        return self.col.find(cond)
            
    def list_apis(self, system=None):
        """
        List all APIs.
        """
        cond = { 'api.name' : { '$ne' : None } }
        if  system:
            cond['system'] = system
        gen = (row['api']['name'] for row in \
                self.col.find(cond, ['api.name']))
        return gen2list(gen)
    
    def list_apicalls(self, qhash=None, api=None, url=None):
        "Replace ad-hoc calls in AbstractService"
        cond = {}
        if qhash:
            cond['apicall.qhash'] = qhash
        if api:
            cond['apicall.api'] = api
        if url:
            cond['apicall.url'] = url
        
        return list(self.col.find(cond))

    def api_params(self, api):
        """
        Retrieve API parameters from analytics DB
        """
        cond = {'api.name':api}
        gen = (row['api']['params'] for row in \
                self.col.find(cond, ['api.params']))
        return gen2list(gen)

    def api_counter(self, api, args=None):
        """
        Retrieve API counter from analytics DB. User must supply
        API name and optional dict of parameters.
        """
        cond = {'api.name': api}
        if  args:
            for key, val in args.items():
                cond[key] = val
        return self.col.find_one(cond, ['counter'])['counter']
