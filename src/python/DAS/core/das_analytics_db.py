#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS analytics DB module.
"""

from __future__ import with_statement

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
        self.logger  = config['logger']
        self.verbose = config['verbose']
        self.attempt = config['analyticsdb']['attempt']
        self.dbhost  = config['analyticsdb']['dbhost']
        self.dbport  = config['analyticsdb']['dbport']
        self.dbname  = config['analyticsdb']['dbname']
        self.colname = config['analyticsdb']['collname']

        msg = "DASAnalytics::__init__ %s:%s@%s" \
        % (self.dbhost, self.dbport, self.dbname)
        self.logger.info(msg)
        self.create_db()

    def create_db(self):
        """
        Create analytics DB in MongoDB back-end.
        """
        self.conn = db_connection(self.dbhost, self.dbport)
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

    def add_query(self, dasquery, mongoquery):
        """
        Add DAS-QL/MongoDB-QL queries into analytics.
        """
        if  isinstance(mongoquery, dict):
            mongoquery = encode_mongo_query(mongoquery)
        msg = 'DASAnalytics::add_query("%s", %s)' % (dasquery, mongoquery)
        self.logger.info(msg)
        dhash   = genkey(dasquery)
        qhash   = genkey(mongoquery)

        record  = dict(dasquery=dasquery, mongoquery=mongoquery, 
                        qhash=qhash, dhash=dhash, time=time.time())
        #if  self.col.find({'qhash':qhash}).count():
        #    return #we want multiple records for a given qhash?
                    #otherwise we have no way of recording frequent
                    #similar queries.
                    #alternatively, a lightweight record of inserted at
                    #each invocation?
        self.col.insert(record)
        index = [('qhash', DESCENDING), 
                 ('dhash', DESCENDING),
                 ('time', DESCENDING)]
        self.col.ensure_index(index)

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
        self.logger.info(msg, identifier, start, finish, payload)
        
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
            cond['finish'] = {'$gt': after}
        if before:
            cond['start'] = {'$lt': before}
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
        self.logger.info(msg)
        # find query record
        qhash   = genkey(query)
        record  = self.col.find_one({'qhash':qhash}, fields=['dasquery'])
        if  not record:
            self.add_query("", orig_query)
        # find api record
        record  = self.col.find_one({'qhash':qhash, 'system':system, 
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
        """
        msg    = 'DBSAnalytics::insert_apicall, query=%s, url=%s,'\
                % (query, url)
        msg   += 'api=%s, args=%s, expire=%s' % (api, api_params, expire)
        self.logger.info(msg)
        expire = expire_timestamp(expire)
        query = encode_mongo_query(query)
        self.remove_expired()
        doc  = dict(system=system, url=url, api=api, api_params=api_params,
                        qhash=genkey(query), expire=expire)
        self.col.insert(dict(apicall=doc))
        index_list = [('apicall.url', DESCENDING), 
                      ('apicall.api', DESCENDING),
                      ('qhash', DESCENDING),
                     ]
        self.col.ensure_index(index_list)
        
    def update_apicall(self, query, das_dict):
        """
        Update apicall record with provided DAS dict.
        Moved from AbstractService
        """
        msg    = 'DBSAnalytics::update_apicall, query=%s, das_dict=%s'\
                % (query, das_dict)
        self.logger.info(msg)
        spec   = {'apicall.qhash':genkey(encode_mongo_query(query))} 
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
        self.logger.info(msg)
        qhash = genkey(query)
        cond  = {'qhash':qhash, 'system':system}
        self.col.update(cond, {'$inc' : {'counter':1}})

    def list_systems(self):
        """
        List all DAS systems.
        """
        cond = { 'system' : { '$ne' : None } }
        gen  = (row['system'] for row in self.col.find(cond, ['system']))
        return gen2list(gen)

    def list_queries(self, qhash=None, dhash=None, query_regex=None,
                     key=None, after=None, before=None,):
        """
        List inserted queries based on many criteria.
        """
        cond = {}
        if qhash:
            cond['qhash'] = qhash
        if dhash:
            cond['dhash'] = dhash
        if query_regex:
            cond['dasquery'] = {'$regex':query_regex}
        if key:
            cond['mongoquery.spec'] = {'$elemMatch' : {'key': key}}
        if before and after:
            cond['time'] = {'$gt': after, '$lt': before}
        elif after:
            cond['time'] = {'$gt': after}
        elif before:
            cond['time'] = {'$lt': before}
        
        return self.col.find(cond)
            
        

    def list_apis(self, system=None):
        """
        List all APIs.
        """
        cond = { 'api.name' : { '$ne' : None } }
        if  system:
            cond['system'] = system
        gen  = (row['api']['name'] for row in \
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
        gen  = (row['api']['params'] for row in \
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
