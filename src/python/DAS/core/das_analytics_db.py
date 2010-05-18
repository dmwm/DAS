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
import types

# monogo db modules
#from pymongo.connection import Connection
from pymongo import DESCENDING

# DAS modules
from DAS.utils.utils import gen2list, genkey
from DAS.core.das_mongocache import encode_mongo_query, make_connection

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
        self.dbname  = config['analyticsdb'].\
                        get('analytics_dbname', 'analytics')
        self.colname = config['analyticsdb'].\
                        get('analytics_collname', 'db')

        msg = "DASAnalytics::__init__ %s:%s@%s" \
        % (self.dbhost, self.dbport, self.dbname)
        self.logger.info(msg)
        self.create_db()

    def create_db(self):
        """
        Create analytics DB in MongoDB back-end.
        """
#        self.conn    = Connection(self.dbhost, self.dbport)
        self.conn = make_connection(self.dbhost, self.dbport, self.attempt)
        database  = self.conn[self.dbname]
        self.col  = database[self.colname]

    def delete_db(self):
        """
        Delete analytics DB in MongoDB back-end.
        """
        self.conn.drop_database(self.dbname)

    def add_query(self, dasquery, mongoquery):
        """
        Add DAS-QL/MongoDB-QL queries into analytics.
        """
        if  type(mongoquery) is types.DictType:
            mongoquery = encode_mongo_query(mongoquery)
        msg = 'DASAnalytics::add_query("%s", %s)' % (dasquery, mongoquery)
        self.logger.info(msg)
        dhash   = genkey(dasquery)
        qhash   = genkey(mongoquery)
        record  = dict(dasquery=dasquery, mongoquery=mongoquery, 
                        qhash=qhash, dhash=dhash)
        if  self.col.find({'qhash':qhash}).count():
            return
        self.col.insert(record)
        index = [('qhash', DESCENDING), ('dhash', DESCENDING)]
        self.col.ensure_index(index)

    def add_api(self, system, query, api, args):
        """
        Add API info to analytics DB. 
        Here args is a dict of API parameters.
        """
        orig_query = query
        if  type(query) is types.DictType:
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

    def update(self, system, query):
        """
        Update records for given system/query.
        """
        if  type(query) is types.DictType:
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

    def list_queries(self, system=None, api=None):
        """
        List all queries.
        """
        cond = { 'query' : { '$ne' : None } }
        if  system:
            cond['system'] = system
        if  api:
            cond['api.name'] = api
        gen  = (row['query'] for row in self.col.find(cond, ['query']))
        return gen2list(gen)

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
