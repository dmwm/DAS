#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS analytics DB
"""

from __future__ import with_statement

__revision__ = "$Id: das_analytics_db.py,v 1.11 2009/10/02 18:58:33 valya Exp $"
__version__ = "$Revision: 1.11 $"
__author__ = "Valentin Kuznetsov"

import os
import types
import traceback
try:
    import json # since python 2.6
except:
    import simplejson as json # prior python 2.6

# monogo db modules
from pymongo.connection import Connection
from pymongo import DESCENDING

# DAS modules
from DAS.utils.utils import getarg, gen2list, genkey

class DASAnalytics(object):
    """
    This class manages DAS analytics DB.
    """
    def __init__(self, config):
        self.logger  = config['logger']
        self.verbose = config['verbose']
        self.dbhost  = config['analytics_dbhost']
        self.dbport  = config['analytics_dbport']
        self.dbname  = getarg(config, 'analytics_dbname', 'analytics')
        self.colname = 'db'

        msg = "DASAnalytics::__init__ %s:%s@%s" \
        % (self.dbhost, self.dbport, self.dbname)
        self.logger.info(msg)
        
        self.create_db()

    def create_db(self):
        """
        Establish connection to MongoDB back-end and create DB if
        necessary.
        """
        self.conn    = Connection(self.dbhost, self.dbport)
        self.db      = self.conn[self.dbname]
        self.col     = self.db[self.colname]

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
            mongoquery = json.dumps(mongoquery)
        msg = 'DASAnalytics::add_query(%s, %s)' % (dasquery, mongoquery)
        self.logger.info(msg)
        qhash   = genkey(mongoquery)
        record  = dict(dasquery=dasquery, mongoquery=mongoquery, qhash=qhash)
        if  self.col.find({'qhash':qhash}).count():
            return
        self.col.insert(record)
        index = [('qhash', DESCENDING)]
        self.col.ensure_index(index)

    def add_api(self, system, query, api, args):
        """
        Add API info to analytics DB. 
        Here args is a dict of API parameters.
        """
        if  type(query) is types.DictType:
            query = json.dumps(query)
        msg = 'DASAnalytics::add_api(%s, %s, %s, %s)' \
        % (system, query, api, args)
        self.logger.info(msg)
        # find query record
        qhash   = genkey(str(query))
        record  = self.col.find_one({'qhash':qhash}, fields=['dasquery'])
        if  not record:
            self.add_query("", query)
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
            query = json.dumps(query)
        msg = 'DASAnalytics::update(%s, %s)' % (system, query)
        self.logger.info(msg)
        qhash = genkey(str(query))
        cond  = {'qhash':qhash, 'system':system}
        # TODO:
        # MongoDB has a bug, http://jira.mongodb.org/browse/SERVER-268 
        # which prevent from bulk update of all records, uncomment the
        # line below once it's fixed
#        self.col.update(cond, {'$inc' : {'counter':1}})
        # meanwhile we'll retrieve all records and update them individually
        for record in self.col.find(cond):
            self.col.update({'_id':record['_id']}, {'$inc' : {'counter':1}})

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

    def api_counter(self, api, args={}):
        """
        Retrieve API counter from analytics DB. User must supply
        API name and optional dict of parameters.
        """
        cond = {'api.name': api}
        if  args:
            for key, val in args.items():
                cond[key] = val
        return self.col.find_one(cond, ['counter'])['counter']
