#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS analytics DB
"""

from __future__ import with_statement

__revision__ = "$Id: das_analytics_db.py,v 1.5 2009/09/11 18:42:14 valya Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "Valentin Kuznetsov"

import os
import traceback

# monogo db modules
from pymongo.connection import Connection
from pymongo import DESCENDING

# DAS modules
from DAS.utils.utils import getarg, gen2list

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

        msg = "Init DAS analytics %s:%s@%s" \
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

    def add_api(self, system, query, api, args):
        """
        Add API info to analytics DB. 
        Here args is a dict of API parameters.
        """
        msg = 'DASAnalytics::add_api(%s, %s, %s, %s)' \
        % (system, query, api, args)
        self.logger.info(msg)
        apidict = dict(name=api, params=args)
        record = dict(query=query, system=system, api=apidict, counter=1)
        if  self.col.find({'query':query, 'system':system, 'api.name':api}).count():
            return self.update(query)
        self.col.insert(record)
        index = [('system', DESCENDING), ('query', DESCENDING),
                 ('api.name', DESCENDING) ]
        self.col.ensure_index(index)

    def update(self, system, query):
        """
        Update record with given query.
        """
        msg = 'DASAnalytics::update(%s, %s)' % (system, query)
        self.logger.info(msg)
        cond = {'query':query, 'system':system}
        for row in self.col.find(cond):
            id = row['_id']
            row['counter'] += 1
            self.col.update({'_id':id}, row)

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
