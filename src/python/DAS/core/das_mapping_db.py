#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mapping DB
"""

__revision__ = "$Id: das_mapping_db.py,v 1.19 2009/12/02 21:01:08 valya Exp $"
__version__ = "$Revision: 1.19 $"
__author__ = "Valentin Kuznetsov"

import os
import re
import time
import traceback

# monogo db modules
from pymongo.connection import Connection
from pymongo import DESCENDING

# DAS modules
from DAS.utils.utils import getarg, gen2list

class DASMapping(object):
    """
    This class manages DAS mapping DB.
    """
    def __init__(self, config):
        self.logger  = config['logger']
        self.verbose = config['verbose']
        self.dbhost  = config['mapping_dbhost']
        self.dbport  = config['mapping_dbport']
        self.dbname  = getarg(config, 'mapping_dbname', 'mapping')
        self.colname = 'db'

        msg = "DASMapping::__init__ %s:%s@%s" \
        % (self.dbhost, self.dbport, self.dbname)
        self.logger.info(msg)
        
        self.create_db()

        self.notationcache = {}
        self.init_notationcache()

    def init_notationcache(self):
        """
        Initialize notation cache by reading notations.
        """
        for system, notations in self.notations().items():
            for row in notations:
                key = system, row['api_param']
                self.notationcache[key] = row['api'], row['das_name']

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
        Delete mapping DB in MongoDB back-end.
        """
        self.conn.drop_database(self.dbname)

    def add(self, record):
        """
        Add new record into mapping DB. 
        We insert the following API records into mapping DB

        {system:dbs, 
         api:{name:listBlocks, 
              params:[{name:apiversion:1_2_2, value:*, required:0}, ...]
             },
         daskeys:[{key:block, map:block.name, pattern:pat}, ...],
         api2das:[{api_param:site, das_key:site, 
                   pattern:re.compile('^T[0-3]_')}, ...]

        We insert the following notation record into mapping DB:

        {system:dbs, 
         notations:[{api_param:storage_element_name, das_name:se}, ...]
        }
        """
        msg = 'DASMapping::add(%s)' % record
        self.logger.info(msg)
        self.col.insert(record)
        index = None
        if  record.has_key('api'):
            index = [('system', DESCENDING), ('daskeys', DESCENDING),
                     ('api.name', DESCENDING) ]
        elif record.has_key('notations'):
            index = [('system', DESCENDING), 
                     ('notations.api_param', DESCENDING)]
        elif record.has_key('presentation'):
            index = []
        else:
            msg = 'Invalid record %s, no api/notations keys' % record
            raise Exception(msg)
        if  index:
            self.col.ensure_index(index)

    def list_systems(self):
        """
        List all DAS systems.
        """
        cond = { 'system' : { '$ne' : None } }
        gen  = (row['system'] for row in self.col.find(cond, ['system']))
        return gen2list(gen)

    def daskeys(self):
        """
        Return a dict with all known DAS keys.
        """
        cond  = { 'system' : { '$ne' : None } }
        gen   = (row['system'] for row in self.col.find(cond, ['system']))
        kdict = {}
        for system in gen:
            query = {'system':system, 'api':{'$ne':None}}
            keys  = []
            for row in self.col.find(query):
                for entry in row['daskeys']:
                    if  entry['key'] not in keys:
                        keys.append(entry['key'])
            kdict[system] = keys
        return kdict

    def check_daskey(self, system, daskey):
        """Check if provided system/daskey is valid combination"""
        entity = daskey.split('.')[0]
        cond   = { 'system' : system, 'daskeys.key' : entity }
        gen    = (row['daskeys'] \
                for row in self.col.find(cond, ['daskeys']))
        for maps in gen:
            for imap in maps:
                if  daskey == imap['key'] or daskey == imap['map']:
                    return True
        return False

    def relational_keys(self, system1, system2):
        """
        Return a list of relational keys between provided systems
        """
        for system, keys in self.daskeys().items():
            if  system == system1:
                keys1 = keys
            if  system == system2:
                keys2 = keys
        return list( set(keys1) & set(keys2) )

    def find_system(self, key):
        """
        Return system name for provided DAS key.
        """
        cond = { 'daskeys.key' : key }
        gen  = (row['system'] for row in self.col.find(cond, ['system']))
        systems = []
        for system in gen:
            if  system not in systems:
                systems.append(system)
        systems.sort()
        return systems

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

    def lookup_keys(self, system, daskey, api=None, value=None):
        """
        Returns lookup keys for given system and provided
        selection DAS key, e.g. block => block.name
        """
        query = {'system':system, 'daskeys.key':daskey}
        if  api:
            query['api.name'] = api
        lookupkeys = []
        for row in self.col.find(query):
            for kdict in row['daskeys']:
                if  kdict['key'] == daskey: 
                    lkey = kdict['map']
                else:
                    continue
                if  value and kdict['pattern']:
                    pat = eval(kdict['pattern'])
                    if  pat.match(value): 
                        if  lkey not in lookupkeys:
                            lookupkeys.append(lkey)
                else:
                    if  lkey not in lookupkeys:
                        lookupkeys.append(lkey)
        if  not lookupkeys:
            msg = 'Unable to find look-up key for '
            msg += 'system=%s, daskey=%s, api=%s, value=%s' \
                % (system, daskey, api, value)
            raise Exception(msg)
        return lookupkeys

    def api2das(self, system, api_input_name):
        """
        Translates data-service API input parameter into DAS QL key,
        e.g. run_number => run.
        """
        query = {'system':system, 'api2das.api_param' : api_input_name}
        names = []
        for adas in self.col.find(query, ['api2das']):
            for row in adas['api2das']:
                aparam = row['api_param']
                daskey = row['das_key']
                if  aparam == api_input_name and daskey not in names:
                    names.append(daskey)
        return names

    def das2api(self, system, daskey, value=None):
        """
        Translates DAS QL key into data-service API input parameter
        """
        query = {'system':system, 'api2das.das_key': daskey}
        names = []
        for adas in self.col.find(query, ['api2das']):
            for row in adas['api2das']:
                api_param = row['api_param']
                if  row['das_key'] != daskey:
                    continue
                if  value and row['pattern']:
                    pat = eval(row['pattern'])
                    if  pat.match(value):
                        if  api_param not in names:
                            names.append(api_param)
                else:
                    if  api_param not in names:
                        names.append(api_param)
        return names

    def sort_keys(self, daskey):
        """
        Translates DAS QL key into data-service API input parameter
        """
        query = {'daskeys.key': daskey}
        keys  = []
        for row in self.col.find(query, ['daskeys.map']):
            if  row['daskeys']['map'] not in keys:
                keys.append(row['daskeys']['map'])
        return keys

    def notations(self, system=None):
        """
        Return DAS notation map.
        """
        notationmap = {}
#        spec = {'notations':{'$ne':None}}
        spec = {'notations':{'$exists':True}}
        if  system:
            spec['system'] = system
        for item in self.col.find(spec):
            notationmap[item['system']] = item['notations']
        return notationmap

    def notation2das(self, system, api_param, api=""):
        """
        Translates data-service API parameter name into DAS name, e.g.
        run_number=run. In case when api_param is not presented in DB
        just return it back.
        """
        if  not self.notationcache:
            self.init_notationcache()
        if  self.notationcache.has_key((system, api_param)):
            _api, das_name = self.notationcache[(system, api_param)]
            if  _api:
                if  _api == api:
                    return das_name
            else: # valid for all API names
                return das_name
        return api_param

    def api2daskey(self, system, api):
        """
        Returns list of DAS keys which cover provided data-service API
        """
        query = {'system':system, 'api.name':api}
        keys = []
        for row in self.col.find(query):
            for entry in row['daskeys']:
                keys.append(entry['key'])
        return keys

    def servicemap(self, system, implementation=None):
        """
        Constructs data-service map, e.g.
        {api: {keys:[list of DAS keys], params: dict_of_api_params} }
        """
        query = {'system':system, 'api':{'$ne':None}}
        smap = {}
        for row in self.col.find(query):
            api = row['api']['name']
            keys = []
            for entry in row['daskeys']:
                keys.append(entry['key'])
            params = dict(row['api']['params'])
            if  implementation=='javaservlet':
                params['api'] = api
            smap[api] = dict(keys=keys, params=params)
        return smap

    def presentation(self, daskey):
        """
        Return web UI presentation keys for provided DAS keyword.
        For example once asked for block we present block.name, block.size, etc.
        """
        query = {'presentation':{'$ne':None}}
        for row in self.col.find(query):
            data = row['presentation']
            if  data.has_key(daskey):
                return data[daskey]
        return daskey
