#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mapping DB module
"""

__revision__ = "$Id: das_mapping_db.py,v 1.36 2010/04/14 16:56:28 valya Exp $"
__version__ = "$Revision: 1.36 $"
__author__ = "Valentin Kuznetsov"

import os
import re
import time
import types
import traceback

# monogo db modules
#from pymongo.connection import Connection
from pymongo import DESCENDING

# DAS modules
from DAS.utils.utils import gen2list, access
from DAS.core.das_mongocache import make_connection

class DASMapping(object):
    """
    This class manages DAS mapping DB.
    """
    def __init__(self, config):
        self.logger   = config['logger']
        self.verbose  = config['verbose']
        self.services = config['services']
        self.dbhost   = config['mappingdb']['dbhost']
        self.dbport   = config['mappingdb']['dbport']
        self.dbname   = config['mappingdb'].get('dbname', 'mapping')
        self.attempt  = config['mappingdb']['attempt']
        self.colname  = config['mappingdb'].get('collname', 'db')

        msg = "DASMapping::__init__ %s:%s@%s" \
        % (self.dbhost, self.dbport, self.dbname)
        self.logger.info(msg)
        
        self.create_db()

        self.notationcache = {}
        self.init_notationcache()

    # ===============
    # Management APIs
    # ===============
    def init_notationcache(self):
        """
        Initialize notation cache by reading notations.
        """
        for system, notations in self.notations().items():
            for row in notations:
                key = system, row['notation']
                if  self.notationcache.has_key(key):
                    self.notationcache[key] += [ (row['api'], row['map']) ]
                else:
                    self.notationcache[key] = [ (row['api'], row['map']) ]

    def create_db(self):
        """
        Establish connection to MongoDB back-end and create DB.
        """
#        self.conn = Connection(self.dbhost, self.dbport)
        self.conn = make_connection(self.dbhost, self.dbport, self.attempt)
        self.db   = self.conn[self.dbname]
        self.col  = self.db[self.colname]

    def delete_db(self):
        """
        Delete mapping DB in MongoDB back-end.
        """
        self.conn.drop_database(self.dbname)

    def check_maps(self):
        """
        Check if there are records in Mapping DB
        """
        return self.col.find().count()

    def remove(self, spec):
        """
        Remove record in DAS Mapping DB for provided Mongo spec.
        """
        self.col.remove(spec)
        
    def add(self, record):
        """
        Add new record into mapping DB. Example of URI record

        .. doctest::

            {
             system:dbs, 
             urn : listBlocks, 
             url : "http://a.b.com/api"
             params : [
                 {"apiversion":1_2_2, test:"*"}
             ]
             daskeys: [
                 {"key" : "block", "map":"block.name", "pattern":""}
             ]
             das2api: [
                 {"das_key":"site", "api_param":"se", 
                       "pattern":"re.compile('^T[0-3]_')"}
             ]
            }

        Example of notation record:

        .. doctest::

             notations: [
                 {"notation" : "storage_element_name", "map":"site", "api": ""},
             ]
        """
        msg = 'DASMapping::add(%s)' % record
        self.logger.info(msg)
        self.col.insert(record)
        index = None
        if  record.has_key('urn'):
            index = [('system', DESCENDING), ('daskeys', DESCENDING),
                     ('urn', DESCENDING) ]
        elif record.has_key('notations'):
            index = [('system', DESCENDING), 
                     ('notations.api_param', DESCENDING)]
        elif record.has_key('presentation'):
            index = []
        else:
            msg = 'Invalid record %s' % record
            raise Exception(msg)
        if  index:
            self.col.ensure_index(index)

    # ==================
    # Informational APIs
    # ==================
    def list_systems(self):
        """
        List all DAS systems.
        """
        cond = { 'system' : { '$ne' : None } }
        gen  = (row['system'] for row in self.col.find(cond, ['system']))
        return list( set(gen2list(gen)) & set(self.services) )
#        return gen2list(gen)

    def list_apis(self, system=None):
        """
        List all APIs.
        """
        cond = { 'urn' : { '$ne' : None } }
        if  system:
            cond['system'] = system
        gen  = (row['urn'] for row in self.col.find(cond, ['urn']))
        return gen2list(gen)

    def api_info(self, api_name):
        """
        Return full API info record.
        """
        return self.col.find_one({'urn':api_name})

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

    def daskeys(self, das_system=None):
        """
        Return a dict with all known DAS keys.
        """
        cond  = { 'system' : { '$ne' : None } }
        if  das_system:
            cond  = { 'system' : das_system }
        gen   = (row['system'] for row in self.col.find(cond, ['system']))
        kdict = {}
        for system in gen:
            query = {'system':system, 'urn':{'$ne':None}}
            keys  = []
            for row in self.col.find(query):
                for entry in row['daskeys']:
                    if  entry['key'] not in keys:
                        keys.append(entry['key'])
            kdict[system] = keys
        return kdict

    # ============
    # Look-up APIs
    # ============
    def primary_key(self, das_system, urn):
        """
        Return DAS primary key for provided system and urn
        """
        cond = {'system':das_system, 'urn':urn}
        daskeys = self.col.find(cond, ['daskeys.key'])
        for row in daskeys:
            if  row and row.has_key('daskeys'):
                for dkey in row['daskeys']:
                    if  dkey.has_key('key'):
                        return dkey['key']
        
    def primary_mapkey(self, das_system, urn):
        """
        Return DAS primary map key for provided system and urn
        """
        cond = {'system':das_system, 'urn':urn}
        mapkeys = self.col.find(cond, ['daskeys.map'])
        for row in mapkeys:
            if  row and row.has_key('daskeys'):
                for mkey in row['daskeys']:
                    if  mkey.has_key('map'):
                        return mkey['map']
        
    def find_daskey(self, das_system, map_key, value=None):
        """
        Find das key for given system and map key.
        """
        msg   = 'DASMapping::find_daskeys, '
        cond  = { 'system' : das_system, 'daskeys.map': map_key }
        daskeys = []
        for row in self.col.find(cond, ['daskeys']):
            if  row and row.has_key('daskeys'):
                for dkey in row['daskeys']:
                    if  dkey.has_key('key'):
                        if  value:
                            pval = dkey.get('pattern', '')
                            if  pval:
                                pat = re.compile(pval)
                                if  pat.match(str(value)):
                                    daskeys.append(dkey['key'])
                                else:
                                    msg += 'mismatch: %s, key=%s, value=%s'\
                                            % (das_system, map_key, value)
                                    msg += ', pattern=%s' % pval
                                    self.logger.info(msg)
                            else:
                                daskeys.append(dkey['key'])
                        else:
                            daskeys.append(dkey['key'])
        return daskeys

    def find_mapkey(self, das_system, das_key, value=None):
        """
        Find map key for given system and das key.
        """
        msg   = 'DASMapping::find_mapkey, '
        cond  = { 'system' : das_system, 'daskeys.key': das_key }
        mapkeys = []
        for row in self.col.find(cond, ['daskeys', 'urn']):
            if  row and row.has_key('daskeys'):
                urn = row['urn']
                for key in row['daskeys']:
                    if  key.has_key('map') and key['key'] == das_key:
                        if  value:
                            pval = key.get('pattern', '')
                            pat = re.compile(pval)
                            if  not pat.match(str(value)):
                                msg += 'mismatch: %s, key=%s, value=%s'\
                                        % (das_system, das_key, value)
                                msg += ', pattern=%s' % key['pattern']
                                self.logger.info(msg)
                                continue
                        return key['map']
#                        mapkeys.append((urn, key['map']))
#        return mapkeys

    def find_apis(self, das_system, map_key):
        """
        Find list of apis which correspond to provided
        system and das map key.
        """
        cond  = { 'system' : das_system, 'daskeys.map': map_key }
        apilist = []
        for row in self.col.find(cond, ['urn']):
            if  row.has_key('urn') and row['urn'] not in apilist:
                apilist.append(row['urn'])
        return apilist

    def check_dasmap(self, system, urn, das_map, value=None):
        """
        Check if provided system/urn/das_map is a valid combination
        in mapping db. If value for das_map key is provided we verify
        it against pattern in DB.
        """
        if  not value:
            cond   = { 'system' : system, 'daskeys.map' : das_map, 'urn': urn }
            return self.col.find(cond).count()
        cond = { 'system' : system, 'daskeys.map' : das_map, 'urn': urn }
        for row in self.col.find(cond, ['daskeys.pattern']):
            for item in row['daskeys']:
                pat = re.compile(item['pattern'])
                if  pat.match(str(value)):
                    return True
        return False

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

    def lookup_keys(self, system, daskey, api=None, value=None):
        """
        Returns lookup keys for given system and provided
        selection DAS key, e.g. block => block.name
        """
        query = {'system':system, 'daskeys.key':daskey}
        if  api:
            query['urn'] = api
        lookupkeys = []
        for row in self.col.find(query):
            for kdict in row['daskeys']:
                if  kdict['key'] == daskey: 
                    lkey = kdict['map']
                else:
                    continue
                if  value and kdict['pattern']:
                    pat = re.compile(kdict['pattern'])
                    if  pat.match(str(value)): 
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
        query = {'system':system, 'das2api.api_param' : api_input_name}
        names = []
        for adas in self.col.find(query, ['das2api']):
            for row in adas['das2api']:
                aparam = row['api_param']
                daskey = row['das_key']
                if  aparam == api_input_name and daskey not in names:
                    names.append(daskey)
        return names

    def das2api(self, system, daskey, value=None):
        """
        Translates DAS QL key into data-service API input parameter
        """
        query = {'system':system, 'das2api.das_key': daskey}
        names = []
        for adas in self.col.find(query, ['das2api']):
            for row in adas['das2api']:
                api_param = row['api_param']
                if  row['das_key'] != daskey:
                    continue
                if  value and row['pattern']:
                    pat = re.compile(row['pattern'])
                    if  pat.match(str(value)):
                        if  api_param not in names:
                            names.append(api_param)
                else:
                    if  api_param not in names:
                        names.append(api_param)
        return names

    def notations(self, system=None):
        """
        Return DAS notation map.
        """
        notationmap = {}
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
        name = api_param
        if  self.notationcache.has_key((system, api_param)):
            for item in self.notationcache[(system, api_param)]:
                _api, das_name = item
                if  _api:
                    if  _api == api:
                        name = das_name
                        break
                else: # valid for all API names
                    name = das_name
#        print "\n# notation2das srv=%s, arg=%s, api=%s, new=%s" \
#                % ( system, api_param, api, name )
        return name

    def api2daskey(self, system, api):
        """
        Returns list of DAS keys which cover provided data-service API
        """
        query = {'system':system, 'urn':api}
        keys = []
        for row in self.col.find(query):
            for entry in row['daskeys']:
                keys.append(entry['key'])
        return keys

    def apitag(self, system, api):
        """
        Return apitag if it exists for given api name.
        """
        spec = {'system':system, 'urn': api}
        res  = self.col.find_one(spec)
        if  res.has_key('apitag') and res['apitag']:
            return res['apitag']
        return None

    def servicemap(self, system):
        """
        Constructs data-service map, e.g.

        .. doctest::

            {api: {keys:[list of DAS keys], params: args, 
             url:url, format:ext, expire:exp} }
        """
        query = {'system':system, 'urn':{'$ne':None}}
        smap = {}
        for row in self.col.find(query):
            url  = row['url']
            exp  = row['expire']
            ext  = row['format']
            api  = row['urn']
            wild = row.get('wild_card', '*')
            keys = []
            for entry in row['daskeys']:
                keys.append(entry['key'])
            params = dict(row['params'])
            smap[api] = dict(keys=keys, params=params, url=url, expire=exp,
                                format=ext, wild_card=wild)
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
        return [daskey]
