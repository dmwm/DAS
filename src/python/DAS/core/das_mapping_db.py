#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0703

"""
DAS mapping DB module
"""

__revision__ = "$Id: das_mapping_db.py,v 1.36 2010/04/14 16:56:28 valya Exp $"
__version__ = "$Revision: 1.36 $"
__author__ = "Valentin Kuznetsov"

import re

# monogo db modules
from pymongo import DESCENDING

# DAS modules
from DAS.utils.utils import gen2list
from DAS.utils.das_db import db_connection, create_indexes
from DAS.utils.logger import PrintManager

class DASMapping(object):
    """
    This class manages DAS mapping DB.
    """
    def __init__(self, config):
        self.verbose  = config['verbose']
        self.logger   = PrintManager('DASMapping', self.verbose)
        self.services = config['services']
        self.dburi    = config['mongodb']['dburi']
        self.dbname   = config['mappingdb']['dbname']
        self.colname  = config['mappingdb']['collname']

        msg = "%s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)
        
        self.create_db()

        self.keymap = {}               # to be filled at run time
        self.presentationcache = {}    # to be filled at run time
        self.reverse_presentation = {} # to be filled at run time
        self.notationcache = {}        # to be filled at run time
        self.diffkeycache = {}         # to be filled at run time
        self.apicache = {}             # to be filled at run time
        self.apiinfocache = {}         # to be filled at run time
        self.init_notationcache()
        self.init_presentationcache()

    # ===============
    # Management APIs
    # ===============
    def init_notationcache(self):
        """
        Initialize notation cache by reading notations.
        """
        for system, notations in self.notations().iteritems():
            for row in notations:
                key = system, row['notation']
                if  self.notationcache.has_key(key):
                    self.notationcache[key] += [ (row['api'], row['map']) ]
                else:
                    self.notationcache[key] = [ (row['api'], row['map']) ]

    def init_presentationcache(self):
        """
        Initialize presentation cache by reading presentation map.
        """
        query = {'presentation':{'$ne':None}}
        data  = self.col.find_one(query)
        if  data:
            self.presentationcache = data['presentation']
            for daskey, uilist in self.presentationcache.iteritems():
                for row in uilist:
                    link = None
                    if  row.has_key('link'):
                        link = row['link']
                    if  row.has_key('diff'):
                        self.diffkeycache[daskey] = row['diff']
                    tdict = {daskey : {'mapkey': row['das'], 'link': link}}
                    if  self.reverse_presentation.has_key(row['ui']):
                        self.reverse_presentation[row['ui']].update(tdict)
                    else:
                        self.reverse_presentation[row['ui']] = \
                                {daskey : {'mapkey': row['das'], 'link': link}}

    def create_db(self):
        """
        Establish connection to MongoDB back-end and create DB.
        """
        self.conn = db_connection(self.dburi)
        self.db   = self.conn[self.dbname]
        self.col  = self.db[self.colname]

    def delete_db(self):
        """
        Delete mapping DB in MongoDB back-end.
        """
        self.conn.drop_database(self.dbname)

    def delete_db_collection(self):
        """
        Delete mapping DB collection in MongoDB.
        """
        self.db.drop_collection(self.colname)

    def check_maps(self):
        """
        Check if there are records in Mapping DB
        """
        return self.col.count()

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
        msg = 'record=%s' % record
        self.logger.debug(msg)
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
            create_indexes(self.col, index)

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

    def list_apis(self, system=None):
        """
        List all APIs.
        """
        if  self.apicache and self.apicache.has_key(system):
            return self.apicache[system]
        cond = { 'urn' : { '$ne' : None } }
        if  system:
            cond['system'] = system
        gen  = (row['urn'] for row in self.col.find(cond, ['urn']))
        self.apicache[system] = gen2list(gen)
        return self.apicache[system]

    def api_info(self, api_name):
        """
        Return full API info record.
        """
        return self.apiinfocache.get(\
                api_name, self.col.find_one({'urn':api_name}))

    def relational_keys(self, system1, system2):
        """
        Return a list of relational keys between provided systems
        """
        for system, keys in self.daskeys().iteritems():
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
        msg   = 'system=%s\n' % das_system
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
                                    msg += '-- reject key=%s, val=%s, pat=%s\n'\
                                            % (map_key, value, pval)
                                    self.logger.debug(msg)
                            else:
                                daskeys.append(dkey['key'])
                        else:
                            daskeys.append(dkey['key'])
        return daskeys

    def find_mapkey(self, das_system, das_key, value=None):
        """
        Find map key for given system and das key.
        """
        msg   = 'system=%s\n' % das_system
        cond  = { 'system' : das_system, 'daskeys.key': das_key }
        for row in self.col.find(cond, ['daskeys', 'urn']):
            if  row and row.has_key('daskeys'):
                for key in row['daskeys']:
                    if  key.has_key('map') and key['key'] == das_key:
                        if  value:
                            pval = key.get('pattern', '')
                            pat = re.compile(pval)
                            if  pat.match(str(value)):
                                return key['map']
                            else:
                                msg += '-- reject key=%s, val=%s, pat=%s\n'\
                                        % (das_key, value, key['pattern'])
                                self.logger.debug(msg)
                                continue
                        else:
                            return key['map']

    def mapkeys(self, daskey):
        """
        Find primary key for a given daskey
        """
        if  self.keymap.has_key(daskey):
            return self.keymap[daskey]
        spec = {'daskeys.key' : daskey}
        mapkeys = []
        for row in self.col.find(spec, ['daskeys']):
            for kmap in row['daskeys']:
                if  kmap['key'] == daskey and kmap['map'] not in mapkeys:
                    mapkeys.append(kmap['map'])
        self.keymap[daskey] = mapkeys
        return self.keymap[daskey]

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
                try:
                    aparam = row['api_param']
                    daskey = row['das_key']
                    if  aparam == api_input_name and daskey not in names:
                        names.append(daskey)
                except Exception, err:
                    print "ERROR: look-up api_param/das_key in", row
                    raise err
        return names

    def das2api(self, system, daskey, value=None, api=None):
        """
        Translates DAS QL key into data-service API input parameter
        """
        query = {'system':system}
        if api: # only check this api
            query['urn'] = api 
        names = []
        for adas in self.col.find(query, ['das2api']):
            if  not adas.has_key('das2api'):
                continue
            if  not adas['das2api']:
                names = [daskey]
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
            ckey = row.get('ckey')
            cert = row.get('cert')
            services = row.get('services', '')
            keys = []
            for entry in row['daskeys']:
                keys.append(entry['key'])
            params = dict(row['params'])
            smap[api] = dict(keys=keys, params=params, url=url, expire=exp,
                            format=ext, wild_card=wild, ckey=ckey, cert=cert,
                            services=services)
        return smap

    def presentation(self, daskey):
        """
        Return web UI presentation keys for provided DAS keyword.
        For example once asked for block we present block.name, block.size, etc.
        """
        if  self.presentationcache.has_key(daskey):
            return self.presentationcache[daskey]
        return [daskey]

    def daskey_from_presentation(self, uikey):
        """
        Return triplet (DAS key, DAS access key, link)
        associated with provided UI key.
        """
        if  self.reverse_presentation.has_key(uikey):
            return self.reverse_presentation[uikey]

    def diff_keys(self, daskey):
        """
        Return diff keys for provided DAS key.
        """
        if  self.diffkeycache.has_key(daskey):
            return self.diffkeycache[daskey]
        return []
