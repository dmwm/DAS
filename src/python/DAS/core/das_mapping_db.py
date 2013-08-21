#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0703,R0902,R0904,R0914

"""
DAS mapping DB module. It provides access to DAS API maps. Every map consists
of the following structure:

.. doctest::

    urn: myApi
    url: http://a.b.com
    params: {'file':'required'}
    lookup: file
    das_map: [{'das_key':'file, 'rec_key':'file.name', 'api_arg':'file'}]

Here *urn* denotes data-service API, *url, params* are URL and input arguments
publically available data-service API. The *lookup* attribute represents the
entities which are accessible to look-up in DAS queries (it can be a list of
fields) and *das_map* provides actual mapping from DAS key to record key and
associated api argument. Please note that the first record in *das_map*
represents DAS primary key.
"""

__author__ = "Valentin Kuznetsov"

import re
import time
import threading

# monogo db modules
from pymongo import DESCENDING
from pymongo.errors import ConnectionFailure

# DAS modules
from DAS.utils.utils import dastimestamp, print_exc
from DAS.utils.utils import gen2list, parse_dbs_url, get_dbs_instance
from DAS.utils.das_db import db_connection, is_db_alive, create_indexes
from DAS.utils.logger import PrintManager
from DAS.utils.thread import start_new_thread

def db_monitor(uri, func, sleep, reload_map, reload_time):
    """
    Check status of MongoDB connection and reload DAS maps once in a while.
    """
    time0 = time.time()
    while True:
        conn = db_connection(uri)
        if  not conn or not is_db_alive(uri):
            try:
                conn = db_connection(uri)
                func()
                if  conn:
                    print "### db_monitor re-established connection %s" % conn
                else:
                    print "### db_monitor, lost connection"
            except:
                pass
        if  conn:
            if  time.time()-time0 > reload_time:
                msg = "call %s" % reload_map
                print dastimestamp(), msg
                try:
                    reload_map()
                except Exception as err:
                    print dastimestamp('DAS ERROR '), str(err)
                time0 = time.time()
        time.sleep(sleep)

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
        
        self.conn = None # MongoDB connection, defined at run-time
        self.dbc  = None # MongoDB database, defined at run-time
        self.col  = None # MongoDB collection, defined at run-time
        self.init()

        # Monitoring thread which performs auto-reconnection to MongoDB
        thname = 'mappingdb_monitor'
        sleep  = 5
        reload_time = config['mappingdb'].get('reload_time', 86400)
        start_new_thread(thname, db_monitor, (self.dburi, self.init, sleep,
            self.load_maps, reload_time))

        self.apilkeys = {}             # to be filled at run time
        self.keymap = {}               # to be filled at run time
        self.presentationcache = {}    # to be filled at run time
        self.reverse_presentation = {} # to be filled at run time
        self.notationcache = {}        # to be filled at run time
        self.diffkeycache = {}         # to be filled at run time
        self.apicache = {}             # to be filled at run time
        self.apiinfocache = {}         # to be filled at run time
        self.dbs_global_url = None     # to be determined at run time
        self.dbs_inst_names = None     # to be determined at run time
        self.load_maps()

    # ===============
    # Management APIs
    # ===============
    def load_maps(self):
        "Helper function to reload DAS maps"
        self.init_apilkeyscache()
        self.init_notationcache()
        self.init_presentationcache()

    def init_apilkeyscache(self):
        "Read DAS maps and initialize apilkeys"
        for row in self.col.find({}):
            if  row.has_key('urn'):
                api = row['urn']
                system = row['system']
                lookup = row['lookup']
                key    = (system, api)
                for lkey in lookup.split(','):
                    self.apilkeys.setdefault(key, []).append(lkey)

    def init_notationcache(self):
        """
        Initialize notation cache by reading notations.
        """
        for system, notations in self.notations().iteritems():
            for row in notations:
                key = system, row['api_output']
                if  self.notationcache.has_key(key):
                    self.notationcache[key] += [ (row['api'], row['rec_key']) ]
                else:
                    self.notationcache[key] = [ (row['api'], row['rec_key']) ]

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

    def das_presentation_map(self):
        "Read DAS presentation map"
        query = {'presentation':{'$ne':None}}
        data  = self.col.find_one(query)
        if  data:
            for daskey, uilist in data.get('presentation', {}).iteritems():
                for row in uilist:
                    if  row.has_key('link'):
                        yield row

    def init(self):
        """
        Establish connection to MongoDB back-end and create DB.
        """
        try:
            self.conn = db_connection(self.dburi)
            if  self.conn:
                self.dbc  = self.conn[self.dbname]
                self.col  = self.dbc[self.colname]
            print "### DASMapping:init started successfully"
        except ConnectionFailure as _err:
            tstamp = dastimestamp('')
            thread = threading.current_thread()
            print "### MongoDB connection failure thread=%s, id=%s, time=%s" \
                    % (thread.name, thread.ident, tstamp)
        except Exception as exc:
            print_exc(exc)
            self.conn = None
            self.dbc  = None
            self.col  = None

    def delete_db(self):
        """
        Delete mapping DB in MongoDB back-end.
        """
        if  self.conn:
            self.conn.drop_database(self.dbname)

    def delete_db_collection(self):
        """
        Delete mapping DB collection in MongoDB.
        """
        if  self.dbc:
            self.dbc.drop_collection(self.colname)

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
             params : [{"apiversion":1_2_2, se:"*"}]
             lookup : block
             das_map: [
                 {"das_key":"block", "rec_key":"block.name"},
                 {"das_key":"site", "rec_key":"site.name", "api_arg":"se", "pattern":"^T[0-3]_},
             ]
            }

        Example of notation record:

        .. doctest::

             notations: [
                 {"api_output" : "storage_element_name", "rec_key":"site", "api": ""},
             ]
        """
        msg = 'record=%s' % record
        self.logger.debug(msg)
        self.col.insert(record)
        index = None
        if  record.has_key('urn'):
            index = [('system', DESCENDING),
                     ('das_map.das_key', DESCENDING),
                     ('das_map.rec_key', DESCENDING),
                     ('das_map.api_arg', DESCENDING),
                     ('urn', DESCENDING) ]
        elif record.has_key('notations'):
            index = [('system', DESCENDING), 
                     ('notations.api_output', DESCENDING)]
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
    def dbs_global_instance(self):
        "Retrive from mapping DB DBS url and extract DBS instance"
        url = self.dbs_url()
        return get_dbs_instance(url)

    def dbs_url(self):
        "Retrive from mapping DB DBS url"
        if  self.dbs_global_url:
            return self.dbs_global_url
        url = None
        for srv in self.list_systems():
            if  srv.find('dbs') != -1:
                apis = self.list_apis(srv)
                url  = self.api_info(apis[0])['url']
                url  = parse_dbs_url(srv, url)
                self.dbs_global_url = url
                return url
        return url

    def dbs_instances(self):
        "Retrive from mapping DB DBS instances"
        if  self.dbs_inst_names:
            return self.dbs_inst_names
        insts = []
        for srv in self.list_systems():
            if  srv.find('dbs') != -1:
                apis  = self.list_apis(srv)
                insts = self.api_info(apis[0])['instances']
                self.dbs_inst_names = insts
                return insts
        return insts

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
                for entry in row['das_map']:
                    if  entry['das_key'] not in keys:
                        keys.append(entry['das_key'])
            kdict[system] = keys
        return kdict

    # ============
    # Look-up APIs
    # ============
    def api_lkeys(self, das_system, api):
        """
        Return DAS lookup keys for given das system and api
        """
        if  self.apilkeys.has_key((das_system, api)):
            return self.apilkeys[(das_system, api)]
        cond = {'system':das_system, 'urn':api}
        record = self.col.find_one(cond)
        skeys = record['lookup']
        if  skeys.find(',') != -1:
            skeys = skeys.split(',')
        if  isinstance(skeys, basestring):
            skeys = [skeys]
        self.apilkeys[(das_system, api)] = skeys
        return skeys

    def primary_key(self, das_system, urn):
        """
        Return DAS primary key for provided system and urn. The DAS primary key
        is a first entry in *lookup* attribute of DAS API record.
        """
        cond = {'system':das_system, 'urn':urn}
        record = self.col.find_one(cond)
        pkey = record['lookup']
        if  pkey.find(',') != -1:
            pkey = pkey.split(',')[0]
        return pkey
        
    def primary_mapkey(self, das_system, urn):
        """
        Return DAS primary map key for provided system and urn. For example,
        the file DAS key is mapped to file.name, so this API will return
        file.name
        """
        cond = {'system':das_system, 'urn':urn}
        record = self.col.find_one(cond)
        mapkey = []
        for row in record['das_map']:
            lkey = record['lookup']
            if  lkey.find(',') != -1:
                lkey = lkey.split(',')[0]
            if  row['das_key'] == lkey:
                return row['rec_key']
        return mapkey
        
    def find_daskey(self, das_system, map_key, value=None):
        """
        Find das key for given system and map key.
        """
        msg   = 'system=%s\n' % das_system
        cond  = { 'system' : das_system, 'das_map.rec_key': map_key }
        daskeys = []
        for row in self.col.find(cond, ['das_map']):
            if  row and row.has_key('das_map'):
                for dkey in row['das_map']:
                    if  dkey.has_key('das_key'):
                        if  value:
                            pval = dkey.get('pattern', '')
                            if  pval:
                                pat = re.compile(pval)
                                if  pat.match(str(value)):
                                    daskeys.append(dkey['das_key'])
                                else:
                                    msg += '-- reject key=%s, val=%s, pat=%s\n'\
                                            % (map_key, value, pval)
                                    self.logger.debug(msg)
                            else:
                                daskeys.append(dkey['das_key'])
                        else:
                            daskeys.append(dkey['das_key'])
        return daskeys

    def find_mapkey(self, das_system, das_key, value=None):
        """
        Find map key for given system and das key.
        """
        msg   = 'system=%s\n' % das_system
        cond  = { 'system' : das_system, 'das_map.das_key': das_key }
        for row in self.col.find(cond, ['das_map', 'urn']):
            if  row and row.has_key('das_map'):
                for key in row['das_map']:
                    if  key.has_key('rec_key') and key['das_key'] == das_key:
                        if  value:
                            pval = key.get('pattern', '')
                            pat = re.compile(pval)
                            if  pat.match(str(value)):
                                return key['rec_key']
                            else:
                                msg += '-- reject key=%s, val=%s, pat=%s\n'\
                                        % (das_key, value, key['pattern'])
                                self.logger.debug(msg)
                                continue
                        else:
                            return key['rec_key']

    def mapkeys(self, daskey):
        """
        Find all lookup keys (primary keys) for a given daskey
        """
        if  self.keymap.has_key(daskey):
            return self.keymap[daskey]
        spec = {'das_map.das_key' : daskey}
        mapkeys = []
        for row in self.col.find(spec, ['das_map']):
            for kmap in row['das_map']:
                if  kmap['das_key'] == daskey and \
                    kmap['rec_key'] not in mapkeys:
                    mapkeys.append(kmap['rec_key'])
        self.keymap[daskey] = mapkeys
        return self.keymap[daskey]

    def find_apis(self, das_system, map_key):
        """
        Find list of apis which correspond to provided
        system and das map key.
        """
        cond  = { 'system' : das_system, 'das_map.rec_key': map_key }
        apilist = []
        for row in self.col.find(cond, ['urn']):
            if  row.has_key('urn') and row['urn'] not in apilist:
                apilist.append(row['urn'])
        return apilist

    def check_dasmap(self, system, urn, das_map, value=''):
        """
        Check if provided system/urn/das_map is a valid combination
        in mapping db. If value for das_map key is provided we verify
        it against pattern in DB.
        """
        if  not value:
            cond   = {'system':system, 'das_map.rec_key':das_map, 'urn':urn}
            return self.col.find(cond).count()
        cond = { 'system' : system, 'das_map.rec_key' : das_map, 'urn': urn }
        for row in self.col.find(cond, ['das_map']):
            for item in row['das_map']:
                if  not item:
                    continue
                if  item['rec_key'] == das_map:
                    if  item.has_key('pattern'):
                        pat = re.compile(item['pattern'])
                        if  pat.match(str(value)):
                            return True
                    else:
                        return True
        return False

    def find_system(self, key):
        """
        Return system name for provided DAS key.
        """
        cond = { 'das_map.das_key' : key }
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
        query = {'system':system, 'das_map.das_key':daskey}
        if  api:
            query['urn'] = api
        lookupkeys = []
        for row in self.col.find(query):
            for kdict in row['das_map']:
                if  kdict['das_key'] == daskey: 
                    lkey = kdict['rec_key']
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
        query = {'system':system, 'das_map.api_arg' : api_input_name}
        names = []
        for adas in self.col.find(query, ['das_map']):
            for row in adas['das_map']:
                try:
                    if  row.has_key('api_arg'):
                        aparam = row['api_arg']
                        daskey = row['das_key']
                        if  aparam == api_input_name and daskey not in names:
                            names.append(daskey)
                except Exception, err:
                    print "ERROR: look-up api_param/das_key in", row
                    raise err
        return names

    def das2api(self, system, rec_key, value=None, api=None):
        """
        Translates DAS record key into data-service API input parameter,
        e.g. run.number => run_number
        """
        query = {'system':system}
        if api: # only check this api
            query['urn'] = api 
        names = []
        for adas in self.col.find(query, ['das_map']):
            if  not adas.has_key('das_map'):
                continue
            if  not adas['das_map']:
                names = [rec_key]
            for row in adas['das_map']:
                if  row.has_key('api_arg'):
                    api_param = row['api_arg']
                    pattern = row.get('pattern', '')
                    if  row['rec_key'] != rec_key:
                        continue
                    if  value and pattern:
                        pat = re.compile(pattern)
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
            for entry in row['das_map']:
                keys.append(entry['das_key'])
        return keys

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
            lookup = row['lookup']
            wild = row.get('wild_card', '*')
            ckey = row.get('ckey')
            cert = row.get('cert')
            services = row.get('services', '')
            keys = []
            for entry in row['das_map']:
                keys.append(entry['das_key'])
            params = dict(row['params'])
            smap[api] = dict(keys=keys, params=params, url=url, expire=exp,
                            format=ext, wild_card=wild, ckey=ckey, cert=cert,
                            services=services, lookup=lookup)
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
