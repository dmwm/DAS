#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703,R0902,R0904,R0914

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
from collections import defaultdict
import hashlib


# monogo db modules
from pymongo import DESCENDING
from pymongo.errors import ConnectionFailure

# DAS modules
from DAS.utils.utils import dastimestamp, print_exc, md5hash, TRANSIENT_FIELDS
from DAS.utils.utils import gen2list, parse_dbs_url, get_dbs_instance
from DAS.utils.das_db import db_connection, is_db_alive, create_indexes
from DAS.utils.das_db import find_one
from DAS.utils.logger import PrintManager
from DAS.utils.thread import start_new_thread

import DAS.utils.jsonwrapper as json

def check_map_record(record):
    "Check hash of given map record"
    rec = dict(record)
    # remove _id MongoDB Object
    for key in ['_id']+TRANSIENT_FIELDS:
        if  key in rec:
            del rec[key]
    if  'hash' in rec:
        md5 = rec.pop('hash')
        rec_md5 = md5hash(rec)
        if  rec_md5 != md5:
            err  = 'Invalid hash record:\n%s\n' % json.dumps(rec)
            err += '\nrecord hash  : %s' % md5
            err += '\nobtained hash: %s\n' % md5hash(rec)
            raise Exception(err)


def verification_token(iterator):
    """
    given an iterator for all dasmap records,
    returns a verification token, that defines the state of the dasmpas
    """
    all_hashes = hashlib.md5()
    hashes = sorted(row['hash'] for row in iterator if 'hash' in row)
    for hash_ in hashes:
        all_hashes.update(hash_)
    return all_hashes.hexdigest()


class DASMapping(object):
    """
    This class manages DAS mapping DB.
    """
    __cached_inst = None
    __cached_params = None

    def __new__(cls, config):
        """
        creates a new instance of the class and cache it or return an existing
         instance if one exists (only when the params match).

        only the last instance is cached, but this simplifies the implementation
        as the param 'config' might be a complex unhashable object.
        """
        # check if we can reuse an existing instance
        if cls.__cached_inst and cls.__cached_params == config:
            if  config['verbose']:
                print "DASMapping::__new__: returning a cached instance"
            return cls.__cached_inst

        # otherwise create and initialize a new instance
        if  config['verbose']:
            print "DASMapping::__new__: creating a new instance"
        self = object.__new__(cls)

        self.verbose  = config['verbose']
        self.logger   = PrintManager('DASMapping', self.verbose)
        self.services = config['services']
        self.dburi    = config['mongodb']['dburi']
        self.dbname   = config['mappingdb']['dbname']
        self.colname  = config['mappingdb']['collname']
        self.map_test = config.get('map_test', True)
        self.main_dbs = config['das'].get('main_dbs', 'dbs3')
        self.dbsinsts = config['das'].get('dbs_instances', [])

        msg = "%s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)
        
        self.init()

        self.daskeyscache = {}         # to be filled at run time
        self.systems = []              # to be filled at run time
        self.dasmapscache = {}         # to be filled at run time
        self.keymap = {}               # to be filled at run time
        self.presentationcache = {}    # to be filled at run time
        self.reverse_presentation = {} # to be filled at run time
        self.notationcache = {}        # to be filled at run time
        self.diffkeycache = {}         # to be filled at run time
        self.apicache = {}             # to be filled at run time
        self.dbs_global_url = None     # to be determined at run time
        self.dbs_inst_names = None     # to be determined at run time
        self.load_maps()

        # cache the instance and return it
        DASMapping.__cached_inst = self
        DASMapping.__cached_params = config
        return self

    @property
    def col(self):
        "Return MongoDB collection object"
        conn = db_connection(self.dburi)
        dbc  = conn[self.dbname]
        col  = dbc[self.colname]
        return col

    # ===============
    # Management APIs
    # ===============
    def load_maps(self):
        "Helper function to reload DAS maps"
        self.init_dasmapscache()
        self.init_notationcache()
        self.init_presentationcache()
        self.systems = None        # re-initialize DAS system list
        self.list_systems()
        self.dbs_global_url = None # re-initialize DAS dbs global url
        self.dbs_url()
        self.dbs_inst_names = None # re-initialize DAS dbs instances
        self.dbs_instances()

    def init_dasmapscache(self, records=[]):
        "Read DAS maps and initialize DAS API maps"
        if  not records:
            spec = {'type':'service'}
            records = self.col.find(spec, exhaust=True)
        for row in records:
            if  'urn' in row:
                api = row['urn']
                srv = row['system']
                for dmap in row['das_map']:
                    for key, val in dmap.iteritems():
                        if  key == 'pattern':
                            pat = re.compile(val)
                            dmap[key] = pat
                key = (row['system'], row['urn'])
                self.dasmapscache[key] = row

    def init_notationcache(self):
        """
        Initialize notation cache by reading notations.
        """
        for system, notations in self.notations().iteritems():
            for row in notations:
                key = system, row['api_output']
                if  key in self.notationcache:
                    self.notationcache[key] += [ (row['api'], row['rec_key']) ]
                else:
                    self.notationcache[key] = [ (row['api'], row['rec_key']) ]

    def init_presentationcache(self):
        """
        Initialize presentation cache by reading presentation map.
        """
        spec  = {'type':'presentation'}
        data  = find_one(self.col, spec)
        if  data:
            self.presentationcache = data['presentation']
            for daskey, uilist in self.presentationcache.iteritems():
                for row in uilist:
                    link = None
                    if  'link' in row:
                        link = row['link']
                    if  'diff' in row:
                        self.diffkeycache[daskey] = row['diff']
                    tdict = {daskey : {'mapkey': row['das'], 'link': link}}
                    if  row['ui'] in self.reverse_presentation:
                        self.reverse_presentation[row['ui']].update(tdict)
                    else:
                        self.reverse_presentation[row['ui']] = \
                                {daskey : {'mapkey': row['das'], 'link': link}}

    def das_presentation_map(self):
        "Read DAS presentation map"
        spec  = {'type':'presentation'}
        data  = find_one(self.col, spec)
        if  data:
            for daskey, uilist in data.get('presentation', {}).iteritems():
                for row in uilist:
                    if  'link' in row:
                        yield row

    def init(self):
        """
        Establish connection to MongoDB back-end and create DB.
        """
        col = None
        try:
            conn = db_connection(self.dburi)
            if  conn:
                dbc  = conn[self.dbname]
                col  = dbc[self.colname]
#            print "### DASMapping:init started successfully"
        except ConnectionFailure as _err:
            tstamp = dastimestamp('')
            thread = threading.current_thread()
            print "### MongoDB connection failure thread=%s, id=%s, time=%s" \
                    % (thread.name, thread.ident, tstamp)
        except Exception as exc:
            print_exc(exc)
        if  col:
            index = [('type', DESCENDING),
                     ('system', DESCENDING),
                     ('urn', DESCENDING),
                     ('das_map.das_key', DESCENDING),
                     ('das_map.rec_key', DESCENDING),
                     ('das_map.api_arg', DESCENDING),
                     ]
            create_indexes(col, index)

    def delete_db(self):
        """
        Delete mapping DB in MongoDB back-end.
        """
        conn = db_connection(self.dburi)
        if  conn:
            conn.drop_database(self.dbname)

    def delete_db_collection(self):
        """
        Delete mapping DB collection in MongoDB.
        """
        conn = db_connection(self.dburi)
        if  conn:
            dbc  = conn[self.dbname]
            dbc.drop_collection(self.colname)

    def check_maps(self):
        """
        Check Mapping DB and return true/false based on its content
        """
        if  not self.map_test:
            return True # do not test DAS maps, useful for unit tests
        udict = defaultdict(int)
        ndict = defaultdict(int)
        pdict = defaultdict(int)
        adict = {}
        maps_hash = False
        for row in self.col.find(exhaust=True):
            check_map_record(row)
            if  'urn' in row:
                udict[row['system']] += 1
            elif 'notations' in row:
                ndict[row['system']] += 1
            elif 'presentation' in row:
                pdict['presentation'] += 1
            elif 'arecord' in row:
                arec = row['arecord']
                system = arec['system']
                rec = {arec['type']:arec['count']}
                if  system in adict:
                    adict[system].update(rec)
                else:
                    adict[system] = rec
            elif 'verification_token' in row:
                maps_hash = row['verification_token']

        # retrieve uri/notation/presentation maps
        ulist = []
        nlist = []
        for system in adict.keys():
            if  'uri' in adict[system]:
                ulist.append(adict[system]['uri'] == udict[system])
                nlist.append(adict[system]['notations'] == ndict[system])
        status_umap = sum(ulist) == len(ulist)
        status_nmap = sum(nlist) == len(nlist)
        status_pmap = adict.get('presentation', {}).get('presentation', 0) == 1
        # verify completeness of maps
        calc_token = verification_token(self.col.find(exhaust=True))
        status_complete = maps_hash and maps_hash == calc_token
        if  self.verbose:
            print "### DAS map status, umap=%s, nmap=%s, pmap=%s, complete=%s" \
                    % (status_umap, status_nmap, status_pmap, status_complete)
        if not status_complete:
            print "### DAS map hash do not match, got=%s calculated=%s" \
                    % (maps_hash, calc_token)
        # multiply statuses as a result of this map check
        return status_umap*status_nmap*status_pmap*status_complete

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
        self.init_dasmapscache([record])

    # ==================
    # Informational APIs
    # ==================
    def dbs_global_instance(self, system=None):
        "Retrive from mapping DB DBS url and extract DBS instance"
        if  not system:
            system = self.main_dbs
        url = self.dbs_url(system)
        return get_dbs_instance(url)

    def dbs_url(self, system=None):
        "Retrive from mapping DB DBS url"
        if  not system:
            system = self.main_dbs
        systems = self.list_systems()
        dbses   = set(['dbs3'])
        if  dbses & set(systems) != dbses:
            # use caching only when we operate with single DBS
            if  self.dbs_global_url:
                return self.dbs_global_url
        url = None
        for srv in systems:
            if  srv == system:
                apis = self.list_apis(srv)
                url  = self.api_info(srv, apis[0])['url']
                url  = parse_dbs_url(srv, url)
                self.dbs_global_url = url
                return url
        return url

    def dbs_instances(self, system=None):
        "Retrive from mapping DB DBS instances"
        # use dbs istances from the config
        if  self.dbsinsts and not system:
            return self.dbsinsts
        # default dbs
        if  not system:
            system = self.main_dbs
        systems = self.list_systems()
        dbses   = set(['dbs3'])
        if  dbses & set(systems) != dbses:
            # use caching only when we operate with single DBS
            if  self.dbs_inst_names:
                return self.dbs_inst_names
        insts = []
        dbs_global_inst = self.dbs_global_instance(system)
        if  system == 'dbs3' and dbs_global_inst:
            dbs_namespace = dbs_global_inst.split('/')[0]
        else:
            dbs_namespace = None
        for srv in systems:
            if  srv == system:
                apis  = self.list_apis(srv)
                insts = self.api_info(srv, apis[0])['instances']
                if  dbs_namespace:
                    insts = [d for d in insts if d.startswith(dbs_namespace)]
                self.dbs_inst_names = insts
                return insts
        return insts

    def list_systems(self):
        """
        List all DAS systems.
        """
        if  not self.systems:
            spec = { 'type': 'service', 'system' : { '$ne' : None } }
            gen  = (row['system'] \
                    for row in self.col.find(spec, ['system'], exhaust=True))
            self.systems = list( set(gen2list(gen)) & set(self.services) )
        return self.systems

    def list_apis(self, system=None):
        """
        List all APIs.
        """
        if  self.apicache and system in self.apicache:
            return self.apicache[system]
        spec = { 'type': 'service', 'urn' : { '$ne' : None } }
        if  system:
            spec['system'] = system
        gen  = (row['urn'] \
                for row in self.col.find(spec, ['urn'], exhaust=True))
        self.apicache[system] = gen2list(gen)
        return self.apicache[system]

    def api_info(self, srv, api_name):
        """
        Return full API info record.
        """
        return self.dasmapscache[(srv, api_name)]

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
        if  das_system in self.daskeyscache:
            return self.daskeyscache[das_system]

        spec  = { 'type': 'service', 'system' : { '$ne' : None } }
        if  das_system:
            spec  = { 'system' : das_system }
        gen   = (row['system'] \
                for row in self.col.find(spec, ['system'], exhaust=True))
        gen   = [r for r in gen]
        kdict = {}
        for system in gen:
            spec = {'system':system, 'urn':{'$ne':None}}
            keys = []
            for row in self.col.find(spec, exhaust=True):
                for entry in row['das_map']:
                    if  entry['das_key'] not in keys:
                        keys.append(entry['das_key'])
            kdict[system] = keys
        # cache it
        self.daskeyscache[das_system] = kdict
        return kdict

    # ============
    # Look-up APIs
    # ============
    def api_lkeys(self, das_system, api):
        """
        Return DAS lookup keys for given das system and api
        """
        entry = self.dasmapscache[(das_system, api)]
        skeys = entry['lookup'].split(',')
        return skeys

    def primary_key(self, das_system, urn):
        """
        Return DAS primary key for provided system and urn. The DAS primary key
        is a first entry in *lookup* attribute of DAS API record.
        """
        spec = {'system':das_system, 'urn':urn}
        record = find_one(self.col, spec)
        if  not record:
            return None
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
        spec = {'system':das_system, 'urn':urn}
        record = find_one(self.col, spec)
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
        daskeys = []
        for key, record in self.dasmapscache.iteritems():
            srv, _urn = key
            if  das_system != srv:
                continue
            for row in record['das_map']:
                das_key = row['das_key']
                rec_key = row['rec_key']
                if  rec_key != map_key:
                    continue
                pat = row.get('pattern', None)
                if  value:
                    if  pat:
                        if  pat.match(str(value)):
                            daskeys.append(das_key)
                        else:
                            msg += '-- reject key=%s, val=%s, pat=%s\n'\
                                    % (map_key, value, pat.pattern)
                            self.logger.debug(msg)
                    else:
                        daskeys.append(das_key)
                else:
                    daskeys.append(das_key)
        return daskeys

    def find_mapkey(self, das_system, das_key, value=None):
        """
        Find map key for given system and das key.
        """
        msg   = 'system=%s\n' % das_system
        for key, record in self.dasmapscache.iteritems():
            srv, _urn = key
            if  das_system != srv:
                continue
            for row in record['das_map']:
                if  row['das_key'] != das_key:
                    continue
                rec_key = row['rec_key']
                pat = row.get('pattern', None)
                if  value:
                    if  pat:
                        if  pat.match(str(value)):
                            return rec_key
                        else:
                            msg += '-- reject key=%s, val=%s, pat=%s\n'\
                                    % (das_key, value, pat.pattern)
                            self.logger.debug(msg)
                            continue
                    else:
                        return rec_key
                else:
                    return rec_key

    def mapkeys(self, daskey):
        """
        Find all lookup keys (primary keys) for a given daskey
        """
        if  daskey in self.keymap:
            return self.keymap[daskey]
        spec = {'das_map.das_key' : daskey}
        mapkeys = []
        for row in self.col.find(spec, ['das_map'], exhaust=True):
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
        spec  = { 'system' : das_system, 'das_map.rec_key': map_key }
        apilist = []
        for row in self.col.find(spec, ['urn'], exhaust=True):
            if  'urn' in row and row['urn'] not in apilist:
                apilist.append(row['urn'])
        return apilist

    def find_system(self, key):
        """
        Return system name for provided DAS key.
        """
        spec = { 'das_map.das_key' : key }
        gen  = (row['system'] \
                for row in self.col.find(spec, ['system'], exhaust=True))
        systems = []
        for system in gen:
            if  system not in systems:
                systems.append(system)
        systems.sort()
        return systems

    def lookup_keys(self, system, api, daskey=None, value=None):
        """
        Returns lookup keys for given system and provided
        selection DAS key, e.g. block => block.name
        """
        entry = self.dasmapscache.get((system, api), None)
        if  not entry:
            return []
        lkeys = entry.get('lookup', []).split(',')
        rkeys = []
        if  daskey in lkeys:
            for dmap in entry['das_map']:
                rec_key = dmap['rec_key']
                if  daskey:
                    if  dmap['das_key'] == daskey:
                        pat = dmap.get('pattern', None)
                        if  value:
                            if  pat.match(str(value)):
                                rkeys.append(rec_key)
                        else:
                            if  rec_key not in rkeys:
                                rkeys.append(rec_key)
                else:
                    rkeys.append(rec_key)
        return rkeys

    def api2das(self, system, api_input_name):
        """
        Translates data-service API input parameter into DAS QL key,
        e.g. run_number => run.
        """
        query = {'system':system, 'das_map.api_arg' : api_input_name}
        names = []
        for adas in self.col.find(query, ['das_map'], exhaust=True):
            for row in adas['das_map']:
                try:
                    if  'api_arg' in row:
                        aparam = row['api_arg']
                        daskey = row['das_key']
                        if  aparam == api_input_name and daskey not in names:
                            names.append(daskey)
                except Exception, err:
                    print "ERROR: look-up api_param/das_key in", row
                    raise err
        return names

    def check_api_match(self, system, api, icond):
        "Check if given API covers condition parameters"
        entry = self.dasmapscache.get((system, api), None)
        names = []
        if  not entry:
            return False
        ikeys = [k.split('.')[0] for k in icond.keys()]
        dkeys = []
        for row in entry.get('das_map', []):
            if  'api_arg' in row:
                das_key = row['das_key']
                dkeys.append(das_key)
            else:
                dkeys.append(row['das_key'])
        if  set(ikeys) & set(dkeys) == set(ikeys):
            return True
        return False

    def das2api(self, system, api, rec_key, value=None):
        """
        Translates DAS record key into data-service API input parameter,
        e.g. run.number => run_number
        """
        entry = self.dasmapscache.get((system, api), None)
        names = []
        if  not entry:
            return [rec_key]
        for row in entry.get('das_map', []):
            if  'api_arg' in row:
                api_param = row['api_arg']
                pat = row.get('pattern', None)
                if  row['rec_key'] != rec_key:
                    continue
                if  value and pat:
                    if  pat.match(str(value)):
                        if  api_param not in names:
                            names.append(api_param)
                else:
                    if  api_param not in names:
                        names.append(api_param)
            else:
                names.append(row['rec_key'])
        return names

    def notations(self, system=None):
        """
        Return DAS notation map.
        """
        notationmap = {}
        spec = {'type':'notation'}
        if  system:
            spec['system'] = system
        for item in self.col.find(spec, exhaust=True):
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
        if  (system, api_param) in self.notationcache:
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
        spec = {'system':system, 'urn':api}
        keys = []
        for row in self.col.find(spec, exhaust=True):
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
        spec = {'system':system, 'urn':{'$ne':None}}
        smap = {}
        for row in self.col.find(spec, exhaust=True):
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
        if  daskey in self.presentationcache:
            return self.presentationcache[daskey]
        return [daskey]

    def daskey_from_presentation(self, uikey):
        """
        Return triplet (DAS key, DAS access key, link)
        associated with provided UI key.
        """
        if  uikey in self.reverse_presentation:
            return self.reverse_presentation[uikey]

    def diff_keys(self, daskey):
        """
        Return diff keys for provided DAS key.
        """
        if  daskey in self.diffkeycache:
            return self.diffkeycache[daskey]
        return []

    def inputvalues_uris(self):
        """
        Return the info on how to fetch the list of allowed input values for
        certain commonly used input fields (from enabled DAS systems only)
        """
        uris = []
        for row in self.col.find({'type': 'input_values'}, exhaust=True):
            # check that system is active
            if row['system'] not in self.services:
                continue
            uris.extend(row['input_values'])
        return uris
