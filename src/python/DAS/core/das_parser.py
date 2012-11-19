#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS Query Language parser.
"""

__author__ = "Valentin Kuznetsov"

from DAS.utils.das_config import das_readconfig
from DAS.core.das_mapping_db import DASMapping
from DAS.core.das_analytics_db import DASAnalytics
from DAS.core.das_ql import das_special_keys, das_operators
from DAS.core.das_ply import DASPLY, ply2mongo
from DAS.utils.utils import print_exc, genkey
from DAS.utils.regex import last_key_pattern
from DAS.utils.logger import PrintManager
from DAS.core.das_parsercache import DASParserDB
from DAS.core.das_parsercache import PARSERCACHE_VALID, PARSERCACHE_INVALID

def decompose(query):
    """Extract selection keys and conditions from input query"""
    skeys = query.get('fields', [])
    cond  = query.get('spec', {})
    return skeys, cond

def ambiguous_msg(query, keys):
    """
    Provide a message for ambiguous query
    """
    query = str(query).strip()
    msg  = 'Ambiguous query "%s"\n\n' % query
    msg += 'Please provide selection key, e.g.\n'
    for key in keys:
        msg += '%s %s\n' % (key, query)
    msg += '\n'
    msg += 'DAS-QL syntax: selection_key key=value'
    return msg

def ambiguos_val_msg(query, key, val):
    """Provide message for ambiguos value in DAS query for given key"""
    query = str(query).strip()
    msg  = 'Provided query=%s\n' % query
    msg += 'Contains ambiguous condition %s=%s\n' % (key, val)
    msg += 'DAS does not support AND|OR operations, please revisit your '
    msg += 'query and choose either value'
    return msg

class QLManager(object):
    """
    DAS QL manager.
    """
    def __init__(self, config=None):
        if  not config:
            config = das_readconfig()
        if  not config.has_key('dasmapping'):
            config['dasmapping'] = DASMapping(config)
        if  not config.has_key('dasanalytics'):
            config['dasanalytics'] = DASAnalytics(config)
        if  not config['dasmapping'].check_maps():
            msg = "No DAS maps found in MappingDB"
            raise Exception(msg)
        self.map         = config['dasmapping']
        self.analytics   = config['dasanalytics']
        self.dasservices = config['services']
        self.daskeysmap  = self.map.daskeys()
        self.operators   = list(das_operators())
        self.daskeys     = list(das_special_keys())
        self.verbose     = config['verbose']
        self.logger      = PrintManager('QLManger', self.verbose)
        for val in self.daskeysmap.values():
            for item in val:
                self.daskeys.append(item)
        parserdir   = config['das']['parserdir']
        self.dasply = DASPLY(parserdir, self.daskeys, self.dasservices, 
                verbose=self.verbose)

        self.enabledb = config['parserdb']['enable']
        if  self.enabledb:
            self.parserdb = DASParserDB(config)

    def parse(self, query):
        """
        Parse input query and return query in MongoDB form.
        Optionally parsed query can be written into analytics DB.
        """
        mongo_query = self.mongo_query(query)
        self.convert2skeys(mongo_query)
        return mongo_query

    def add_to_analytics(self, query, mongo_query):
        "Add DAS query to analytics DB"
        self.analytics.add_query(query, mongo_query)

    def mongo_query(self, query):
        """
        Return mongo query for provided input query
        """
        # NOTE: somehow I need to keep build call just before using
        # PLY parser, otherwise it fails to parse.
        self.dasply.build()
        if  self.verbose:
            msg = "input query='%s'" % query
            self.logger.debug(msg)
            self.dasply.test_lexer(query)
        if  self.enabledb:
            status, value = self.parserdb.lookup_query(query)
            if status == PARSERCACHE_VALID and \
                len(last_key_pattern.findall(query)) == 0:
                mongo_query = value
            elif status == PARSERCACHE_INVALID:
                raise Exception(value)
            else:
                try:
                    ply_query = self.dasply.parser.parse(query)
                    mongo_query = ply2mongo(ply_query)
                except Exception as exc:
#                    self.parserdb.insert_invalid_query(query, exp)
                    print "Fail to parse query=%s" % query
                    raise exc
                try:
                    self.parserdb.insert_valid_query(query, mongo_query)
                except Exception as exc:
                    msg = "Fail to insert into parserdb"
                    print_exc(msg, print_traceback=True)
                    raise exc
        else:
            try:
                ply_query   = self.dasply.parser.parse(query)
                mongo_query = ply2mongo(ply_query)
            except Exception as exc:
                msg = "Fail to parse query='%s'" % query
                print_exc(msg, print_traceback=False)
                raise exc
        if  set(mongo_query.keys()) & set(['fields', 'spec']) != \
                set(['fields', 'spec']):
            raise Exception('Invalid MongoDB query %s' % mongo_query)
        if  not mongo_query['fields'] and len(mongo_query['spec'].keys()) > 1:
            raise Exception(ambiguous_msg(query, mongo_query['spec'].keys()))
        for key, val in mongo_query['spec'].iteritems():
            if  isinstance(val, list):
                raise Exception(ambiguos_val_msg(query, key, val))
        return mongo_query

    def convert2skeys(self, mongo_query):
        """
        Convert DAS input keys into DAS selection keys.
        """
        if  not mongo_query['spec']:
            for key in mongo_query['fields']:
                for system in self.map.list_systems():
                    mapkey = self.map.find_mapkey(system, key)
                    if  mapkey:
                        mongo_query['spec'][mapkey] = '*'
            return
        spec = mongo_query['spec']
        to_replace = []
        for key, val in spec.iteritems():
            for system in self.map.list_systems():
                mapkey = self.map.find_mapkey(system, key, val)
                if  mapkey and mapkey != key and \
                    mongo_query['spec'].has_key(key):
                    to_replace.append((key, mapkey))
                    continue
        for key, mapkey in to_replace:
            if  mongo_query['spec'].has_key(key):
                mongo_query['spec'][mapkey] = mongo_query['spec'][key]
                del mongo_query['spec'][key]
        
    def services(self, query):
        """Find out DAS services to use for provided query"""
        skeys, cond = decompose(query)
        if  not skeys:
            skeys = []
        if  isinstance(skeys, str):
            skeys = [skeys]
        slist = []
        # look-up services from Mapping DB
        for key in skeys + [i for i in cond.keys()]:
            for service, keys in self.daskeysmap.iteritems():
                if  service not in self.dasservices:
                    continue
                value = cond.get(key, None)
                daskeys = self.map.find_daskey(service, key, value)
                if  set(keys) & set(daskeys) and service not in slist:
                    slist.append(service)
        # look-up special key condition
        requested_system = query.get('system', None)
        if  requested_system:
            if  isinstance(requested_system, str):
                requested_system = [requested_system]
            return list( set(slist) & set(requested_system) )
        return slist

    def service_apis_map(self, query):
        """
        Find out which APIs correspond to provided query.
        Return a map of found services and their apis.
        """
        skeys, cond = decompose(query)
        if  not skeys:
            skeys = []
        if  isinstance(skeys, str):
            skeys = [skeys]
        adict = {}
        mapkeys = [key for key in cond.keys() if key not in das_special_keys()]
        services = self.services(query)
        for srv in services:
            alist = self.map.list_apis(srv)
            for api in alist:
                daskeys = self.map.api_info(api)['das_map']
                maps = [r['rec_key'] for r in daskeys]
                if  set(mapkeys) & set(maps) == set(mapkeys): 
                    if  adict.has_key(srv):
                        new_list = adict[srv] + [api]
                        adict[srv] = list( set(new_list) )
                    else:
                        adict[srv] = [api]
        return adict

    def params(self, query):
        """
        Return dictionary of parameters to be used in DAS Core:
        selection keys, conditions and services.
        """
        skeys, cond = decompose(query)
        services = []
        for srv in self.services(query):
            if  srv not in services:
                services.append(srv)
        return dict(selkeys=skeys, conditions=cond, services=services)

# Invoke QLManager once (singleton)
QL_MANAGER_OBJECTS = {}

def ql_manager(config=None):
    "Return QLManager instance"
    key = genkey(str(config))
    if  QL_MANAGER_OBJECTS.has_key(key):
        obj = QL_MANAGER_OBJECTS[key]
    else:
        obj = QLManager(config)
        QL_MANAGER_OBJECTS[key] = obj
    return obj
