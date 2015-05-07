#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS Query Language parser.
"""
from __future__ import print_function

__author__ = "Valentin Kuznetsov"

# system modules
import time

from DAS.utils.das_config import das_readconfig
from DAS.core.das_mapping_db import DASMapping
from DAS.core.das_ql import das_special_keys, das_operators
from DAS.core.das_ply import DASPLY, ply2mongo
from DAS.utils.utils import print_exc, genkey, dastimestamp
from DAS.utils.regex import last_key_pattern
from DAS.utils.logger import PrintManager
from DAS.core.das_parsercache import DASParserDB
from DAS.core.das_parsercache import PARSERCACHE_VALID, PARSERCACHE_INVALID
#from DAS.utils.spawn_manager import spawn

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

def ply_parse_query(query, keys, services, pdir='/tmp', verbose=False):
    """Get ply object for given query."""
    dasply = DASPLY(pdir, keys, services, verbose=verbose)
    dasply.build()
#    ply_query = dasply.parser.parse(query)
#    ply_query = spawn(dasply.parser.parse, query)
#    return ply_query
    error = None
    for trial in xrange(1, 3):
        try:
            ply_query = dasply.parser.parse(query)
            return ply_query
        except Exception as exc:
            msg = "Fail to parse query=%s, trial=%s, exception=%s" \
                    % (query, trial, str(exc))
            print(dastimestamp('DAS WARNING ') + ' ' + msg)
            error = exc
        time.sleep(trial/10.)
    raise error


def ply_output(query, keys, services, pdir='/tmp', verbose=False):
    """Print PLY/lexer output"""
    if  verbose:
        dasply = DASPLY(pdir, keys, services, verbose=verbose)
        dasply.build()
        print("input query='%s'" % query)
        dasply.test_lexer(query)

class QLManager(object):
    """
    DAS QL manager.
    """
    def __init__(self, config=None):
        if  not config:
            config = das_readconfig()
        self.dasmapping  = DASMapping(config)
        if  not self.dasmapping.check_maps():
            msg = "No DAS maps found in MappingDB"
            raise Exception(msg)
        self.dasservices = config['services']
        self.daskeysmap  = self.dasmapping.daskeys()
        self.operators   = list(das_operators())
        self.daskeys     = list(das_special_keys())
        self.verbose     = config['verbose']
        self.logger      = PrintManager('QLManger', self.verbose)
        for val in self.daskeysmap.values():
            for item in val:
                self.daskeys.append(item)
        parserdir   = config['das']['parserdir']
        self.parserdir = parserdir

        self.enabledb = config['parserdb']['enable']
        if  self.enabledb:
            self.parserdb = DASParserDB(config)

    def parse(self, query):
        """
        Parse input query and return query in MongoDB form.
        """
        mongo_query = self.mongo_query(query)
        self.convert2skeys(mongo_query)
        return mongo_query

    def get_ply_query(self, query):
        """
        Get ply object for given query. Since we rely on PLY package and it may
        fail under the load we use couple of trials.
        """
        ply_query = ply_parse_query(query, self.daskeys, self.dasservices,
                    self.parserdir, self.verbose)
        return ply_query

    def mongo_query(self, query):
        """
        Return mongo query for provided input query
        """
        mongo_query = None
        if  self.verbose:
            ply_output(query, self.daskeys, self.dasservices,
                    self.parserdir, self.verbose)
        parse_again = True
        if  self.enabledb:
            status, value = self.parserdb.lookup_query(query)
            if status == PARSERCACHE_VALID and \
                len(last_key_pattern.findall(query)) == 0:
                mongo_query = value
                parse_again = False
            elif status == PARSERCACHE_INVALID:
                # we unable to find query in parserdb, so will parse again
                parse_again = True
            else:
                ply_query = self.get_ply_query(query)
                if  ply_query:
                    try:
                        mongo_query = ply2mongo(ply_query)
                        parse_again = False
                    except Exception as exc:
                        msg = "Fail in ply2mongo, query=%s, ply_query=%s" \
                                % (query, ply_query)
                        print(msg)
                    try:
                        self.parserdb.insert_valid_query(query, mongo_query)
                    except Exception as exc:
                        msg = "Fail to insert into parserdb, exception=%s" \
                                % str(exc)
                        print_exc(msg, print_traceback=True)
        if  parse_again:
            try:
                ply_query   = self.get_ply_query(query)
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
                for system in self.dasmapping.list_systems():
                    mapkey = self.dasmapping.find_mapkey(system, key)
                    if  mapkey:
                        mongo_query['spec'][mapkey] = '*'
            return
        spec = mongo_query['spec']
        to_replace = []
        for key, val in spec.iteritems():
            for system in self.dasmapping.list_systems():
                mapkey = self.dasmapping.find_mapkey(system, key, val)
                if  mapkey and mapkey != key and \
                    key in mongo_query['spec']:
                    to_replace.append((key, mapkey))
                    continue
        for key, mapkey in to_replace:
            if  key in mongo_query['spec']:
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
                daskeys = self.dasmapping.find_daskey(service, key, value)
                if  set(keys) & set(daskeys) and service not in slist:
                    slist.append(service)
        # look-up special key condition
        requested_system = query.get('system', None)
        if  requested_system:
            if  isinstance(requested_system, basestring):
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
            alist = self.dasmapping.list_apis(srv)
            for api in alist:
                daskeys = self.dasmapping.api_info(srv, api)['das_map']
                maps = [r['rec_key'] for r in daskeys]
                if  set(mapkeys) & set(maps) == set(mapkeys): 
                    if  srv in adict:
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
    if  key in QL_MANAGER_OBJECTS:
        obj = QL_MANAGER_OBJECTS[key]
    else:
        obj = QLManager(config)
        QL_MANAGER_OBJECTS[key] = obj
    return obj
