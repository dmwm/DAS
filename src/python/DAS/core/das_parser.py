#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS Query Language parser.
"""

__revision__ = "$Id: das_parser.py,v 1.7 2010/05/03 19:47:25 valya Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "Valentin Kuznetsov"

import re
import time
import urllib
from DAS.core.das_lexer import DASLexer
from DAS.core.das_ql import das_filters, das_aggregators, das_reserved
from DAS.core.das_ql import das_special_keys
from DAS.core.das_ql import das_operators, MONGO_MAP, URL_MAP
from DAS.core.das_ply import DASPLY, ply2mongo
from DAS.utils.utils import adjust_value, convert2date, das_dateformat
from DAS.utils.regex import key_attrib_pattern, last_key_pattern
from DAS.core.das_parsercache import DASParserDB, PARSERCACHE_NOTFOUND
from DAS.core.das_parsercache import PARSERCACHE_VALID, PARSERCACHE_INVALID
from DAS.utils.regex import int_number_pattern, float_number_pattern
import DAS.utils.jsonwrapper as json

def decompose(query):
    """Extract selection keys and conditions from input query"""
    skeys = query.get('fields', [])
    cond  = query.get('spec', {})
    return skeys, cond

class QLManager(object):
    """
    DAS QL manager.
    """
    def __init__(self, config):
        if  not config['dasmapping']:
            msg = "No mapping found in provided config=%s" % config
            raise Exception(msg)
        if  not config['dasanalytics']:
            msg = "No analytics found in provided config=%s" % config
            raise Exception(msg)
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
        self.logger      = config['logger']
        for val in self.daskeysmap.values():
            for item in val:
                self.daskeys.append(item)
        parserdir   = config['das']['parserdir']
        self.dasply = DASPLY(parserdir, self.daskeys, self.dasservices, 
                verbose=self.verbose)

        self.enabledb = config['parserdb']['enable']
        if  self.enabledb:
            self.parserdb = DASParserDB(config)

    def parse(self, query, add_to_analytics=True):
        """
        Parse input query and return query in MongoDB form.
        Optionally parsed query can be written into analytics DB.
        """
        if  query and isinstance(query, str) and \
            query[0] == "{" and query[-1] == "}":
            mongo_query = json.loads(query)
            if  mongo_query.keys() != ['fields', 'spec']:
                raise Exception('Invalid MongoDB query %s' % query)
            if  not mongo_query['fields'] and \
                len(mongo_query['spec'].keys()) > 1:
                msg = 'Ambiguous query "%s", please provide selection key' \
                        % query
                raise Exception(msg)
            if  add_to_analytics:
                self.analytics.add_query(query, mongo_query)
            return mongo_query
        try:
            mongo_query = self.mongo_query(query)
        except:
            print "\nUnable to convert input query='%s' into MongoDB one\n" \
                % query
            raise
        self.convert2skeys(mongo_query)
        if  add_to_analytics:
            self.analytics.add_query(query, mongo_query)
        return mongo_query

    def mongo_query(self, query):
        """
        Return mongo query for provided input query
        """
        # NOTE: somehow I need to keep build call just before using
        # PLY parser, otherwise it fails to parse.
        self.dasply.build()
        if  self.verbose:
            msg = "QLManager::mongo_query, input query='%s'" % query
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
                    self.parserdb.insert_valid_query(query, mongo_query)
                except Exception, exp:
                    self.parserdb.insert_invalid_query(query, exp)
                    raise exp
        else:
            try:
                ply_query   = self.dasply.parser.parse(query)
                mongo_query = ply2mongo(ply_query)
            except Exception, exp:
                raise exp
        if  set(mongo_query.keys()) & set(['fields','spec']) != \
                set(['fields', 'spec']):
            raise Exception('Invalid MongoDB query %s' % mongo_query)
        if  not mongo_query['fields'] and len(mongo_query['spec'].keys()) > 1:
            msg = 'Ambiguous query "%s", please provide selection key' % query
            raise Exception(msg)
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
        for key, val in spec.items():
            for system in self.map.list_systems():
                mapkey = self.map.find_mapkey(system, key, val)
                if  mapkey and mapkey != key and \
                    mongo_query['spec'].has_key(key):
                    mongo_query['spec'][mapkey] = mongo_query['spec'][key]
                    del mongo_query['spec'][key]
                    continue
        
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
            for service, keys in self.daskeysmap.items():
                if  service not in self.dasservices:
                    continue
                value = cond.get(key, None)
                daskeys = self.map.find_daskey(service, key, value)
                if  set(keys) & set(daskeys) and service not in slist:
                    slist.append(service)
        # look-up special key condition
        if  cond.has_key('system'):
            requested_system = cond['system']
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
                daskeys = self.map.api_info(api)['daskeys']
                maps = [r['map'] for r in daskeys]
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

