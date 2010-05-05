#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Core class for Data Aggregation Service (DAS) framework.
It performs the following tasks:

- registers data-services found in DAS configuration file (das.cfg).
- invoke data-service APIs
- merge results based on common keys
- pass results to presentation layer (CLI or WEB)
"""

__revision__ = "$Id: das_core.py,v 1.72 2010/04/14 17:37:53 valya Exp $"
__version__ = "$Revision: 1.72 $"
__author__ = "Valentin Kuznetsov"

# system modules
import re
import os
import time
import types
import traceback

# DAS modules
from DAS.core.das_ql import das_operators
from DAS.core.das_parser import QLManager
from DAS.core.das_mapping_db import DASMapping
from DAS.core.das_analytics_db import DASAnalytics
from DAS.core.das_mongocache import DASMongocache, loose, convert2pattern
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.utils.utils import genkey, getarg

# DAS imports
import DAS.utils.jsonwrapper as json

class DASTimer(object):
    """
    DAS timer class keeps track of execution time.
    """
    def __init__(self):
        self.timer = {'init':[time.time()]}
    def record(self, tag):
        """Record time for given tag"""
        if  self.timer.has_key(tag):
            time0 = self.timer[tag]
            self.timer[tag] = time0 + [time.time()]
        else:
            self.timer[tag] = [time.time()]

class DASCore(object):
    """
    DAS core class.
    """
    def __init__(self, config=None, debug=None, nores=False, logger=None):
        if  config:
            dasconfig = config
        else:
            dasconfig = das_readconfig()
        verbose       = dasconfig['verbose']
        self.stdout   = debug
        if  type(debug) is types.IntType:
            self.verbose = debug
            dasconfig['verbose'] = debug
        else:
            self.verbose = verbose
        if  self.verbose:
            self.timer = DASTimer()
        self.operators = das_operators()

        # set noresults option
        self.noresults = False
        if  nores:
            dasconfig['write_cache'] = True
            self.noresults = nores

        if  not logger:
            logdir = dasconfig['logdir']
            self.logger = DASLogger(idir=logdir, verbose=self.verbose)
        else:
            self.logger = logger
        dasconfig['logger'] = self.logger

        # define Mapping/Analytics/Parser in this order since Parser depends
        # on first two
        dasmapping = DASMapping(dasconfig)
        dasconfig['dasmapping'] = dasmapping
        self.mapping = dasmapping

        self.analytics = DASAnalytics(dasconfig)
        dasconfig['dasanalytics'] = self.analytics

        self.mongoparser = QLManager(dasconfig)
        dasconfig['mongoparser'] = self.mongoparser

        dasroot = os.environ['DAS_ROOT']

        # init DAS cache
        self.rawcache = DASMongocache(dasconfig)
        dasconfig['rawcache'] = self.rawcache

        # plug-in architecture: loop over registered data-services in
        # dasconfig; load appropriate module/class; register data
        # service with DASCore.
        self.systems = dasmapping.list_systems()
        for name in self.systems:
            try:
                klass  = 'src/python/DAS/services/%s/%s_service.py'\
                    % (name, name)
                srvfile = os.path.join(dasroot, klass)
                with file(srvfile) as srvclass:
                    for line in srvclass:
                        if  line.find('(DASAbstractService)') != -1:
                            klass = line.split('(DASAbstractService)')[0]
                            klass = klass.split('class ')[-1] 
                            break
                mname  = 'DAS.services.%s.%s_service' % (name, name)
                module = __import__(mname, fromlist=[klass])
                obj = getattr(module, klass)(dasconfig)
                setattr(self, name, obj)
            except IOError:
                try:
                    mname  = 'DAS.services.generic_service'
                    module = __import__(mname, fromlist=['GenericService'])
                    obj    = module.GenericService(name, dasconfig)
                    setattr(self, name, obj)
                except:
                    traceback.print_exc()
                    msg = "Unable to load %s data-service plugin" % name
                    raise Exception(msg)
            except:
                traceback.print_exc()
                msg = "Unable to load %s data-service plugin" % name
                raise Exception(msg)

        # loop over systems and get system keys, add mapping keys to final list
        self.service_keys = {}
        self.service_parameters = {}
        for name in self.systems: 
            skeys = getattr(self, name).keys()
            self.service_keys[getattr(self, name).name] = skeys
            sparams = getattr(self, name).parameters()
            self.service_parameters[getattr(self, name).name] = sparams

        if  self.verbose:
            self.timer.record('DASCore.__init__')
        self.dasconfig = dasconfig

    def keys(self):
        """
        Return map of data service keys
        """
        return self.service_keys

    def das_keys(self):
        """
        Return map of data service keys
        """
        _keys = []
        for values in self.service_keys.values():
            for key in values:
                if  key not in _keys:
                    _keys.append(key)
        return _keys

    def adjust_query(self, query, add_to_analytics=True):
        """Check that provided query is indeed in MongoDB format"""
        err = '\nDASCore::result unable to load the input query=%s' % query
        if  type(query) is types.StringType: # DAS-QL
            try:
                query = json.loads(query)
            except:
                try:
                    query = self.mongoparser.parse(query, 
                                add_to_analytics)
                except:
                    traceback.print_exc()
                    raise Exception(err)
        err = '\nDASCore::result query not in MongoDB format, %s' % query
        if  type(query) is not types.DictType:
            raise Exception(err)
        else:
            if  not query.has_key('fields') and not query.has_key('spec'):
                raise Exception(err)
        return query

    def result(self, query, idx=0, limit=None, skey=None, sorder='asc'):
        """
        Get results either from cache or from explicit call
        """
        results = []
        query   = self.adjust_query(query)
        # check if we have any service which cover the query
        # otherwise decompose it into list of queries
        service_map = self.mongoparser.service_apis_map(query)
        if  not service_map:
            msg  = 'DASCore::result there is no single API to answer'
            msg += 'input query, will decompose it ...'
            self.logger.info(msg)
            skeys = query['fields']
            if  len(skeys) == 1: # no way we can proceed
                return results
            for key in skeys:
                newquery = dict(fields=[key], spec=query['spec'])
                self.call(newquery) # process query
        else:
            self.call(query) # process query

        # lookup provided query in a cache
        if  not self.noresults:
            results = self.get_from_cache(query, idx, limit, skey, sorder)
        return results

    def remove_from_cache(self, query):
        """
        Delete in cache entries about input query
        """
        self.rawcache.remove_from_cache(query)

    def get_status(self, query):
        """
        Look-up status of provided query in a cache.
        """
        status = 0
        record = self.rawcache.das_record(query)
        if  record:
            status = record['das']['status']
        return status

    def in_raw_cache(self, query):
        """
        Return true/false for input query if it exists in raw-cache.
        """
        query, dquery = convert2pattern(loose(query))
        return self.rawcache.incache(query, collection='merge')

    def in_raw_cache_nresults(self, query):
        """
        Return total number of results (count) for progived query.
        """
        query, dquery = convert2pattern(loose(query))
        return self.rawcache.nresults(query, collection='merge')

    def call(self, query, add_to_analytics=True):
        """
        Top level DAS api which execute a given query using underlying
        data-services. It follows the following steps:

            - parse input query
            - identify data-sercices based on selection keys
              and where clause conditions
            - construct DAS workflow and execute data-service 
              API calls. At this step individual 
              data-services store results into DAS cache.

        Return status 0/1 depending on success of the calls, can be
        used by workers on cache server.
        """
        if  self.rawcache.similar_queries(query):
            if  self.in_raw_cache(query):
                return 1

        query = self.adjust_query(query, add_to_analytics)
        msg = 'DASCore::call, query=%s' % query
        self.logger.info(msg)
        params = self.mongoparser.params(query)
        services = params['services']
        self.logger.info('DASCore::call, services = %s' % services)
        qhash = genkey(query)
        try:
            for srv in services:
                self.logger.info('DASCore::call %s(%s)' % (srv, query))
                if  self.verbose:
                    self.timer.record(srv)
                getattr(getattr(self, srv), 'call')(query)
                if  self.verbose:
                    self.timer.record(srv)
        except:
            traceback.print_exc()
            return 0
        self.rawcache.update_das_record(query, 'merging')
        if  self.verbose:
            self.timer.record('merge')
        self.rawcache.merge_records(query)
        if  self.verbose:
            self.timer.record('merge')
        self.rawcache.update_das_record(query, 'ok')
        return 1

    def get_from_cache(self, query, idx=0, limit=None, skey=None, sorder='asc'):
        """
        Look-up results from the merge cache and yield them for
        further processing.
        """
        msg = 'DASCore::get_from_cache, query=%s, idx=%s, limit=%s, skey=%s, order=%s'\
                % (query, idx, limit, skey, sorder)
        self.logger.info(msg)
        spec      = query.get('spec', {})
        fields    = query.get('fields', None)
        if  fields:
            prim_keys = []
            for key in fields:
                for srv in self.systems:
                    prim_key = self.mapping.find_mapkey(srv, key)
                    if  prim_key and prim_key not in prim_keys:
                        prim_keys.append(prim_key)
            if  prim_keys:
                query['spec'].update({"das.primary_key": {"$in":prim_keys}})
        mapreduce = query.get('mapreduce', None)
        filters   = query.get('filters', None)
        if  filters:
            query['fields'] = filters
        if  mapreduce:
            res = self.rawcache.map_reduce(mapreduce, spec)
        else:
            res = self.rawcache.get_from_cache(\
                query, idx, limit, skey, sorder, collection='merge')
        for row in res:
            yield row
