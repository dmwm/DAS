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

__revision__ = "$Id: das_core.py,v 1.80 2010/05/04 21:13:39 valya Exp $"
__version__ = "$Revision: 1.80 $"
__author__ = "Valentin Kuznetsov"

# system modules
import re
import os
import time
import traceback

# DAS modules
from DAS.core.das_ql import das_operators
from DAS.core.das_parser import QLManager
from DAS.core.das_mapping_db import DASMapping
from DAS.core.das_analytics_db import DASAnalytics
from DAS.core.das_mongocache import DASMongocache, loose, convert2pattern
from DAS.core.das_aggregators import das_func
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.utils.utils import genkey, getarg, unique_filter
from DAS.utils.das_timer import das_timer, get_das_timer

# DAS imports
import DAS.utils.jsonwrapper as json

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
        if  isinstance(debug, int):
            self.verbose = debug
            dasconfig['verbose'] = debug
        else:
            self.verbose = verbose
        das_timer('DASCore::init', self.verbose)
        self.operators = das_operators()

        # set noresults option
        self.noresults = False
        if  nores:
            dasconfig['write_cache'] = True
            self.noresults = nores

        logfile = dasconfig.get('logfile', None)
        logformat = dasconfig.get('logformat')
        if  debug:
            logfile = None # I want stdout printouts in debug mode
            logformat = '%(message)s'
        if  not logger:
            self.logger = DASLogger(logfile=logfile, verbose=self.verbose,
                name='DAS', format=logformat)
        else:
            self.logger = logger
        dasconfig['logfile'] = logfile
        dasconfig['logformat'] = logformat
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

        # init DAS cache
        self.rawcache = DASMongocache(dasconfig)
        dasconfig['rawcache'] = self.rawcache

        # plug-in architecture: loop over registered data-services in
        # dasconfig; load appropriate module/class; register data
        # service with DASCore.
        self.systems = dasmapping.list_systems()
        if  not os.environ.has_key('DAS_PYTHONPATH'):
            msg = 'DAS_PYTHONPATH environment variable is not set'
            raise Exception(msg)
        dasroot = os.environ['DAS_PYTHONPATH']
        for name in self.systems:
            try:
                klass  = 'DAS/services/%s/%s_service.py' \
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

        self.dasconfig = dasconfig
        das_timer('DASCore::init', self.verbose)

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
        err = '\nDASCore::result unable to load the input query: "%s"' % query
        if  isinstance(query, str): # DAS-QL
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
        if  not isinstance(query, dict):
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
            if  not skeys:
                skeys = []
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

    def bare_query(self, iquery):
        """
        Remove from provided query filters/mapreduce, etc. and leave
        only bare query.
        """
        query = dict(iquery)
        for key in query.keys():
            if  key not in ['spec', 'fields']:
                del query[key]
        msg = 'DASCore::bare_query, input query=%s, output query =%s' \
                % (iquery, query)
        self.logger.debug(msg)
        return query

    def get_status(self, query):
        """
        Look-up status of provided query in a cache.
        """
        iquery = dict(query)
        query  = self.bare_query(query)
        if  self.rawcache.similar_queries(query) and self.in_raw_cache(query):
            return 'ok' # self.call set status ok for processed queries
        status = 0
#        record = self.rawcache.das_record(query)
        record = self.rawcache.find_spec(query)
        if  record:
            status = record['das']['status']
        return status

    def in_raw_cache(self, query):
        """
        Return true/false for input query if it exists in raw-cache.
        """
        query, dquery = convert2pattern(loose(query))
        return self.rawcache.incache(query, collection='merge')

    def in_raw_cache_nresults(self, query, coll='merge'):
        """
        Return total number of results (count) for progived query.
        """
        query, dquery = convert2pattern(loose(query))
        return self.rawcache.nresults(query, collection=coll)

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
        das_timer('DASCore::call', self.verbose)
        # adjust query first, since rawcache.similar_queries
        # expects a mongo query (this could be a string)
        # this also guarantees the query in question hits
        # analytics
        query = self.adjust_query(query, add_to_analytics)
        
        if  self.rawcache.similar_queries(query):
            if  self.in_raw_cache(query):
                das_timer('DASCore::call', self.verbose)
                return 1

        
        msg = 'DASCore::call, query=%s' % query
        self.logger.info(msg)
        params = self.mongoparser.params(query)
        services = params['services']
        self.logger.info('DASCore::call, services = %s' % services)
        qhash = genkey(query)
        try:
            for srv in services:
                self.logger.info('DASCore::call %s(%s)' % (srv, query))
                das_timer(srv, self.verbose)
                getattr(getattr(self, srv), 'call')(query)
                das_timer(srv, self.verbose)
        except:
            traceback.print_exc()
            return 0
        self.rawcache.update_das_record(query, 'merging')
        das_timer('merge', self.verbose)
        self.rawcache.merge_records(query)
        das_timer('merge', self.verbose)
        self.rawcache.update_das_record(query, 'ok')
        self.rawcache.add_to_record(query, {'das.timer': get_das_timer()})
        das_timer('DASCore::call', self.verbose)
        return 1

    def get_from_cache(self, query, idx=0, limit=None, skey=None, sorder='asc'):
        """
        Look-up results from the merge cache and yield them for
        further processing.
        """
        das_timer('DASCore::get_from_cache', self.verbose)
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
        aggregators = query.get('aggregators', None)
        mapreduce   = query.get('mapreduce', None)
        filters     = query.get('filters', None)
        unique      = False
        if  filters:
            fields  = query['fields']
            if  not fields or not isinstance(fields, list):
                fields = []
            for filter in filters:
                if  filter == 'unique':
                    unique = True
                    continue
                if  filter.find('=') != -1:
                    key, val = filter.split('=')
                    query['spec'][key.strip()] = val.strip()
                else:
                    if  filter not in fields:
                        fields.append(filter)
            if  fields:
                query['fields'] = fields

#            query['fields'] = filters
        if  mapreduce:
            res = self.rawcache.map_reduce(mapreduce, spec)
        elif aggregators:
            res = []
            _id = 0
            for func, key in aggregators:
                rows = self.rawcache.get_from_cache(query, collection='merge')
                if  func == 'avg':
                    nres = self.rawcache.nresults(query, collection='merge')
                    if  not nres:
                        data = 'N/A'
                    else:
                        data = float(das_func('sum', key, rows))/nres
                else:
                    data = das_func(func, key, rows)
                res += [{'_id':_id, 'function': func, 'key': key, 'result': data}]
                _id += 1
        else:
            res = self.rawcache.get_from_cache(\
                query, idx, limit, skey, sorder, collection='merge')
        # check if we have unique filter
        if  unique:
            for row in unique_filter(res):
                yield row
        else:
            for row in res:
                yield row
        das_timer('DASCore::get_from_cache', self.verbose)
