#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Core class for Data Aggregation Service (DAS) framework.
It performs the following tasks:

- registers data-services found in DAS configuration file (das.cfg).
- invoke data-service subqueries and either multiplex results or
  combine them together for presentation layer (CLI or WEB).
- creates DAS views
"""

from __future__ import with_statement

__revision__ = "$Id: das_core.py,v 1.54 2010/01/25 20:23:03 valya Exp $"
__version__ = "$Revision: 1.54 $"
__author__ = "Valentin Kuznetsov"

import re
import os
import time
import types
import traceback
import DAS.utils.jsonwrapper as json

from DAS.core.qlparser import MongoParser
#from DAS.core.das_viewmanager import DASViewManager
from DAS.core.das_mapping_db import DASMapping
from DAS.core.das_analytics_db import DASAnalytics
from DAS.core.das_mongocache import loose, convert2pattern
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.utils.utils import genkey, getarg

import DAS.core.das_functions as das_functions

class DASTimer(object):
    """
    DAS timer class, keeps track of execution time
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
    def __init__(self, config=None, debug=None, nores=False):
        if  config:
            dasconfig = config
        else:
            dasconfig = das_readconfig()
        verbose       = dasconfig['verbose']
        self.stdout   = debug
        if  type(debug) is types.IntType:
            self.verbose = debug
            dasconfig['verbose'] = debug
            for system in dasconfig['systems']:
                sysdict = dasconfig[system]
                sysdict['verbose'] = debug
        else:
            self.verbose = verbose
        if  self.verbose:
            self.timer = DASTimer()

        # set noresults option
        if  nores:
            dasconfig['write_cache'] = True
            self.noresults = nores

        logdir = dasconfig['logdir']
        self.logger = DASLogger(idir=logdir, verbose=self.verbose, stdout=debug)
        dasconfig['logger'] = self.logger

        # define Mapping/Analytics/Parser in this order since Parser depends
        # on first two
        dasmapping = DASMapping(dasconfig)
        dasconfig['dasmapping'] = dasmapping
        self.mapping = dasmapping

        self.analytics = DASAnalytics(dasconfig)
        dasconfig['dasanalytics'] = self.analytics

        self.mongoparser = MongoParser(dasconfig)
        dasconfig['mongoparser'] = self.mongoparser

#        self.viewmgr = DASViewManager(dasconfig)

        dasroot = os.environ['DAS_ROOT']
        self.rawcache = None
        # load from configuration what will be used as a raw/cold cache
        if  dasconfig.has_key('rawcache') and dasconfig['rawcache']:
            klass   = dasconfig['rawcache']
            name    = klass.lower().replace('das', 'das_')
            stm     = "from DAS.core.%s import %s\n" % (name, klass)
            obj     = compile(str(stm), '<string>', 'exec')
            eval(obj) # load class def
            klassobj = '%s(dasconfig)' % klass
            setattr(self, 'rawcache', eval(klassobj))
            dasconfig['rawcache'] = self.rawcache
            self.logger.info('DASCore::__init__ rawcache=%s' % klass)
        else:
            msg = 'DAS configuration file does not provide rawcache'
            raise Exception(msg)

        # load from configuration what will be used as a hot cache
#        if  dasconfig.has_key('hotcache') and dasconfig['hotcache']:
#            klass   = dasconfig['hotcache']
#            name    = klass.lower().replace('das', 'das_')
#            stm     = "from DAS.core.%s import %s\n" % (name, klass)
#            obj     = compile(str(stm), '<string>', 'exec')
#            eval(obj) # load class def
#            klassobj = '%s(dasconfig)' % klass
#            setattr(self, 'hotcache', eval(klassobj))
#            self.cache   = self.hotcache
#            self.logger.info('DASCore::__init__ hotcache=%s' % klass)

        # plug-in architecture: loop over registered data-services in
        # dasconfig; load appropriate module/class; register data
        # service with DASCore.
        for name in dasconfig['systems']:
            try:
                klass   = 'src/python/DAS/services/%s/%s_service.py'\
                    % (name, name)
                srvfile = os.path.join(dasroot, klass)
                with file(srvfile) as srvclass:
                    for line in srvclass:
                        if  line.find('(DASAbstractService)') != -1:
                            klass = line.split('(DASAbstractService)')[0]
                            klass = klass.split('class ')[-1] 
                            break
                stm = "from DAS.services.%s.%s_service import %s\n" \
                    % (name, name, klass)
                obj = compile(str(stm), '<string>', 'exec')
                eval(obj) # load class def
                klassobj = '%s(dasconfig)' % klass
                setattr(self, name, eval(klassobj))
            except:
                traceback.print_exc()
                msg = "Unable to load %s plugin (%s_service.py)" \
                % (name, name)
                raise Exception(msg)

        self.service_keys = {}
        self.service_parameters = {}
        # loop over systems and get system keys,
        # add mapping keys to final list
        for name in dasconfig['systems']: 
            skeys = getattr(self, name).keys()
            self.service_keys[getattr(self, name).name] = skeys
            sparams = getattr(self, name).parameters()
            self.service_parameters[getattr(self, name).name] = sparams

        # find out names of function from agg module
        self.das_functions = \
        [item for item in das_functions.__dict__.keys() if item.find('__') == -1]

        self.das_aggregation = {} # determine at run-time
        if  self.verbose:
            self.timer.record('DASCore.__init__')
        self.dasconfig = dasconfig

    def keys(self):
        """
        return map of data service keys
        """
        return self.service_keys

    def das_keys(self):
        """
        return map of data service keys
        """
        _keys = []
        for values in self.service_keys.values():
            for key in values:
                _keys.append(key)
        return _keys

    def plot(self, query):
        """plot data for requested query"""
        results = self.result(query)
        for item in results:
            print item
        return

#    def get_view(self, name=None):
#        """return DAS view"""
#        if  name:
#            return self.viewmgr.get(name)
#        return self.viewmgr.all()

#    def create_view(self, name, query, 
#                        login='nobody', fullname='N/A', group='users'):
#        """create DAS view"""
#        return self.viewmgr.create(name, query)

#    def update_view(self, name, query):
#        """update DAS view"""
#        return self.viewmgr.update(name, query)

#    def delete_view(self, name):
#        """delete DAS view"""
#        return self.viewmgr.delete(name)

#    def viewanalyzer(self, input):
#        """
#        Simple parser input and look-up if it's view or DAS query
#        """
#        pat = re.compile('^view')
#        if  pat.match(input):
#            qlist = input.replace('view ', '').strip().split()
#            name  = qlist[0]
#            cond  = ''
#            if  len(qlist) > 1:
#                cond = ' '.join(qlist[1:])
#            query = self.viewmgr.get(name) + ' ' + cond
#        else:
#            query = input
#        return query

#    def aggregation(self, results):
#        """
#        Perform aggregation of information if DAS functions
#        is found.
#        """
#        results  = [i for i in results]
#        agg_dict = {}
#        for func, arg in self.das_aggregation.items():
#            agg  = getattr(das_functions, func)(arg, results)
#            for key, val in agg.items():
#                agg_dict[key] = val
#        first = results[0]
#        try:
#            del first['system'] # don't account as selection key
#        except:
#            pass
#        if  len(first.keys()) != len(agg_dict.keys()):
#            for row in results:
#                for key, val in agg_dict.items():
#                    row[key] = val
#                yield row
#        else:
#            yield agg_dict

    def adjust_query(self, query):
        # check that provided query is indeed in MongoDB format.
        err = '\nDASCore::result unable to load the input query=%s' % query
        if  type(query) is types.StringType: # DAS-QL
            try:
                query = json.loads(query)
            except:
                try:
                    query = self.mongoparser.requestquery(query)
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
        query = self.adjust_query(query)
        # lookup provided query in a cache
        self.call(query) 
        if  not self.noresults:
            results = self.get_from_cache(query, idx, limit, skey, sorder)
#        if  self.das_aggregation:
#            results = self.aggregation(results)
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
        status = 'no data'
        record = self.rawcache.das_record(query)
        if  record:
            status = record['das']['status']
        return status

    def in_raw_cache(self, query):
        """
        Look-up input query if it exists in raw-cache.
        """
        query, dquery = convert2pattern(loose(query))
        return self.rawcache.incache(query, collection='merge')

    def in_raw_cache_nresults(self, query):
        """
        Look-up how manu records for given query exists in raw-cache.
        """
        query, dquery = convert2pattern(loose(query))
        return self.rawcache.nresults(query, collection='merge')

    def call(self, query):
        """
        Top level DAS api which execute a given query using underlying
        data-services. It follows the following steps:

        1. identify data-sercices in questions, based on selection keys
           and where clause conditions by parsing input query
        2. construct workflow and execute data-service calls with found
           sub-queries. At this step individual data-services invoke
           store results into DAS cache.

        Return status 0/1 depending on success of the calls, can be
        used by workers on cache server.
        """
        if  self.in_raw_cache(query):
            return 1

        query = self.adjust_query(query)
        msg = 'DASCore::call, query=%s' % query
        self.logger.info(msg)
        params = self.mongoparser.params(query)
        services = params['services'].keys()
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
        spec   = getarg(query, 'spec', {})
        fields = getarg(query, 'fields', None)
        mapreduce = getarg(query, 'mapreduce', None)
        if  not fields:
            fields = None
        if  mapreduce:
            res = self.rawcache.map_reduce(mapreduce, spec)
        else:
            query = dict(spec=spec, fields=fields)
            res = self.rawcache.get_from_cache(\
                loose(query), idx, limit, skey, sorder, collection='merge')
        for row in res:
            yield row
