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

__revision__ = "$Id: das_core.py,v 1.29 2009/09/09 18:40:36 valya Exp $"
__version__ = "$Revision: 1.29 $"
__author__ = "Valentin Kuznetsov"

import re
import os
import time
import types
import traceback

from DAS.core.qlparser import QLParser
from DAS.core.das_viewmanager import DASViewManager
from DAS.core.das_mapping import DASMapping
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger

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
    DAS core class:
    service_keys = {'service':{list of keys]}
    service_maps = {('service1', 'service2'):'key'}
    """
    def __init__(self, debug=None):
        dasconfig    = das_readconfig()
        verbose      = dasconfig['verbose']
        self.stdout  = debug
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

        logdir = dasconfig['logdir']
        self.logger = DASLogger(idir=logdir, verbose=self.verbose, stdout=debug)
        dasconfig['logger'] = self.logger

        dasmapping = DASMapping(dasconfig)
        dasconfig['dasmapping'] = dasmapping

        self.viewmgr = DASViewManager(dasconfig)

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
            self.logger.info('DASCore::__init__, rawcache=%s' % klass)
        else:
            msg = 'DAS configuration file does not provide rawcache'
            raise Exception(msg)

        # load from configuration what will be used as a hot cache
        if  dasconfig.has_key('hotcache') and dasconfig['hotcache']:
            klass   = dasconfig['hotcache']
            name    = klass.lower().replace('das', 'das_')
            stm     = "from DAS.core.%s import %s\n" % (name, klass)
            obj     = compile(str(stm), '<string>', 'exec')
            eval(obj) # load class def
            klassobj = '%s(dasconfig)' % klass
            setattr(self, 'hotcache', eval(klassobj))
            self.cache   = self.hotcache
            self.logger.info('DASCore::__init__, hotcache=%s' % klass)

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
        # load abstract service for all data-services
#        conf4all = dict(dasconfig)
#        conf4all['all'] = dict(url='', logger=self.logger, dasmapping=dasmapping, 
#                                verbose=self.verbose, expire=0)
#        self.all = DASAbstractService('all', conf4all)

        self.service_maps = dasconfig['mapping']
        self.service_keys = {}
        self.service_parameters = {}
        # loop over systems and get system keys,
        # add mapping keys to final list
        for name in dasconfig['systems']: 
            skeys = getattr(self, name).keys()
# This code is commented out on purpose, but I should keep it around
# The service_maps which is a dict of service:keys should keep only keys
# which provided by service (the output of service), while mapping
# between services should not be included (the code below). The mapping
# keys are used in multiplex step, but should not be used when we place
# service queries 
#            for key, val in self.service_maps.items():
#                if  name in list(key):
#                    skeys += [s for s in val if s not in skeys]
            self.service_keys[getattr(self, name).name] = skeys
            sparams = getattr(self, name).parameters()
            self.service_parameters[getattr(self, name).name] = sparams

        # find out names of function from agg module
        self.das_functions = \
        [item for item in das_functions.__dict__.keys() if item.find('__') == -1]

        # init QL parser
        srv_weights = dasconfig['srv_weights']
        self.qlparser = QLParser(self.service_keys, self.service_parameters,
                        self.das_functions, srv_weights)
        self.das_aggregation = {} # determine at run-time
        if  self.verbose:
            self.timer.record('DASCore.__init__')

    def keys(self):
        """
        return map of data service keys
        """
        return self.service_keys

    def plot(self, query):
        """plot data for requested query"""
        results = self.result(query)
        for item in results:
            print item
        return

    def get_view(self, name=None):
        """return DAS view"""
        if  name:
            return self.viewmgr.get(name)
        return self.viewmgr.all()

    def create_view(self, name, query, 
                        login='nobody', fullname='N/A', group='users'):
        """create DAS view"""
        return self.viewmgr.create(name, query)

    def update_view(self, name, query):
        """update DAS view"""
        return self.viewmgr.update(name, query)

    def delete_view(self, name):
        """delete DAS view"""
        return self.viewmgr.delete(name)

    def viewanalyzer(self, input):
        """
        Simple parser input and look-up if it's view or DAS query
        """
        pat = re.compile('^view')
        if  pat.match(input):
            qlist = input.replace('view ', '').strip().split()
            name  = qlist[0]
            cond  = ''
            if  len(qlist) > 1:
                cond = ' '.join(qlist[1:])
            query = self.viewmgr.get(name) + ' ' + cond
        else:
            query = input
        return query

    def aggregation(self, results):
        """
        Perform aggregation of information if DAS functions
        is found.
        """
#            print "will do aggregation", self.das_aggregation
        results  = [i for i in results]
        agg_dict = {}
        for func, arg in self.das_aggregation.items():
            agg  = getattr(das_functions, func)(arg, results)
            for key, val in agg.items():
                agg_dict[key] = val
        first = results[0]
        try:
            del first['system'] # don't account as selection key
        except:
            pass
        if  len(first.keys()) != len(agg_dict.keys()):
            for row in results:
                for key, val in agg_dict.items():
                    row[key] = val
                yield row
        else:
            yield agg_dict

    def result(self, query, idx=0, limit=None):
        """
        Get results either from cache or from explicit call
        """
        if  hasattr(self, 'cache'):
            if  self.cache.incache(query):
                results = self.cache.get_from_cache(query, idx, limit)
            else:
                # NOTE: the self.call returns generator, update_cache
                # consume and iterate over its items. So if I need to
                # re-use it, the update_cache will yeild them back
#                results = self.call(query) 
                results = self.call(self.get_params(query)) 
                if  self.das_aggregation:
                    results = self.aggregation(results)
                results = self.cache.update_cache(query, results, 
                                expire=self.cache.limit)
        else:
#            results = self.call(query)
            results = self.call(self.get_params(query)) 
            if  self.das_aggregation:
                results = self.aggregation(results)
        return results

    def update_cache(self, query, expire=600):
        """
        Update cache with result of the query
        """
        status = 0
        if  hasattr(self, 'cache'):
            try:
                status = self.cache.incache(query)
                if  status:
                    self.logger.info('found in DAS cache')
            except:
                traceback.print_exc()
                status = 0
                pass
            if  not status:
                # NOTE: the self.call returns generator, update_cache
                # consume and iterate over its items. So if I need to
                # re-use it, the update_cache will yeild them back
                self.logger.info('updating DAS cache')
#                results = self.call(query) 
                results = self.call(self.get_params(query)) 
                if  self.das_aggregation:
                    results = self.aggregation(results)
                try:
                    results = self.cache.update_cache(query, results, expire)
                    # loop over results since it's generator
                    for i in results:
                        pass
                    status = 1
                except:
                    status = 0
                    pass
        return status

    def remove_from_cache(self, query):
        """
        Delete in cache entries about input query
        """
        if  hasattr(self, 'cache'):
            if  self.cache.incache(query):
                self.cache.remove_from_cache(query)

    def get_params(self, uinput):
        """
        The purpose of this method is to parse input query and return
        a parameter dict produced by QLParser
        """
        query = self.viewanalyzer(uinput)
        self.logger.info("DASCore::get_query_params, user input '%s'" % uinput)
        self.logger.info("DASCore::get_query_params, DAS query '%s'" % query)
        params   = self.qlparser.params(query)
        self.logger.debug('DASCore::call, QLParser results:\n %s' % params)
        self.das_aggregation = params['functions']
        return params

    def call(self, params):
        """
        Top level DAS api which execute a given query using underlying
        data-services. It follows the following steps:
        Step 1. identify data-sercices in questions, based on selection keys
                and where clause conditions
        Step 2. construct worksflow and execute data-service calls with found
                sub-queries.
        Step 3. Collect results into service sets, multiplex them together
                using cartesian product, and return result set back to the user
        Return a list of generators containing results for further processing.

        Input is param dict returned by QLParser. We use a dict since call method
        is generator. So the QLParser results can be used elsewhere outside of it.
        """
        sellist  = params['selkeys']
        ulist    = params['unique_keys']
        services = params['unique_services']
        dasqueries  = params['dasqueries']
        query    = params['query']
        self.logger.info('DASCore::call, QL parser results\n%s' % params)
        # main loop, it goes over query dict in daslist. The daslist
        # contains subqueries (in a form of dict={system:subquery}) to
        # be executed by underlying systems. The number of entreis in daslist
        # is determined by number of logical OR operators which separates
        # conditions applied to data-services. For example, if we have
        # find run where dataset=/a/b/c or hlt=ok we will have 2 entries:
        # - list of runs from DBS
        # - list of runs from run-summary
        # while if we have
        # find run where dataset=/a/b/c/ and hlt=ok we may have 1 or more
        # data-services to be called, based on provided selection keys and
        # conditions.
        #
        if  not dasqueries.keys():
            msg = 'Unable to find data-services for given query'
            raise Exception(msg)
        for srv, queries in dasqueries.items():
            for squery in queries:
                self.logger.info('DASCore::call %s(%s)' % (srv, squery))
                if  self.verbose:
                    self.timer.record(srv)
#                res = getattr(getattr(self, srv), 'call')(squery)
                getattr(getattr(self, srv), 'call')(squery)
                if  self.verbose:
                    self.timer.record(srv)
#                for row in res:
#                    yield row
        # use last used service to get mongo query
        mongo_query = getattr(getattr(self, srv), 'mongo_query_parser')(query)
        del mongo_query['spec']['das.system'] # I don't to specify a system
        res = self.rawcache.get_from_cache(query=mongo_query)
        for row in res:
            yield row
