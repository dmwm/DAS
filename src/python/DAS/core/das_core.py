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

__revision__ = "$Id: das_core.py,v 1.16 2009/05/28 18:59:10 valya Exp $"
__version__ = "$Revision: 1.16 $"
__author__ = "Valentin Kuznetsov"

import re
import os
import time
import types
import traceback

from DAS.core.qlparser import QLParser
from DAS.core.das_viewmanager import DASViewManager

from DAS.utils.utils import cartesian_product
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger

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

        self.viewmgr = DASViewManager()

        dasroot = os.environ['DAS_ROOT']
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

        self.service_maps = dasconfig['mapping']
        self.service_keys = {}
        # loop over systems and get system keys,
        # add mapping keys to final list
        for name in dasconfig['systems']: 
            skeys = getattr(self, name).keys()
            for key, val in self.service_maps.items():
                if  list(key).count(name):
                    skeys += [s for s in val if not skeys.count(s)]
            self.service_keys[getattr(self, name).name] = skeys
        self.qlparser = QLParser(self.service_keys)
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

    def create_view(self, name, query):
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

    def result(self, query, idx=0, limit=None):
        """
        Get results either from cache or from explicit call
        """
        if  hasattr(self, 'cache'):
            if  self.cache.incache(query):
                results = self.cache.get_from_cache(query, idx, limit)
            else:
                # TODO: I can put threads here to update_cache in
                # background and only allow to retrieve results 
                # from cache
                #
                # NOTE: the self.call returns generator, update_cache
                # consume and iterate over its items. So if I need to
                # re-use it, the update_cache will yeild them back
                results = self.call(query) 
                results = self.cache.update_cache(query, results, expire=600)
        else:
            results = self.call(query)
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
                results = self.call(query) 
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

    def json(self, query):
        """
        Wrap returning results into DAS header and return JSON dict.
        """
        # TODO: replace request_url, version with real values
        init_time   = time.time()

        rdict = {}
        rdict['request_timestamp'] = str(init_time)
        rdict['request_url'] = ''
        rdict['request_version'] = __version__
        rdict['request_expires'] = ''
        rdict['request_call'] = ''
        rdict['call_time'] = ''
        rdict['request_query'] = query

        results  = self.result(query)
        end_time = time.time()

        rdict['call_time'] = str(end_time-init_time)
        rdict['results'] = results
        return rdict

    def call(self, uinput):
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
        """
        query = self.viewanalyzer(uinput)
        self.logger.info("DASCore::call, user input '%s'" % uinput)
        self.logger.info("DASCore::call, DAS query '%s'" % query)

        params   = self.qlparser.params(query)
        sellist  = params['selkeys']
        ulist    = params['unique_keys']
        services = params['unique_services']
        daslist  = params['daslist']
        self.logger.info('DASCore::call, unique set of keys %s' % ulist)
        self.logger.info('DASCore::call, unique set of services %s' % services)
        # main loop, it goes over query dict in daslist. The daslist
        # contains subqueries (in a form of dict={system:subquery}) to
        # be executed by underlying systems. The number of entreis in daslist
        # is determined by number of logical OR operators which separates
        # conditions applied to data-services. For example, if we have
        # find run where dataset=/a/b/c or hlt=ok we will have 2 entries:
        # - list of runs from DBS
        # - list of runs from run-summary
        # while if we have
        # find run where dataset=/a/b/c/ and hlt=ok we end-up with 1 entry
        # find all runs in DBS and make cartesian product with those found
        # in run-summary.
        for qdict in daslist:
            self.logger.info('DASCore::call, qdict = %s' % str(qdict))
            rdict = {}
            for service in services:
                if  self.verbose:
                    self.timer.record(service)
                # find if we already run one service whose results
                # can be used in current one
                cond_dict = self.find_cond_dict(service, rdict)
                if  qdict.has_key(service):
                    squery = qdict[service]
                    res = getattr(getattr(self, service), 'call')\
                                 (squery, ulist, cond_dict)
                    rdict[service] = res
                else:
                    qqq = "find " + ','.join(sellist)
                    res = getattr(getattr(self, service), 'call')\
                                    (qqq, ulist, cond_dict)
                    rdict[service] = res
                if  self.verbose:
                    self.timer.record(service)
            # if result dict contains only single result set just return it
            systems = rdict.keys()
            if  len(systems) == 1:
                for entry in rdict[systems[0]]:
                    yield entry
                return

            # find pairs who has relationships, e.g. (dbs, phedex),
            # and make cartesian product out of them based on found relation keys
            list0 = rdict[systems[0]]
            list1 = rdict[systems[1]]
            idx  = 2
            while 1:
                product = cartesian_product(list0, list1)
                if  idx >= len(systems):
                    break
#                list0 = [i for i in product] # may be I should do: list0 = product
                list0 = product
                list1 = rdict[systems[idx]]
                idx += 1
            for entry in product:
                yield entry

    def find_cond_dict(self, service, rdict):
        """
        For given service find if it contains in provided result dict
        a key. If so, return a dictionary with values for those found
        keys.
        """
        cond_dict = {}
        if  rdict:
            for key in rdict.keys():
                map1 = (key, service)
                map2 = (service, key)
                for mmm in [map1, map2]:
                    if  self.service_maps.has_key(mmm):
                        skey = self.service_maps[mmm]
                        for skey in self.service_maps[mmm]:
                            prev = list(set(\
                                   [item[skey] for item in rdict[key]\
                                        if item.has_key(skey)]))
                            if  prev:
                                cond_dict[skey] = prev
        return cond_dict

