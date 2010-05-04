#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Define core class for Data Aggregation Service (DAS)
"""

__revision__ = "$Id: das_core.py,v 1.3 2009/04/13 19:01:56 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

import time
import types
from DAS.core.qlparser import dasqlparser
from DAS.utils.utils import cartesian_product, gen2list
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.services.dbs.dbs_service import DBSService
from DAS.services.sitedb.sitedb_service import SiteDBService
#from DAS.services.runsum.runsum_service import RunSummaryService
from DAS.services.phedex.phedex_service import PhedexService
from DAS.services.monitor.monitor_service import MonitorService
from DAS.services.lumidb.lumidb_service import LumiDBService

class DASCore(object):
    """
    DAS core class:
    service_keys = {'service':{list of keys]}
    service_maps = {('service1', 'service2'):'key'}
    """
    def __init__(self, mode=None, debug=None):
        dasconfig    = das_readconfig()
        self.mode    = mode # used to distinguish html vs cli 
        dasconfig['mode'] = mode
        verbose = dasconfig['verbose']
        self.stdout  = debug
        if  type(debug) is types.IntType:
            self.verbose = debug
            dasconfig['verbose'] = debug
            for system in dasconfig['systems']:
                sysdict = dasconfig[system]
                sysdict['verbose'] = debug
        else:
            self.verbose = verbose

        self.logger  = DASLogger(verbose=self.verbose, stdout=debug)
        dasconfig['logger'] = self.logger

        self.cache_servers  = dasconfig['cache_servers']
        self.cache_lifetime = dasconfig['cache_lifetime']
        self.couch_servers  = dasconfig['couch_servers']
        self.couch_lifetime = dasconfig['couch_lifetime']
        # I can step forward and pass class names into config as well
        # this will allow to inialize data-service on a fly
        # but it will require to load their modules first
        # for example
        # for name in dasconfig['systems']:
        #     from service.name import ServiceClass
        #     getattr(self, name) = ServiceClass(dasconfig)
        self.dbs     = DBSService(dasconfig)
#        self.runsum  = RunSummaryService(dasconfig)
        self.sitedb  = SiteDBService(dasconfig)
        self.phedex  = PhedexService(dasconfig)
        self.monitor = MonitorService(dasconfig)
        self.lumidb  = LumiDBService(dasconfig)

        self.service_maps = dasconfig['mapping']
        self.service_keys = {}
        for name in dasconfig['systems']: 
            self.service_keys[getattr(self, name).name] = \
                getattr(self, name).keys()

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

    def queryanalyzer(self, qldict):
        """
        Analyze set of queries against data services. Consturct new query based
        on data-service conditions. Example, the following query
        query find dataset, block, admin where site=T2 and run=123
        produces a dictionary (by qlparser) which now can be analyzed
        to combine q0 and q1 into single condition set.
        qldict {'condlist': {'q1': 'run=123', 'q0': 'site=T2'}, 
                'input': 'find dataset, block, admin where site=T2 and run=123', 
                'queries': {'q1': 'find dataset,block,admin where run=123', 
                            'q0': 'find dataset,block,admin where site=T2'}, 
                'sellist': ['dataset', 'block', 'admin'], 
                'query': 'find dataset, block, admin where q0 and q1'}
        """
        query = qldict['query']
        queries = qldict['queries']

        # make copies
        qldict['qlqueries'] = dict(queries)
        qldict['qlquery'] = str(query)

        condlist = query.split(' where ')
        if  len(condlist) == 1:
            return qldict
        qkeys   = queries.keys()
        qkeys.sort()
        newcond = []
        idx = 0
        while 1:
            curr_query = queries[qkeys[idx]]
            try:
                next_query = queries[qkeys[idx+1]]
            except IndexError:
                break
            set1 = set(self.findservices(curr_query))
            set2 = set(self.findservices(next_query))
            systems = set1 & set2
            if  systems:
                if  newcond:
                    last = newcond[-1][0]
                    if  last == systems:
                        items = [systems, newcond[-1][1:], qkeys[idx+1]]
                        newcond[-1] = items
                else:
                    newcond.append([systems, qkeys[idx], qkeys[idx+1]]) 
            else:
                newcond.append([systems, qkeys[idx+1]])
            idx += 1
        counter = 0
        for item in newcond:
            first_key = item[1]
            last_key  = item[-1]
            substr = query[query.find(first_key) :\
                           query.rfind(last_key)+len(last_key)]
            newcond = ""
            for elem in substr.split():
                if  elem == 'and' or elem == 'or':
                    newcond += ' ' + elem
                else:
                    cond = queries[elem].split(' where ')[-1]
                    newcond += ' ' + cond
            newkey = "cond%s" % counter
            qldict['condlist'][newkey] = newcond.strip()
            query = query.replace(substr, newkey)
            queries[newkey] = query.split(' where ')[0] + ' where %s' % \
                                newcond.strip()
            for key in item[1:]:
                del queries[key]
            counter += 1
        qldict['query'] = query
        return qldict

    def findservices(self, query):
        """
        for provided query find out which service to use. Algorithm based on
        1. if where clause is not found, use list of selected keywords and
           find out their mapping to services
        2. if where clause is found, use it to find out which service to use
        In both cases DBS takes priority over other services if selected key
        or where clause key is mapped to DBS and service in question.
        """
        slist = []
        if  query.find(' where') == -1:
            query = query.replace('find ', '').replace('plot ', '')
            sellist = query.split(',')
            for service, keys in self.service_keys.items():
                for key in sellist:
                    if  keys.count(key) and not slist.count(service):
                        slist.append(service)
        else:
            # TODO: I need to cover all cases for cond_key, so far I only 
            # use = operator, what about like, etc.
            cond = query.split(' where ')[-1]
            cond_key = cond.split()[0].split("=")[0]
            slist += [s for s in self.findmappedservices(cond_key) \
                                if not slist.count(s)]
        return slist

    def findmappedservices(self, cond_key):
        """
        Generator function which finds mapped services for provided key.
        DBS takes priority over other services.
        """
        for service, keylist in self.service_keys.items():
            for key in keylist:
                if  key == cond_key:
                    yield service

    def result(self, query):
        """
        Wrap returning results into returning list
        """
        results    = self.call(query)
        resultlist = []
        idx        = 1
        for res in results:
            item = dict(res)
            item['id'] = idx
            if  not resultlist.count(item):
                resultlist.append(item)
            idx += 1
        return resultlist

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

    def call(self, query):
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
        qldict   = self.queryanalyzer(dasqlparser(query))
        self.logger.info('DASCore::call(%s)' % query)
        self.logger.debug('DASCore::call, qldict = %s' % str(qldict))
        sellist  = qldict['sellist']

        # find list of services we need to query based on selection keys
        # of the query
        services = []
        for key in sellist:
            services += [s for s in self.findmappedservices(key) \
                                    if not services.count(s)]

        # loop over all sub-queries and update a list of involved services
        qdict   = {}
        for key, query in qldict['queries'].items():
            slist = self.findservices(query)
            qdict[query] = slist
            services += [s for s in slist if not services.count(s)]

        # always gives DBS priority to look-up data
        if  services.count('dbs'):
            services.remove('dbs')
            services = ['dbs'] + services

        # walk through services and look-up if a single service cover
        # all selection keys, if so we do not need other services.
        for srv in services:
            set1 = set(sellist)
            set2 = set(getattr(self, srv).keys())
            if  (set1 & set2) == set1:
                services = [srv]
                break
            
        self.logger.info('DASCore::call, complete list of services %s' \
                        % services)

        # IMPORTANT
        # TODO: I need ANTRL parser to return me a list of keys
        # list of conditions, then I need to combine those to make
        # unique list of keys to retreive
        ###########

        # form unique set to retrieve from all services, based on 
        # selection keys of the query and inter-service relationships
        ulist   = list(sellist)
        
        for service in services:
            if  service != self.dbs.name:
                keys = self.relation_keys(self.dbs.name, service)
                for key in keys:
                    if  not ulist.count(key):
                        ulist.append(key)
        self.logger.info('DASCore::call, unique set of keys %s' % ulist)

        # I need one more loop to discard services which doesn't cover
        # output selection list, e.g.
        # find dataset, admin where site=T2_UK
        # finds dbs, sitedb, phedex based on their relation keys
        # but phedex doesn't need to be used since not selection keys
        # from this service.
        discard_services = []
        for service in services:
            if  not set(self.service_keys[service]) & set(sellist):
                discard_services.append(service)
        for srv in discard_services:
            services.remove(srv)

        # call data-services to execute sub-queries
        # TODO: I need to cover the case when 
        # find dataset,admin where site=T2
        # should result to the same query pass to both services
        rdict = {}
        for service in services:
            # find if we already run one service whose results
            # can be used in current one
            cond_dict = self.find_cond_dict(service, rdict)
            if  qdict:
                for query, slist in qdict.items():
                    if  slist.count(service):
                        res = getattr(getattr(self, service), 'call')\
                                     (query, ulist, cond_dict)
                    else:
                        qqq = "find " + ','.join(sellist)
                        res = getattr(getattr(self, service), 'call')\
                                     (qqq, ulist, cond_dict)
                    rdict[service] = res
            else:
                qqq = "find " + ','.join(sellist)
                res = getattr(getattr(self, service), 'call')\
                                (qqq, ulist, cond_dict)
                rdict[service] = res

        # if result dict contains only single result set just return it
        systems = rdict.keys()
        if  len(systems) == 1:
            return rdict[systems[0]]
        # find pairs who has relationships, e.g. (dbs, phedex),
        # and make cartesian product out of them based on found relation keys
        reldict = {}
        result = ""
        for sys0 in systems:
            for sys1 in systems:
                rel_keys = self.relation_keys(sys0, sys1)
                if  sys1 != sys0 and rel_keys:
                    reldict[(sys0, sys1)] = rel_keys
                    set0    = rdict[sys0]
                    set1    = rdict[sys1]
                    if  not result:
                        result  = cartesian_product(set0, set1, rel_keys)
                    else:
                        result  = cartesian_product(result, set1, rel_keys)
        finalset = gen2list(result)
        return finalset

#        set0    = rdict[systems[0]]
#        set1    = rdict[systems[1]]
#        result  = cartesian_product(set0, set1)
#        if  len(systems) > 2:
#            for rest in systems[2:]: 
#                result = cartesian_product(result, rdict[rest])
#        finalset = gen2list(result)
#        return finalset
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

    def mapping(self, service):
        """retrieve mapping for given service, return dict[key]=service_api"""

    def relation_keys(self, service1, service2):
        """return a relation key(s) between two services"""
        key = (service1, service2)
        if  self.service_maps.has_key(key):
            keys = self.service_maps[(service1, service2)]
            msg  = 'DASCore::key(%s, %s) found relation keys %s' \
                    % (service1, service2, keys)
            self.logger.info(msg)
            return keys
        return []

    def create_view(self, iset):
        """create a view for provided set"""
