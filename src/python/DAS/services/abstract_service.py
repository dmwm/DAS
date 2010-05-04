#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Abstract interface for DAS service
"""
__revision__ = "$Id: abstract_service.py,v 1.11 2009/05/15 14:19:59 valya Exp $"
__version__ = "$Revision: 1.11 $"
__author__ = "Valentin Kuznetsov"

import types
import urllib
import urllib2
import traceback
from DAS.utils.utils import genresults
from DAS.utils.utils import cartesian_product
from DAS.core.das_couchdb import DASCouchDB
from DAS.core.basemanager import BaseManager
from DAS.core.das_mapping import jsonparser, das2api, das2result
from DAS.core.qlparser import QLLexer

class DASAbstractService(object):
    """
    Abstract class describing DAS service. It initialized with a name who
    is used to identify service parameters from DAS configuration file.
    Those parameters are keys, verbosity level, URL of the data-service.
    """
    def __init__(self, name, config):
        self.name         = name
        try:
            sdict         = config[name]
            self.verbose  = int(sdict['verbose'])
            self.expire   = int(sdict['expire'])
            self.url      = sdict['url']
            self.mode     = config['mode']
            self.logger   = config['logger']
        except:
            traceback.print_exc()
            print config
            raise Exception('fail to parse DAS config')

        self.map          = {} # to be defined by data-service implementation
        self.qllexer      = None # to be defined at run-time in self.worker
        self._keys        = None # to be defined at run-time in self.keys

        # define internal couch DB manager to put 'raw' results into CouchDB
        mgr               = BaseManager(config)
        self._couch       = DASCouchDB(mgr)

    def keys(self):
        """
        Return service keys
        """
        if  self._keys:
            return self._keys
        srv_keys = []
        for api, params in self.map.items():
            for key in params['keys']:
                if  not key in srv_keys:
                    srv_keys.append(key)
        self._keys = srv_keys
        return srv_keys

    def getdata(self, url, params):
        """
        Invoke URL call and retrieve data from data-service.
        User provides a set of parameters passed to data-service URL.
        """
        msg = 'DAS::%s getdata(%s, %s)' % (self.name, url, params)
        self.logger.info(msg)
        # call couch cache to get results from it,
        # otherwise call data service as shown below.
        cquery = "%s %s" % (url, params)
        res = self._couch.get_from_cache(cquery)
        if  res:
            return res

        data = urllib2.urlopen(url, urllib.urlencode(params, doseq=True))
        results = data.read()
        # to prevent unicode/ascii errors like
        # UnicodeDecodeError: 'utf8' codec can't decode byte 0xbf in position
        if  type(results) is types.StringType:
            results = unicode(results, errors='ignore')

        self.logger.debug('DAS::%s results=%s' % (self.name, results))

        # store to couch 'raw' data coming out of concrete data service
        # will add 'query' and 'timestamp' for every row in results
        self._couch.update_cache(cquery, results, self.expire)

        return results

    def api(self, query, cond_dict=None):
        """
        Data service api method, can be defined by data-service class.
        return a list of results with selected keys.
        """
        self.logger.info('DAS::%s api(%s)' % (self.name, query))
        msg = 'DAS::%s api uses cond_dict\n%s' % (self.name, str(cond_dict))
        self.logger.debug(msg)
        results = self.worker(query, cond_dict)
        return results

    def product(self, resdict):
        """
        Make cartesian product for all entries in provided result dict.
        """
        data = []
        if  not resdict:
            return data
        keys = resdict.keys()
        if  len(keys) == 1:
            for rows in resdict.values():
                data += rows
        else: # need to make cartesian product of results
            set0 = resdict[keys[0]]
            set1 = resdict[keys[1]]
            result = cartesian_product(set0, set1)
            if  len(keys) > 2:
                for rest in keys[2:]: 
                    result = cartesian_product(result, resdict[rest])
            data = result
        return data

    def call(self, query, collect_list, cond_dict=None):
        """
        Invoke service API to execute given query.
        Return results as a collect list set.
        """
        msg = 'DAS::%s call(%s, %s)' % (self.name, query, collect_list)
        self.logger.info(msg)
        # call couch cache to get results from it,
        # otherwise call data service as shown below.
        cquery = '%s @ %s' % (query, self.name)
        res = self._couch.get_from_cache(cquery)
        if  res:
            return res

        skeys = [key for key in collect_list if self.keys().count(key)]
        # add exception for DBS for aggregated functions
        if  self.name == 'dbs':
            for key in collect_list:
                for agg in ['count', 'sum']:
                    if  key.find('%s(' % agg) != -1:
                        kkk = key.replace('%s(' % agg, '').replace(')', '')
                        if  self.keys().count(kkk):
                            skeys += [key]

        split = query.split(' where ')
        if  len(split) == 1:
            cond = ""
        else:
            cond = ' where ' + split[-1]
        service_query = "find " + ','.join(skeys) + cond
        # ask data-service api to get result set in form [row1, row2, ...]
        res = self.api(service_query, cond_dict)
        results = genresults(self.name, res, collect_list)
        # store to couch 'raw' data coming out of concrete data service
        # will add 'query' and 'timestamp' for every row in results
        self._couch.update_cache(cquery, results, self.expire)

        return results

    def adjust(self, apidict):
        """
        There are some special cases (based on DBS QL legacy) when we have
        one to many sel.key mapping to API. For example, the "site" can be
        used in SiteDB as cms name or as se name. To fix that we use
        data-service specific adjust call to convert such sel. key value
        from one format to another
        """
        return

    def adjust_result(self, api, jsondict):
        """
        Some data-services return a single result as a dict with
        non-meaningful keys, e.g. sitedb returns {0:{row}, 1:{row}}
        Instead we expect results returned with lists:
        {'key':[{row}, {row}]}
        Use this adjustment call (implemented in data-service code)
        to fix this problem
        """
        return jsondict

    def worker(self, query, icond_dict=None):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        msg = 'DASAbstractService::worker(%s, %s)' % (query, icond_dict)
        self.logger.info(msg)

        if  not self.qllexer:
            self.qllexer = QLLexer({self.name:self.keys()})

        selkeys = self.qllexer.selkeys(query)
        conditions = self.qllexer.conditions(query)
        msg = 'DASAbstractService::worker, selkeys %s' % selkeys
        self.logger.debug(msg)
        msg = 'DASAbstractService::worker, conditions %s' % conditions
        self.logger.debug(msg)

        # loop over conditions and create input params dict which will
        # be passed to data-service, convert DAS condition keys into
        # ones accepted by data-service
        params = {}
        for item in conditions:
            if  type(item) is types.DictType: # cond dict
                oper = item['op']
                val  = item['value']
                key  = item['key']
            else:
                continue # TODO: what should I do if I find brackets?
            keylist = das2api(self.name, key)
            if  type(keylist) is not types.ListType:
                keylist = [keylist]
            for newkey in keylist:
                params[newkey] = val
        # loop over input condition dict which can be provided by
        # another data-service who found values for some keys used here
        for key in icond_dict:
            keylist = das2api(self.name, key)
            if  type(keylist) is not types.ListType:
                keylist = [keylist]
            for newkey in keylist:
                params[newkey] = icond_dict[key]

        # translate selection keys into ones data-service APIs provides
        keylist = [das2result(self.name, key) for key in selkeys]
        keylist = []
        for key in selkeys:
            res = das2result(self.name, key)
            for item in das2result(self.name, key):
                keylist.append(item)

        apiname = ""
#        args = {}
        # check if all requested keys are covered by one API
        for api, aparams in self.map.items():
            if  set(selkeys) & set(aparams['keys']) == set(selkeys):
                apiname = api
                args = aparams['params']
                for par in aparams['params']:
                    if  params.has_key(par):
                        args[par] = params[par]
                if  aparams.has_key('api'):
                    apidict = aparams['api']
                    apikey  = apidict.keys()[0]
                    apival  = apidict[apikey]
                    args[apikey] = apival 
                    url = self.url # JAVA, e.g. http://host/Servlet
                else: # if we have http://host/apiname?...
                    url = self.url + '/' + apiname
                res = self.getdata(url, args)
                res = res.replace('null', '\"null\"')
                jsondict = eval(res)
#                jsondict = self.adjust_result(api, jsondict)
                if  self.verbose > 2:
                    print "\n### %s::%s returns" % (self.name, api)
                    print jsondict
                if  jsondict.has_key('error'):
                    continue
                data = jsonparser(self.name, jsondict, keylist)
                return data

        # if one API doesn't cover sel keys, will call multiple APIs
        if  self.verbose > 2:
            print "\n#### call multiple APIs"
        apidict = {}
        for key in selkeys:
            for api, aparams in self.map.items():
                if  aparams['keys'].count(key) and not apidict.has_key(api):
#                    args = {}
                    args = aparams['params']
                    for par in aparams['params']:
                        if  params.has_key(par):
                            args[par] = params[par]
                    apidict[api] = args
        self.adjust(apidict)
        resdict  = {}
        for api, args in apidict.items():
            url = self.url + '/' + api
            res = self.getdata(url, args)
            res = res.replace('null', '\"null\"')
            jsondict = eval(res)
#            jsondict = self.adjust_result(api, jsondict)
            if  self.verbose > 2:
                print "\n### %s::%s returns" % (self.name, api)
                print jsondict, keylist
            data = jsonparser(self.name, jsondict, keylist)
            resdict[api] = data
            first_row = data[0]
            keys = first_row.keys()
        data = self.product(resdict)
        return data
