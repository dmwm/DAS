#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Abstract interface for DAS service
"""
__revision__ = "$Id: abstract_service.py,v 1.2 2009/03/12 20:54:07 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

import types
import urllib
import urllib2
from DAS.utils.utils import query_params, transform_dict2list
from DAS.utils.utils import genresults, results2couch
from DAS.utils.utils import cartesian_product, genkey
from DAS.core.das_couchdb import DASCouchDB
from DAS.core.basemanager import BaseManager

class DASAbstractService(object):
    """
    Abstract class describing DAS service. It initialized with a name who
    is used to identify service parameters from DAS configuration file.
    Those parameters are keys, verbosity level, URL of the data-service.
    """
    def __init__(self, name, config):
        self.name         = name
        sdict             = config[name]
        self.verbose      = int(sdict['verbose'])
        self.expire       = int(sdict['expire'])
        self.url          = sdict['url']
        self.mode         = config['mode']
        self.logger       = config['logger']
        self.map          = {} # to be defined by data-serice implementation
        # define internal couch DB manager to put 'raw' results into CouchDB
        mgr               = BaseManager(config)
        self._couch       = DASCouchDB(mgr)

    def keys(self):
        """
        Return service keys
        """
        srv_keys = []
        for api, params in self.map.items():
            for key in params['keys']:
                if  not key in srv_keys:
                    srv_keys.append(key)
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
        return

    def product(self, resdict, rel_keys=None):
        """
        Make cartesian product for all entries in provided result dict.
        """
        data = []
        keys = resdict.keys()
        if  len(keys) == 1:
            for rows in resdict.values():
                data += rows
        else: # need to make cartesian product of results
            set0 = resdict[keys[0]]
            set1 = resdict[keys[1]]
            result = cartesian_product(set0, set1, rel_keys)
            if  len(keys) > 2:
                for rest in keys[2:]: 
                    result = cartesian_product(result, resdict[rest], rel_keys)
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
