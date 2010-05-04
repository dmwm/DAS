#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS service
"""
__revision__ = "$Id: dbs_service.py,v 1.5 2009/05/27 20:28:04 valya Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "Valentin Kuznetsov"

import types
import memcache
from DAS.services.abstract_service import DASAbstractService
from DAS.services.dbs.dbs_parser import parser, parser_dbshelp
from DAS.utils.utils import genkey, map_validator

class DBSService(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dbs', config)
        self.results = []
        self.params  = {'apiversion': 'DBS_2_0_6', 'api': 'executeQuery'}
        cachelist    = config['cache_servers'].split(',')
        self.cache   = memcache.Client(cachelist, debug=self.verbose)
        # DBS uses DBSServlet and API passed as parameter, so we don't
        # define api in map and rather use url w/ DBSServlet
        self.map     = {
            '' : {
                'keys' : self.dbs_keys(),
                'params' : self.params
            }
        }
        map_validator(self.map)
        # apart from other service DBS provides API to retrive QL keys
        # overwrite parent class settings
#        self.service_keys = self.dbs_keys()

    def dbs_keys(self):
        """
        Fetch DBS-QL keys and store them in memcache.
        """
        url   = self.url
        params = dict(self.params)
        params['api'] = 'getHelp'

        key = genkey(url)
        res = self.cache.get(key)
        if  res:
            return res
        try:
            res = parser_dbshelp(self.getdata(url, params))
            self.cache.set(key, res, 24*60*60) # store results into memcache
        except:
            return []
        return res

    def api(self, query, cond_dict=None):
        """
        Invoke DBS API to execute given query.
        Return results as a list of dict, e.g.
        [{'run':1,'dataset':/a/b/c'}, ...]
        """
        url     = self.url
        params  = dict(self.params)
        params['query'] = query
        res     = self.getdata(url, params) 
        if  type(res) is types.GeneratorType:
            res = [i for i in res][0]
        data    = parser(res)
#        data = parser(self.getdata(url, params))
        return data

