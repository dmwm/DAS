#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS service
"""
__revision__ = "$Id: dbs_service.py,v 1.6 2009/05/28 18:59:11 valya Exp $"
__version__ = "$Revision: 1.6 $"
__author__ = "Valentin Kuznetsov"

import types
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
        self._keys   = None
        # DBS uses DBSServlet and API passed as parameter, so we don't
        # define api in map and rather use url w/ DBSServlet
        self.map     = {
            '' : {
                'keys' : self.dbs_keys(),
                'params' : self.params
            }
        }
        map_validator(self.map)

    def dbs_keys(self):
        """
        Fetch DBS-QL keys and store them in memcache.
        """
        if  self._keys:
            return self._keys
        url    = self.url
        params = dict(self.params)
        params['api'] = 'getHelp'
        res = self.getdata(url, params)
        if  type(res) is types.GeneratorType:
            res = [i for i in res][0]
        data    = parser_dbshelp(res)
        return data

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
        return data

