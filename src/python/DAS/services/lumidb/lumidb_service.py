#!/usr/bin/env python
"""
LumiDB service
"""
__revision__ = "$Id: lumidb_service.py,v 1.1 2009/04/07 20:10:09 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import re
import types

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator

class LumiDBService(DASAbstractService):
    def __init__(self, config):
        DASAbstractService.__init__(self, 'lumidb', config)
        self.map = {
            'findTrgPathByRun' : {
                'keys': ['trigpath', 'run'],
                'params' : {'run_number':''}
            },
            'findIntegratedLuminosity' : {
                'keys': ['intlumi', 'run'],
                'params' : {'run_number':'', 'tag':'', 'hlt_path':''}
            },
            'findAvgIntegratedLuminosity' : {
                'keys': ['avglumi', 'run'],
                'params' : {'run_number':'', 'tag':'', 'hlt_path':''}
            },
            'findIntRawLumi' : {
                'keys': ['intrawlumi', 'run'],
                'params' : {'run_number':''}
            },
            'findL1Counts' : {
                'keys': ['L1counts', 'run'],
                'params' : {'run_number':'', 'cond_name':''}
            },
            'findHLTCounts' : {
                'keys': ['HLTcounts', 'run'],
                'params' : {'run_number':'', 'path_name':'', 'count_type':''}
            },
            'findRawLumi' : {
                'keys': ['rawlumi', 'run'],
                'params' : {'run_number':'', 'tag':''}
            },
            'listLumiByBunch' : {
                'keys': ['lumibybunch', 'run'],
                'params' : {'run_number':'', 'lumi_section_number':'', 'option':''}
            },
            'listLumiSummary' : {
                'keys': ['lumisummary', 'run'],
                'params' : {'run_number':'', 'lumi_section_number':'', 'version':''}
            },
            'listLumiTrigger' : {
                'keys': ['lumitrigger', 'run'],
                'params' : {'run_number':'', 'lumi_section_number':''}
            },
            'listLumiDeadTime' : {
                'keys': ['lumideadtime', 'run'],
                'params' : {'run_number':'', 'lumi_section_number':''}
            },
        }
        map_validator(self.map)

    def api(self, query, cond_dict=None):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        Return: [{'key':'value', 'key2':'value2'}, ...]
        """
        print "CALL lumidb::api"
        print query
        print cond_dict
        condlist = query.split(' where ')
        cond = query.split(' where ')[-1]
        cond_key, cond_val = cond.split()[0].split("=")
        params = {'api':'findIntRawLumi', 'run_number' : int(cond_val)}
        params = {'api':'findTrgPathByRun', 'run_number' : int(cond_val)}
        print params
        # construct params out of provided query and cond_dict
        data  = self.getdata(self.url, params)
        print data
        import sys
        sys.exit(0)
        # parse output data here
        return data

