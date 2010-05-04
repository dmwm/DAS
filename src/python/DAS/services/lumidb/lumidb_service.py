#!/usr/bin/env python
"""
LumiDB service
"""
__revision__ = "$Id: lumidb_service.py,v 1.7 2009/06/05 14:12:37 valya Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator 

class LumiDBService(DASAbstractService):
    def __init__(self, config):
        DASAbstractService.__init__(self, 'lumidb', config)
        self.map = {
            'findTrgPathByRun' : {
                'api' : {'api':'findTrgPathByRun'},
                'keys': ['trigpath'],
                'params' : {'run_number':''}
            },
            'findIntegratedLuminosity' : {
                'api' : {'api':'findIntegratedLuminosity'},
                'keys': ['intlumi'],
                'params' : {'run_number':'', 'tag':'', 'hlt_path':''}
            },
            'findAvgIntegratedLuminosity' : {
                'api' : {'api':'findAvgIntegratedLuminosity'},
                'keys': ['avglumi'],
                'params' : {'run_number':'', 'tag':'', 'hlt_path':''}
            },
            'findIntRawLumi' : {
                'api' : {'api':'findIntRawLumi'},
                'keys': ['intrawlumi'],
                'params' : {'run_number':''}
            },
            'findL1Counts' : {
                'api' : {'api':'findL1Counts'},
                'keys': ['L1counts'],
                'params' : {'run_number':'', 'cond_name':''}
            },
            'findHLTCounts' : {
                'api' : {'api':'findHLTCounts'},
                'keys': ['HLTcounts'],
                'params' : {'run_number':'', 'path_name':'', 'count_type':''}
            },
            'findRawLumi' : {
                'api' : {'api':'findRawLumi'},
                'keys': ['rawlumi'],
                'params' : {'run_number':'', 'tag':''}
            },
            'listLumiByBunch' : {
                'api' : {'api':'listLumiByBunch'},
                'keys': ['lumibybunch'],
                'params' : {'run_number':'', 'lumi_section_number':'', 
                            'option':''}
            },
            'listLumiSummary' : {
                'api' : {'api':'listLumiSummary'},
                'keys': ['lumisummary'],
                'params' : {'run_number':'', 'lumi_section_number':'', 
                            'version':'current'}
            },
#            'listLumiTrigger' : {
#                'api' : {'api':'listLumiTrigger'},
#                'keys': ['lumitrigger'],
#                'params' : {'run_number':'', 'lumi_section_number':''}
#            },
#            'listLumiDeadTime' : {
#                'api' : {'api':'listLumiDeadTime'},
#                'keys': ['lumideadtime'],
#                'params' : {'run_number':'', 'lumi_section_number':''}
#            },
        }
        map_validator(self.map)
