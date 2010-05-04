#!/usr/bin/env python
"""
LumiDB service
"""
__revision__ = "$Id: lumidb_service.py,v 1.6 2009/05/13 15:19:32 valya Exp $"
__version__ = "$Revision: 1.6 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator 

class LumiDBService(DASAbstractService):
    def __init__(self, config):
        DASAbstractService.__init__(self, 'lumidb', config)
        self.map = {
            'findTrgPathByRun' : {
                'api' : {'api':'findTrgPathByRun'},
                'keys': ['trigpath', 'run'],
                'params' : {'run_number':''}
            },
            'findIntegratedLuminosity' : {
                'api' : {'api':'findIntegratedLuminosity'},
                'keys': ['intlumi', 'run'],
                'params' : {'run_number':'', 'tag':'', 'hlt_path':''}
            },
            'findAvgIntegratedLuminosity' : {
                'api' : {'api':'findAvgIntegratedLuminosity'},
                'keys': ['avglumi', 'run'],
                'params' : {'run_number':'', 'tag':'', 'hlt_path':''}
            },
            'findIntRawLumi' : {
                'api' : {'api':'findIntRawLumi'},
                'keys': ['intrawlumi', 'run'],
                'params' : {'run_number':''}
            },
            'findL1Counts' : {
                'api' : {'api':'findL1Counts'},
                'keys': ['L1counts', 'run'],
                'params' : {'run_number':'', 'cond_name':''}
            },
            'findHLTCounts' : {
                'api' : {'api':'findHLTCounts'},
                'keys': ['HLTcounts', 'run'],
                'params' : {'run_number':'', 'path_name':'', 'count_type':''}
            },
            'findRawLumi' : {
                'api' : {'api':'findRawLumi'},
                'keys': ['rawlumi', 'run'],
                'params' : {'run_number':'', 'tag':''}
            },
            'listLumiByBunch' : {
                'api' : {'api':'listLumiByBunch'},
                'keys': ['lumibybunch', 'run'],
                'params' : {'run_number':'', 'lumi_section_number':'', 
                            'option':''}
            },
            'listLumiSummary' : {
                'api' : {'api':'listLumiSummary'},
                'keys': ['lumisummary', 'run'],
                'params' : {'run_number':'', 'lumi_section_number':'', 
                            'version':'current'}
            },
#            'listLumiTrigger' : {
#                'api' : {'api':'listLumiTrigger'},
#                'keys': ['lumitrigger', 'run'],
#                'params' : {'run_number':'', 'lumi_section_number':''}
#            },
#            'listLumiDeadTime' : {
#                'api' : {'api':'listLumiDeadTime'},
#                'keys': ['lumideadtime', 'run'],
#                'params' : {'run_number':'', 'lumi_section_number':''}
#            },
        }
        map_validator(self.map)
