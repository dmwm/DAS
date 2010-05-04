#!/usr/bin/env python
"""
LumiDB service
"""
__revision__ = "$Id: lumidb_service.py,v 1.4 2009/04/23 01:11:31 valya Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "Valentin Kuznetsov"

import re
import types

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, query_params

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
                'params' : {'run_number':'', 'lumi_section_number':'', 'option':''}
            },
            'listLumiSummary' : {
                'api' : {'api':'listLumiSummary'},
                'keys': ['lumisummary', 'run'],
                'params' : {'run_number':'', 'lumi_section_number':'', 'version':'current'}
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

    def api_v1(self, query, cond_dict=None):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        Return: [{'key':'value', 'key2':'value2'}, ...]
        """
        selkeys, cond = query_params(query)
        params = {}
        for key in cond.keys():
            oper, val = cond[key]
            if  oper == '=':
                params[key] = val
            else:
                raise Exception("DAS::%s, not supported operator '%s'" \
                % (self.name, oper))
        if  params.has_key('run'): # LumiDB uses run_number
            val = params['run']
            params['run_number'] = val
            del params['run']
        for key in cond_dict: # LumiDB uses run_number rather then run key
            if  key == 'run':
                name = 'run_number'
            else:
                name = key
            params[name] = cond_dict[key]

        apiname = ""
        args = {}
        # check if all requested keys are covered by one API
        for api, aparams in self.map.items():
            if  set(selkeys) & set(aparams['keys']) == set(selkeys):
                apiname = api
                for par in aparams['params']:
                    if  params.has_key(par):
                        args[par] = params[par]
                args['api'] = api
                res = self.getdata(self.url, args)
                jsondict = eval(res)
                if  jsondict.has_key('error'):
                    continue
                print "### lumidb:jsondict", jsondict
                data = getattr(self, 'parser_%s' % apiname)(jsondict)
                return data

        # if one API doesn't cover sel keys, will call multiple APIs
        apidict = {}
        for key in selkeys:
            for api, aparams in self.map.items():
                if  aparams['keys'].count(key) and not apidict.has_key(api):
                    args = {}
                    for par in aparams['params']:
                        if  params.has_key(par):
                            args[par] = params[par]
                    apidict[api] = args
        rel_keys = []
        resdict  = {}
        for api, args in apidict.items():
            args['api'] = api
            res = self.getdata(self.url, args)
            jsondict = eval(res)
            data = getattr(self, 'parser_%s' % api)(jsondict)
            resdict[api] = data
            first_row = data[0]
            keys = first_row.keys()
            if  not rel_keys:
                rel_keys = set(list(keys))
            else:
                rel_keys = rel_keys & set(keys)
        data = self.product(resdict, rel_keys)
        return data


    def parser_findTrgPathByRun(self, jsondict):
        """
        Parser of output for findTrgPathByRun lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'trigpath':val}
            data.append(row)
        return data

    def parser_findIntegratedLuminosity(self, jsondict):
        """
        Parser of output for findIntegratedLuminosity lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'intlumi':val}
            data.append(row)
        return data

    def parser_findAvgIntegratedLuminosity(self, jsondict):
        """
        Parser of output for findAvgIntegratedLuminosity lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'avglumi':val}
            data.append(row)
        return data

    def parser_findIntRawLumi(self, jsondict):
        """
        Parser of output for findIntRawLumi lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'intrawlumi':val}
            data.append(row)
        return data

    def parser_findL1Counts(self, jsondict):
        """
        Parser of output for findL1Counts lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'L1counts':val}
            data.append(row)
        return data

    def parser_findHLTCounts(self, jsondict):
        """
        Parser of output for findHLTCounts lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'HLTcounts':val}
            data.append(row)
        return data

    def parser_findRawLumi(self, jsondict):
        """
        Parser of output for findRawLumi lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'rawlumi':val}
            data.append(row)
        return data

    def parser_listLumiByBunch(self, jsondict):
        """
        Parser of output for listLumiByBunch lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'lumibybunch':val}
            data.append(row)
        return data

    def parser_listLumiSummary(self, jsondict):
        """
        Parser of output for listLumiSummary lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'lumisummary':val}
            data.append(row)
        return data

    def parser_listLumiTrigger(self, jsondict):
        """
        Parser of output for listLumiTrigger lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'lumitrigger':val}
            data.append(row)
        return data

    def parser_listLumiDeadTime(self, jsondict):
        """
        Parser of output for listLumiDeadTime lumidb API
        """
        data = []
        for key, val in jsondict.items():
            row = {'lumideadtime':val}
            data.append(row)
        return data

