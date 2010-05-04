#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Dashboard service
"""
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import time
import types
import urllib
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator
from DAS.services.dashboard.dashboard_parser import parser
from DAS.core.das_mapping import jsonparser4key

def convert_datetime(sec):
    """Convert seconds since epoch to date format used in dashboard"""
    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(sec))

class DashboardService(DASAbstractService):
    """
    Helper class to provide Dashboard service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dashboard', config)
        self.headers = {'Accept': 'text/xml'}
        self.map = {
            'jobsummary-plot-or-table' : {
                'keys' : ['jobsummary'],
                'params' : {
                    'user': '',
                    'site': '',
                    'ce': '',
                    'submissiontool': '',
                    'dataset': '',
                    'application': '',
                    'rb': '',
                    'activity': '',
                    'grid': '',
                    'date1': '',
                    'date2': '',
                    'jobtype': '',
                    'tier': '',
                    'check': 'submitted',
                }
            }
        }
        map_validator(self.map)

    def api(self, query, cond_dict=None):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        selkeys, cond = self.query_parser(query)
        api   = self.map.keys()[0] # we have only one key
        args  = dict(self.map[api]['params'])
        date1 = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(time.time()-24*60*60))
        date2 = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())
        args['date1'] = date1
        args['date2'] = date2
        for cond_dict in cond:
            if  type(cond_dict) is not types.DictType:
                continue # we got 'and' or 'or'
            key = cond_dict['key']
            op  = cond_dict['op']
            value = cond_dict['value']
            if  op == '=':
                if  key == 'date':
                    if  type(value) is not types.ListType \
                    and len(value) != 2:
                        msg  = 'Dashboard service requires 2 time stamps.'
                        msg += 'Please use either date last XXh format or'
                        msg += 'date in YYYYMMDD-YYYYMMDD'
                        raise Exception(msg)
                    args['date1'] = convert_datetime(value[0])
                    args['date2'] = convert_datetime(value[1])
                else:
                    args[key] = value
            else:
                msg = 'JobSummary does not support operator %s' % op
                raise Exception(msg)
        url = self.url + '/' + api + '?%s' % urllib.urlencode(args)
        res = self.getdata(url, {}, headers=self.headers)
        data = [i for i in parser(res)]
        for value in data:
            row = {}
            for key in selkeys:
                entity = key.split('.')[0]
                if  entity in self.keys():
                    if  key.find('.') != -1: # we got key.attr
                        value = jsonparser4key({entity:value}, key)
                    row[key] = value
                else:
                    row[key] = ''
            yield row
