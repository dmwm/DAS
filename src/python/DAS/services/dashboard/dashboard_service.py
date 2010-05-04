#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Dashboard service
"""
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import time
import urllib
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import query_params, map_validator
from DAS.services.dashboard.dashboard_parser import parser

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
        selkeys, cond = query_params(query)
        api   = self.map.keys()[0] # we have only one key
        args  = dict(self.map[api]['params'])
        date1 = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(time.time()-24*60*60))
        date2 = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())
        data  = None
        for key in selkeys:
            args['date1'] = date1
            args['date2'] = date2
            if  cond.has_key('jobsummary.site'):
                args['site'] = cond['jobsummary.site'][1]
            url = self.url + '/' + api + '?%s' % urllib.urlencode(args)
            res = self.getdata(url, {}, headers=self.headers)
            data = [i for i in parser(res)]
        # TODO: I need to see how to multiplex data for different keys
        print data
        import sys
        sys.exit(0)
        return data

