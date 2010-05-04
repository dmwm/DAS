#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Monitor service
"""
__revision__ = "$Id: monitor_service.py,v 1.3 2009/04/30 18:31:33 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

import time
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import query_params, map_validator

class MonitorService(DASAbstractService):
    """
    Helper class to provide Monitor service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'monitor', config)
        self.map = {
            '/plotfairy/phedex/prod/link-rate:plot' : {
                'keys' : ['monitor.site', 'monitor.node',
                          'monitor.country', 'monitor.region',
                          'monitor.tier' ],
                'params' : {
                    'session': 'kkk777',
                    'version': '1224790775',
                    'span': 'hour',
                    'start': '',
                    'end': '1224792000',
                    'qty': 'destination',
                    'grouping': 'node',
                    'src-grouping': 'same',
                    'links': 'nomss',
                    'from': '',
                    'to': '',
                    'type': 'json',
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
        api  = self.map.keys()[0] # we have only one key
        args = dict(self.map[api]['params'])
        data = []
        for key in selkeys:
            args['grouping'] = key.split('.')[-1]
            args['end'] = '%d' % time.time()
            url = self.url + '/' + api
            res = self.getdata(url, args)
            jsondict = eval(res)
            monitor_time = jsondict['series']
            monitor_data = jsondict['data']
            items = ({'time':t, 'data':d} for t, d in \
                                zip(monitor_time, monitor_data))
            data  = [{key: d} for d in items]
#            data  = [{key: '%s' % str(d)} for d in items]
        # TODO: I need to see how to multiplex data for different keys
        return data

