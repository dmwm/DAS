#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Monitor service
"""
__revision__ = "$Id: monitor_service.py,v 1.10 2010/01/04 19:03:51 valya Exp $"
__version__ = "$Revision: 1.10 $"
__author__ = "Valentin Kuznetsov"

import time
import traceback
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, dasheader

class MonitorService(DASAbstractService):
    """
    Helper class to provide Monitor service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'monitor', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def parser(self, source, args):
        """
        Data parser for Monitor service.
        """
        data = source.read()
        source.close()
        row  = eval(data)
        monitor_time = row['series']
        monitor_data = row['data']
        items = ({'time':list(t), 'data':d} for t, d in \
                            zip(monitor_time, monitor_data))
#        data = []
        for row in items:
            interval = row['time']
            dataval  = row['data']
            for key, val in dataval.items():
                newrow = {'time': interval}
                newrow[args['grouping']] = key
                newrow['rate'] = val
                yield dict(monitor=newrow)
#                data.append(dict(monitor=newrow))
#        return data

    def api(self, query):
        """
        An overview data-service worker.
        """
        api  = self.map.keys()[0] # we have only one key
        args = dict(self.map[api]['params'])
        data = []
        keys = [key for key in self.map[api]['keys'] for api in self.map.keys()]
        cond = query['spec']
        for key, value in cond.items():
            if  key.find('.') != -1:
                args['grouping'] = key.split('.')[-1]
                key, attr = key.split('.', 1)
            if  key not in keys:
                continue
            args['end'] = '%d' % time.time()
            url = self.url + '/' + api
            time0 = time.time()
            print "url, args", url, args
            res = self.getdata(url, args)
            try:
                genrows = self.parser(res, args)
            except:
                traceback.print_exc()
            ctime = time.time() - time0
            header = dasheader(self.name, query, api, self.url, args,
                ctime, self.expire, self.version())
            header['lookup_keys'] = self.lookup_keys(api)
            self.analytics.add_api(self.name, query, api, args)
            self.localcache.update_cache(query, genrows, header)
        return True

