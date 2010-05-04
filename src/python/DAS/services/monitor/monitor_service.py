#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Monitor service
"""
__revision__ = "$Id: monitor_service.py,v 1.6 2009/09/02 19:56:38 valya Exp $"
__version__ = "$Revision: 1.6 $"
__author__ = "Valentin Kuznetsov"

import time
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

    def api(self, query):
        """
        An overview data-service worker.
        """
        selkeys, cond = self.query_parser(query)
        api  = self.map.keys()[0] # we have only one key
        args = dict(self.map[api]['params'])
        data = []
        mongo_query = self.mongo_query_parser(query)
        keys = [key for key in self.map[api]['keys'] for api in self.map.keys()]
        for key in selkeys:
            if  key not in keys:
                continue
            if  key.find('.') != -1:
                args['grouping'] = key.split('.')[-1]
            args['end'] = '%d' % time.time()
            url = self.url + '/' + api
            time0 = time.time()
            res = self.getdata(url, args)
            jsondict = eval(res)
            monitor_time = jsondict['series']
            monitor_data = jsondict['data']
            items = ({'time':list(t), 'data':d} for t, d in \
                                zip(monitor_time, monitor_data))
#            data  = [{key: d} for d in items]
            data  = [{'monitor':{args['grouping'] : d}} for d in items]

            ctime = time.time() - time0
            header = dasheader(self.name, query, api, self.url, args,
                ctime, self.expire, self.version())
            header['lookup_keys'] = self.lookup_keys(api)
            header['selection_keys'] = selkeys
            self.localcache.update_cache(mongo_query, data, header)
        return True

