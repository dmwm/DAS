#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Monitor service
"""
__revision__ = "$Id: monitor_service.py,v 1.15 2010/03/05 18:08:08 valya Exp $"
__version__ = "$Revision: 1.15 $"
__author__ = "Valentin Kuznetsov"

import time
import traceback
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, convert2date
import DAS.utils.jsonwrapper as json

class MonitorService(DASAbstractService):
    """
    Helper class to provide Monitor service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'monitor', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def parser(self, query, dformat, source, args):
        """
        Data parser for Monitor service.
        """
        data = source.read()
        source.close()
#        row  = eval(data)
        try:
            row  = json.loads(data)
        except:
            msg  = "MonitorService::parser,"
            msg += " WARNING, fail to JSON'ify data:\n%s" % data
            self.logger.warning(msg)
#            traceback.print_exc()
            row  = eval(data, { "__builtins__": None }, {})
        monitor_time = row['series']
        monitor_data = row['data']
        items = ({'time':list(t), 'data':d} for t, d in \
                            zip(monitor_time, monitor_data))
        for row in items:
            interval = row['time']
            dataval  = row['data']
            for key, val in dataval.items():
                newrow = {'time': interval}
                newrow[args['grouping']] = key
                newrow['rate'] = val
                yield dict(monitor=newrow)

    def apicall(self, query, url, api, args, dformat, expire):
        """
        An overview data-service worker.
        """
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

            if  not isinstance(value, dict): # we got equal condition
                if  key == 'date':
                    if  isinstance(value, list) and len(value) != 2:
                        msg  = 'Monitor service requires 2 time stamps.'
                        msg += 'Please use either date last XXh format or'
                        msg += 'date in [YYYYMMDD, YYYYMMDD]'
                        raise Exception(msg)
                    if  isinstance(value, str):
                        value = convert2date(value)
                    else:
                        value = [value, value + 24*60*60]
                    args['start'] = value[0]
                    args['end']   = value[1]
            else: # we got some operator, e.g. key :{'$in' : [1,2,3]}
                if  key == 'date':
                    if  value.has_key('$in'):
                        vallist = value['$in']
                    elif value.has_key('$lte') and value.has_key('$gte'):
                        vallist = (value['$gte'], value['$lte'])
                    else:
                        raise Exception(err)
                    args['start'] = convert_datetime(vallist[0])
                    args['end'] = convert_datetime(vallist[-1])
                else:
                    raise Exception(err)


            time0 = time.time()
            res = self.getdata(url, args)
            try:
                genrows = self.parser(query, dformat, res, args)
            except:
                traceback.print_exc()
            dasrows = self.set_misses(query, api, genrows)
            ctime = time.time() - time0
            self.write_to_cache(query, expire, url, api, args, dasrows, ctime)

