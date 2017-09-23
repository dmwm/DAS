#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Monitor service
"""
__author__ = "Valentin Kuznetsov"

# system modules
import json
import time

# DAS modules
from   DAS.services.abstract_service import DASAbstractService
from   DAS.utils.utils import map_validator, convert2date, das_dateformat
from   DAS.utils.regex import unix_time_pattern, date_yyyymmdd_pattern

def convert_datetime(sec):
    """
    Convert seconds since epoch or YYYYMMDD to date YYYY-MM-DD
    """
    value = str(sec)
    pat   = date_yyyymmdd_pattern
    pat2  = unix_time_pattern
    if pat.match(value): # we accept YYYYMMDD
        return das_dateformat(value)
    elif pat2.match(value):
        return value
    else:
        msg = 'Unacceptable date format'
        raise Exception(msg)

class MonitorService(DASAbstractService):
    """
    Helper class to provide Monitor service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'monitor', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def parser(self, dasquery, dformat, source, args):
        """
        Data parser for Monitor service.
        """
        if  hasattr(source, "close"):
            try: # we got data descriptor
                data = source.read()
            except:
                source.close()
                raise
            source.close()
        elif isinstance(source, object) and hasattr(source, 'read'): # StringIO
            data = source.read()
            source.close()
        else:
            data = source
        try:
            row  = json.loads(data)
        except:
            msg  = "MonitorService::parser,"
            msg += " WARNING, fail to JSON'ify data:\n%s" % data
            self.logger.warning(msg)
            row  = eval(data, { "__builtins__": None }, {})
        try:
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
        except:
            yield dict(monitor=row)

    def apicall(self, dasquery, url, api, args, dformat, expire):
        """
        An overview data-service worker.
        """
        keys = [key for key in self.map[api]['keys'] for api in self.map.keys()]
        cond = dasquery.mongo_query['spec']
        for key, value in cond.items():
            if  key.find('.') != -1:
                args['grouping'] = key.split('.')[-1]
                key, _attr = key.split('.', 1)
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
                    if  isinstance(value, str) or isinstance(value, unicode):
                        value = convert2date(value)
                    else:
                        value = [value, value + 24*60*60]
                    args['start'] = value[0]
                    args['end']   = value[1]
            else: # we got some operator, e.g. key :{'$in' : [1,2,3]}
                if  key == 'date':
                    if  '$in' in value:
                        vallist = value['$in']
                    elif '$lte' in value and '$gte' in value:
                        vallist = (value['$gte'], value['$lte'])
                    else:
                        err = 'Unsupported date value'
                        raise Exception(err)
                    args['start'] = convert_datetime(vallist[0])
                    args['end'] = convert_datetime(vallist[-1])
                else:
                    raise Exception(err)
            time0   = time.time()
            res, expire = self.getdata(url, args, expire)
            dasrows = self.parser(dasquery, dformat, res, args)
            ctime   = time.time() - time0
            self.write_to_cache(\
                dasquery, expire, url, api, args, dasrows, ctime)

