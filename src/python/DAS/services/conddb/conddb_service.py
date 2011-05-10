#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
CondDB service
"""
__author__ = "Valentin Kuznetsov"

import time
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, convert2date

def convert_datetime(sec):
    """
    Convert seconds since epoch or YYYYMMDD to date format used in CondDB
    time format: dd-Mon-yy-HH:MM
    """
    value = str(sec)
    if  len(value) == 8 and value[:2] == "20": # we got YYYYMMDD
        return time.strftime("%d-%b-%y-%H:%M", time.strptime(value, "%Y%M%d"))
    return time.strftime("%d-%b-%y-%H:%M", time.gmtime(sec))

class CondDBService(DASAbstractService):
    """
    Helper class to provide CondDB service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'conddb', config)
        self.reserved = ['api', 'apiversion']
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def adjust_params(self, api, kwds, _inst=None):
        """
        Adjust CondDB parameters for specific query requests
        """
        day = 24*60*60
        if  api == 'getLumi':
            if  kwds.has_key('date') and kwds['date'] != 'optional':
                value = kwds['date']
                if  isinstance(value, str):
                    value = convert2date(value)
                elif isinstance(value, dict):
                    value = [kwds['date']['$gte'], kwds['date']['$lte']]
                elif isinstance(value, int):
                    value = [value, value+day]
                else:
                    msg = 'Unsupported date format %s' % kwds['date']
                    raise Exception(msg)
                kwds['startTime'] = convert_datetime(value[0])
                kwds['endTime'] = convert_datetime(value[1])
                del kwds['date']
            elif kwds.has_key('runList'):
                val = kwds['runList']
                if  isinstance(val, dict): # we got a run range
                    kwds['runList'] = '%s-%s' % (val['$gte'], val['$lte'])
        if  api == 'get_run_info':
            if  kwds.has_key('date') and kwds['date'] != 'optional':
                value = kwds['date']
                if  isinstance(value, str):
                    value = convert2date(value)
                elif isinstance(value, dict):
                    value = [kwds['date']['$gte'], kwds['date']['$lte']]
                elif isinstance(value, int):
                    value = [value, value+day]
                else:
                    msg = 'Unsupported date format %s' % kwds['date']
                    raise Exception(msg)
                kwds['startTime'] = convert_datetime(value[0])
                kwds['endTime'] = convert_datetime(value[1])
                del kwds['date']

    def parser(self, query, dformat, source, api):
        """
        CondDB data-service parser.
        """
        gen = DASAbstractService.parser(self, query, dformat, source, api)
        for row in gen:
            if  api == 'get_lumi_info':
                for lumi in row['lumi']['Lumi']:
                    yield lumi
            else:
                yield row

