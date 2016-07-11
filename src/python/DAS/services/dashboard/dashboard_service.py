#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703

"""
Dashboard service
"""
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import time
import xml.etree.cElementTree as ET
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, convert2date, print_exc
                
def convert_datetime(sec):
    """
    Convert seconds since epoch or YYYYMMDD to date format used in dashboard
    """
    value = str(sec)
    if  len(value) == 8: # we got YYYYMMDD
        return "%s-%s-%s" % (value[:4], value[4:6], value[6:8])
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(sec))

class DashboardService(DASAbstractService):
    """
    Helper class to provide Dashboard service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dashboard', config)
        self.headers = {'Accept': 'text/xml'}
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def parser(self, source, api, params=None):
        """
        Dashboard data-service parser.
        """
        close = False
        if  hasattr(source, "read"):
            data = source.read()
            close = True
        elif isinstance(source, object) and hasattr(source, 'read'): # StringIO
            data = source.read()
            close = True
        else:
            data = source
        try:
            elem  = ET.fromstring(data)
            for i in elem:
                if  i.tag == 'summaries':
                    for j in i:
                        row = {}
                        for k in j.getchildren():
                            name = k.tag
                            row[name] = k.text
                        if  params:
                            for key, val in params.items():
                                if  key not in row:
                                    row[key] = val
                        rowkey = self.map[api]['keys'][0]
                        yield {rowkey : row}
        except:
            yield {'error' : data}
        if  close:
            source.close()

    def apicall(self, dasquery, url, api, args, dformat, expire):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        cond   = dasquery.mongo_query['spec']
        count  = 0
        for key, value in cond.items():
            err = 'JobSummary does not support key=%s, value=%s' \
                    % (key, value)
            if  not isinstance(value, dict): # we got equal condition
                if  key == 'date':
                    if  isinstance(value, list) and len(value) != 2:
                        msg  = 'Dashboard service requires 2 time stamps.'
                        msg += 'Please use either date last XXh format or'
                        msg += 'date in [YYYYMMDD, YYYYMMDD]'
                        raise Exception(msg)
                    if  isinstance(value, str) or isinstance(value, unicode):
                        value = convert2date(value)
                    else:
                        value = [value, value + 24*60*60]
                    args['date1'] = convert_datetime(value[0])
                    args['date2'] = convert_datetime(value[1])
                    count += 1
                else:
                    for param in self.dasmapping.das2api(self.name, api, key):
                        args[param] = value
                        count += 1
            else: # we got some operator, e.g. key :{'$in' : [1,2,3]}
                if  key == 'date' or key == 'jobsummary':
                    if  '$in' in value:
                        vallist = value['$in']
                    elif '$lte' in value and '$gte' in value:
                        vallist = (value['$gte'], value['$lte'])
                    else:
                        raise Exception(err)
                    args['date1'] = convert_datetime(vallist[0])
                    args['date2'] = convert_datetime(vallist[-1])
                    count += 1
                else:
                    raise Exception(err)
        if  not count:
            # if no parameter are given, don't pass the API
            msg  = 'DashboardService::api\n\n'
            msg += "--- %s reject API %s, parameters don't match, args=%s" \
                    % (self.name, api, args)
            self.logger.info(msg)
            return
        else:
            if  not args['date1']:
                args['date1'] = convert_datetime(time.time()-24*60*60)
            if  not args['date2']:
                args['date2'] = convert_datetime(time.time())
        # drop date argument, since it's used by DAS not by dashboard data srv
        if  'date' in args:
            args.pop('date')

        time0 = time.time()
        res, expire = self.getdata(url, args, expire, headers=self.headers)
        rawrows = self.parser(res, api, args)
        dasrows = self.translator(api, rawrows)
        ctime = time.time() - time0
        try:
            self.write_to_cache(\
                dasquery, expire, url, api, args, dasrows, ctime)
        except Exception as exc:
            print_exc(exc)
