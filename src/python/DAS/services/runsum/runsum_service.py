#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
RunSummary service
"""
__revision__ = "$Id: runsum_service.py,v 1.21 2010/05/03 14:59:13 valya Exp $"
__version__ = "$Revision: 1.21 $"
__author__ = "Valentin Kuznetsov"

import time
try: # python3, we use urllib.urlencode which now is urllib.parse.urlencode
    import urllib.parse as urllib
except: # fallback to python2, we use urllib.urlencode
    import urllib
import traceback
import xml.etree.cElementTree as ET

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, get_key_cert, adjust_value
from DAS.utils.cern_sso_auth import get_data

def run_summary_url(url, params):
    """Construct Run Summary URL from provided parameters"""
    if  url[-1] == '/':
        url = url[:-1]
    if  url[-1] == '?':
        url = url[:-1]
    paramstr = ''
    for key, val in params.items():
        if  isinstance(val, list):
            paramstr += '%s=%s&' % (key, urllib.quote(val))
        elif key.find('TIME') != -1:
            paramstr += '%s=%s&' % (key, urllib.quote(val))
        else:
            paramstr += '%s=%s&' % (key, val)
    return url + '?' + paramstr[:-1]

def convert_datetime(sec):
    """Convert seconds since epoch to date format used in RunSummary"""
    return time.strftime("%Y.%m.%d %H:%M:%S", time.gmtime(sec))

class RunSummaryService(DASAbstractService):
    """
    Helper class to provide RunSummary service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'runsum', config)
        self.results = []
        self.params  = {'DB':'cms_omds_lb', 'FORMAT':'XML'}
        self._keys   = None
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def apicall(self, dasquery, url, api, args, dformat, expire):
        """
        Invoke DBS API to execute given query.
        Return results as a list of dict, e.g.
        [{'run':1,'dataset':/a/b/c'}, ...]
        """
        # translate selection keys into ones data-service APIs provides
        cond = dasquery.mongo_query['spec']
        args = dict(self.params)
        for key, value in cond.items():
            if  isinstance(value, dict): # we got equal condition
                if  key == 'date':
                    if  isinstance(value, list) and len(value) != 2:
                        msg  = 'RunSummary service requires 2 time stamps.'
                        msg += 'Please use either date last XXh format or'
                        msg += 'date in YYYYMMDD-YYYYMMDD'
                        raise Exception(msg)
                    args['TIME_BEGIN'] = convert_datetime(value[0])
                    args['TIME_END']   = convert_datetime(value[1])
                else:
                    for param in self.dasmapping.das2api(self.name, api, key):
                        args[param] = value
            elif key == 'run.number' or key == 'run.run_number':
                minrun = None
                maxrun = None
                for oper, val in value.items():
                    if  oper == '$in':
                        minrun = int(val[0])
                        maxrun = int(val[-1])
                    elif oper == '$lt':
                        maxrun = int(val) - 1
                    elif oper == '$lte':
                        maxrun = int(val)
                    elif oper == '$gt':
                        minrun = int(val) + 1
                    elif oper == '$gte':
                        minrun = int(val)
                    else:
                        msg = 'RunSummary does not support operator %s' % oper
                        raise Exception(msg)
                args['RUN_BEGIN'] = minrun
                args['RUN_END']   = maxrun
            elif key == 'date' and '$in' in value and \
                len(value['$in']) == 2:
                date1, date2 = value['$in']
                args['TIME_BEGIN'] = convert_datetime(date1)
                args['TIME_END']   = convert_datetime(date2)
            else: # we got some operator, e.g. key :{'$in' : [1,2,3]}
                # TODO: not sure how to deal with them right now, will throw
                msg = 'RunSummary does not support value %s for key=%s' \
                % (value, key)
                raise Exception(msg)
        if  args == self.params: # no parameter is provided
            args['TIME_END'] = convert_datetime(time.time())
            args['TIME_BEGIN'] = convert_datetime(time.time() - 24*60*60)
        key, cert = get_key_cert()
        debug   = 0
        if  self.verbose > 1:
            debug   = 1
        try:
            time0   = time.time()
            api     = list(self.map.keys())[0] # we only register 1 API
            url     = self.map[api]['url']
            expire  = self.map[api]['expire']
            msg     = 'DASAbstractService::%s::getdata(%s, %s)' \
                    % (self.name, url, args)
            self.logger.info(msg)
            data    = get_data(run_summary_url(url, args), key, cert, debug)
            genrows = self.parser(data, api)
            ctime   = time.time()-time0
            self.write_to_cache(\
                dasquery, expire, url, api, args, genrows, ctime)
        except:
            traceback.print_exc()
            msg = 'Fail to process: url=%s, api=%s, args=%s' \
                    % (url, api, args)
            self.logger.warning(msg)

    def parser(self, source, api):
        """
        RunSummary data-service parser.
        """
        row     = {}
        hold    = None
        context = ET.iterparse(source, events=("start", "end"))
        root    = None
        for item in context:
            event, elem = item
            if  event == "start" and root is None:
                root = elem # the first element is root
            if  elem.tag == 'cmsdb':
                continue
            if  event == 'start' and elem.tag == 'runInfo':
                continue
            if  event == 'end' and elem.tag == 'runInfo':
                yield dict(run=row)
                row  = {}
                elem.clear()
            if  hold and event == 'end' and elem.tag == hold:
                hold = None
                continue
            if  event == 'start':
                sub = {}
                children = elem.getchildren()
                # I don't apply notation conversion to all children
                # since those are not likely to overlap
                if  children:
                    for child in children:
                        sub[child.tag] = adjust_value(child.text)
                    row[elem.tag] = sub
                    hold = elem.tag
                else:
                    if  not hold:
                        nkey = self.dasmapping.notation2das\
                            (self.name, elem.tag, api)
                        row[nkey] = adjust_value(elem.text)
        if  root:
            root.clear()
        source.close()
