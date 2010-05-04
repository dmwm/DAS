#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
RunSummary service
"""
__revision__ = "$Id: runsum_service.py,v 1.11 2009/10/21 15:41:04 valya Exp $"
__version__ = "$Revision: 1.11 $"
__author__ = "Valentin Kuznetsov"

import os
import time
import types
import ConfigParser
import traceback
import xml.etree.cElementTree as ET

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, get_key_cert, dasheader
from DAS.services.runsum.run_summary import get_run_summary

def convert_datetime(sec):
    """Convert seconds since epoch to date format used in RunSummary"""
    return time.strftime("%Y.%m.%d %H:%M:%S", time.gmtime(sec))

def runsum_keys():
    """Retrieve run summary keys directly from dasmap.cfg file"""
    if  os.environ.has_key('DAS_ROOT'):
        dasconfig = os.path.join(os.environ['DAS_ROOT'], 'etc/dasmap.cfg')
        if  not os.path.isfile(dasconfig):
            raise EnvironmentError('No DAS mapconfig file %s found' % dasconfig)
    else:
        raise EnvironmentError('DAS_ROOT environment is not set up')
    config = ConfigParser.ConfigParser()
    config.read(dasconfig)
    keys = config.options('runsum')
    return keys

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

    def api(self, query):
        """
        Invoke DBS API to execute given query.
        Return results as a list of dict, e.g.
        [{'run':1,'dataset':/a/b/c'}, ...]
        """
        # translate selection keys into ones data-service APIs provides
        selkeys = query['fields']
        cond = query['spec']
        args = dict(self.params)
        for key, value in cond.items():
            if  type(value) is not types.DictType: # we got equal condition
                if  key == 'date':
                    if  type(value) is not types.ListType \
                    and len(value) != 2:
                        msg  = 'RunSummary service requires 2 time stamps.'
                        msg += 'Please use either date last XXh format or'
                        msg += 'date in YYYYMMDD-YYYYMMDD'
                        raise Exception(msg)
                    args['TIME_BEGIN'] = convert_datetime(value[0])
                    args['TIME_END']   = convert_datetime(value[1])
                else:
                    for param in self.dasmapping.das2api(self.name, key):
                        args[param] = value
            else: # we got some operator, e.g. key :{'$in' : [1,2,3]}
                # TODO: not sure how to deal with them right now, will throw
                msg = 'RunSummary does not support operator %s' % oper
                raise Exception(msg)
        if  args == self.params: # no parameter is provided
            args['TIME_END'] = convert_datetime(time.time())
            args['TIME_BEGIN'] = convert_datetime(time.time() - 24*60*60)
        key, cert = get_key_cert()
        debug   = 0
        if  self.verbose > 1:
            debug = 1
        try:
            time0 = time.time()
            data  = get_run_summary(self.url, args, key, cert, debug)
            api = self.map.keys()[0] # we only register 1 API
            genrows = self.parser(api, data, args)
            ctime = time.time()-time0
            header = dasheader(self.name, query, api, self.url, args,
                ctime, self.expire, self.version())
            header['lookup_keys'] = self.lookup_keys(api)
            header['selection_keys'] = selkeys
            mongo_query = query
            self.analytics.add_api(self.name, query, api, args)
            self.localcache.update_cache(mongo_query, genrows, header)
        except:
            traceback.print_exc()
            msg = 'Fail to process: url=%s, api=%s, args=%s' \
                    % (self.url, api, args)
            self.logger.warning(msg)
        return True

    def parser(self, api, data, params=None):
        """
        RunSummary XML parser, it returns a list of dict rows, e.g.
        [{'file':value, 'run':value}, ...]
        """
        try:
            elem  = ET.fromstring(data)
        except:
            print "data='%s'" % data
            raise Exception('Unable to parse run summary output')
        for i in elem:
            if  i.tag == 'runInfo':
                row = {}
                for j in i:
                    if  j.tag:
                        newkey = self.dasmapping.notation2das(self.name, j.tag)
                        row[newkey] = j.text
                    nrow = {}
                    for k in j.getchildren():
                        if  k.tag:
                            nkey = self.dasmapping.notation2das(self.name, k.tag)
                            nrow[nkey] = k.text
                    if  nrow:
                        row[newkey] = nrow
                yield {'run' : row}
