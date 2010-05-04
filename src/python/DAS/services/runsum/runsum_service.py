#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
RunSummary service
"""
__revision__ = "$Id: runsum_service.py,v 1.7 2009/09/02 19:56:38 valya Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "Valentin Kuznetsov"

import os
import time
import ConfigParser
try:
    # Python 2.5
    import xml.etree.ElementTree as ET
except:
    # prior requires elementtree
    import elementtree.ElementTree as ET

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, get_key_cert, dasheader
#from DAS.services.runsum.runsum_parser import parser as runsum_parser
from DAS.services.runsum.run_summary import get_run_summary

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
        selkeys, conditions = self.query_parser(query)
        params  = dict(self.params)
        for item in conditions:
            params[item['key'].upper()] = item['value']
        key, cert = get_key_cert()
        debug   = 0
        if  self.verbose > 1:
            debug = 1
        try:
            time0 = time.time()
            data  = get_run_summary(self.url, params, key, cert, debug)
            api = self.map.keys()[0] # we only register 1 API
            genrows = self.parser(api, data, params)
            ctime = time.time()-time0
            header = dasheader(self.name, query, api, self.url, params,
                ctime, self.expire, self.version())
            header['lookup_keys'] = self.lookup_keys(api)
            header['selection_keys'] = selkeys
            mongo_query = self.mongo_query_parser(query)
            self.localcache.update_cache(mongo_query, genrows, header)
        except:
            msg = 'Fail to process: url=%s, api=%s, args=%s' \
                    % (self.url, api, params)
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
