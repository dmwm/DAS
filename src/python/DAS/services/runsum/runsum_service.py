#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
RunSummary service
"""
__revision__ = "$Id: runsum_service.py,v 1.2 2009/06/03 19:43:21 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

import os

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator
from DAS.services.runsum.runsum_parser import parser as runsum_parser
from DAS.services.runsum.run_summary import get_run_summary
from DAS.core.das_mapping import json2das, das2api, das2result

class RunSummaryService(DASAbstractService):
    """
    Helper class to provide RunSummary service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'runsum', config)
        self.results = []
        self.params  = {'DB':'cms_omds_lb', 'FORMAT':'XML'}
        self._keys   = None
        # DBS uses DBSServlet and API passed as parameter, so we don't
        # define api in map and rather use url w/ DBSServlet
        self.map     = {
            '' : {
                'keys' : self.runsum_keys(),
                'params' : self.params
            }
        }
        map_validator(self.map)

    def runsum_keys(self):
        keys = ['run', 'seq', 'bfield', 'hlt', 'hlt.version', 'hlt.desc']
        return keys

    def api(self, query, cond_dict=None):
        """
        Invoke DBS API to execute given query.
        Return results as a list of dict, e.g.
        [{'run':1,'dataset':/a/b/c'}, ...]
        """
        # translate selection keys into ones data-service APIs provides
        selkeys, conditions = self.query_parser(query)
        keylist = []
        for key in selkeys:
            res = das2result(self.name, key)
            for item in das2result(self.name, key):
                keylist.append(item)

        url     = self.url
        params  = dict(self.params)
        for item in conditions:
            params[item['key'].upper()] = item['value']
        key     = os.path.join(os.environ['HOME'], '.globus/newkey.pem')
        cert    = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
        debug   = 0
        if  self.verbose > 1:
            debug = 1
        res     = get_run_summary(self.url, params, key, cert, debug)
#        data    = runsum_parser(res)
        # similar to worker method, get all results, parse them and do
        # mapping between returned keys to DAS ones
        res     = runsum_parser(res)
        jsondict= [i for i in res][0]
        # NOTE: json2das accepts dict as {'system':{result dict}}
        data = json2das(self.name, {'runsum':jsondict}, keylist, selkeys)
        return data
