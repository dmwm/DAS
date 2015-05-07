#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS core module
"""

import os
import socket
import unittest

from pymongo import MongoClient

from DAS.utils.das_config import das_readconfig
from DAS.core.das_core import DASCore
from DAS.core.das_query import DASQuery
from DAS.utils.ddict import DotDict
from DAS.utils.utils import deepcopy

class testDASCore(unittest.TestCase):
    """
    A test class for the DAS core module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug = 0
        self.das = DASCore(debug=debug, multitask=False)
        config = deepcopy(das_readconfig())
        dburi = config['mongodb']['dburi']
        connection = MongoClient(dburi)
        connection.drop_database('das') 

    def testAggregators(self):
        """test DASCore aggregators via zip service"""
        # test DAS workflow
        query  = "zip=14850 | grep zip.code | count(zip.code)"
        dquery = DASQuery(query)
        result = self.das.call(dquery)
        result = self.das.get_from_cache(dquery)
        result = [r for r in result][0]
        if  'das' in result:
            del result['das'] # strip off DAS info
        expect = {"function": "count", "result": {"value": 1}, 
                  "key": "zip.code", "_id":0}
        self.assertEqual(expect, result)

    def testIPService(self):
        """test DASCore with IP service"""
        ipaddr = socket.gethostbyname('cmsweb.cern.ch')
        # test DAS workflow
        query  = "ip=%s" % ipaddr
        dquery = DASQuery(query)
        result = self.das.call(dquery)
        expect = "ok"
        self.assertEqual(expect, result)

        # test results
        query  = "ip=%s | grep ip.address" % ipaddr
        dquery = DASQuery(query)
        result = self.das.get_from_cache(dquery)
        result = [r for r in result][0]
        result = DotDict(result).get('ip.address')
        expect = ipaddr
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()


