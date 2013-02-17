#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS core module
"""

import os
import unittest

from pymongo.connection import Connection

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
        self.das = DASCore(debug=debug)
        config = deepcopy(das_readconfig())
        dburi = config['mongodb']['dburi']
        connection = Connection(dburi)
        connection.drop_database('das') 

    def testAggregators(self):
        """test DASCore aggregators via zip service"""
        # test DAS workflow
        query  = "zip=14850 | grep zip.Placemark.address | count(zip.Placemark.address)"
        dquery = DASQuery(query)
        result = self.das.call(dquery)
        result = self.das.get_from_cache(dquery)
        result = [r for r in result][0]
        if  result.has_key('das'):
            del result['das'] # strip off DAS info
        expect = {"function": "count", "result": {"value": 1}, 
                  "key": "zip.Placemark.address", "_id":0}
        self.assertEqual(expect, result)

    def testIPService(self):
        """test DASCore with IP service"""
        # test DAS workflow
        query  = "ip=137.138.141.145"
        dquery = DASQuery(query)
        result = self.das.call(dquery)
        expect = "ok"
        self.assertEqual(expect, result)

        # test results
        query  = "ip=137.138.141.145 | grep ip.City"
        dquery = DASQuery(query)
        result = self.das.get_from_cache(dquery)
        result = [r for r in result][0]
        result = DotDict(result).get('ip.City')
        expect = 'Geneva'
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()


