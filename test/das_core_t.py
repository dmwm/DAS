#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS core module
"""

import os
import types
import unittest

from pymongo.connection import Connection

from DAS.utils.das_config import das_readconfig
from DAS.core.das_core import DASCore
from DAS.utils.utils import dotdict

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
        connection = Connection("localhost", 27017)
        connection.drop_database('das') 

    def testAddressService(self):
        """test DASCore with as address service, via google_maps"""
        # test DAS workflow
        query  = "city=Ithaca"
        query  = self.das.adjust_query(query)
        result = self.das.call(query)
        expect = 1
        self.assertEqual(expect, result)

        # test results
        query  = "city=Ithaca | grep city.Placemark.address"
        query  = self.das.adjust_query(query)
        result = self.das.get_from_cache(query)
        result = [r for r in result][0]
        result = dotdict(result)._get('city.Placemark.address')
        expect = 'Ithaca, NY, USA'
        self.assertEqual(expect, result)

    def testAggregators(self):
        """test DASCore aggregators via zip service"""
        # test DAS workflow
        query  = "zip=10000 | grep zip.Placemark.address | count(zip.Placemark.address)"
        query  = self.das.adjust_query(query)
        result = self.das.call(query)
        result = self.das.get_from_cache(query)
        result = [r for r in result][0]
        expect = {"function": "count", "result": 5, "key": "zip.Placemark.address"}
        self.assertEqual(expect, result)

    def testIPService(self):
        """test DASCore with IP service"""
        # test DAS workflow
        query  = "ip=137.138.141.145"
        query  = self.das.adjust_query(query)
        result = self.das.call(query)
        expect = 1
        self.assertEqual(expect, result)

        # test results
        query  = "ip=137.138.141.145 | grep ip.City"
        query  = self.das.adjust_query(query)
        result = self.das.get_from_cache(query)
        result = [r for r in result][0]
        result = dotdict(result)._get('ip.City')
        expect = 'Geneva'
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()


