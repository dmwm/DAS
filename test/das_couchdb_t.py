#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS couchdb cache module
"""

import unittest
from utils.utils import genkey
from core.das_couchdb import DASCouchDB
from core.das_core import DASCore

class testDASCouchDB(unittest.TestCase):
    """
    A test class for the DAS cache module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug = 0
        self.das = DASCore(debug=debug)
        self.dascache = DASCouchDB(self.das)

    def test_result(self):                          
        """test DAS couchdb cache result method"""
        query  = "find dataset,admin,node where site=T2_UK_SGrid_Bristol"
#        query  = "find site where site=T2_UK"
        if  self.dascache.test():
            result = self.dascache.result(query)
            result.sort()
            expect = self.das.result(query)
            expect.sort()
            self.assertEqual(expect, result)
#
# main
#
if __name__ == '__main__':
    unittest.main()
