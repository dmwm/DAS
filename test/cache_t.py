#!/usr/bin/env python
#pylint: disable-msg=c0301,c0103

"""
unit test for cache module
"""

import unittest
from core.cache import Cache
from core.das_core import DASCore

class testCache(unittest.TestCase):
    """
    a test class for the das cache module
    """
    def setUp(self):
        """
        set up das core module
        """
        debug = 0
        self.das = DASCore(debug=debug)
        self.dascache = Cache(self.das)

    def test_result(self):                          
        """test cache result method"""
        query  = "find site where site=T2_UK"
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

