#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS cache module
"""

import unittest
from core.das_memcache import DASCache
from core.das_core import DASCore

class testDASCache(unittest.TestCase):
    """
    A test class for the DAS cache module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.dascache = DASCache(debug=1)
        self.das = DASCore()

    def test_key(self):                          
        """test DAS cache key generator"""
        query  = "find dataset,admin,node where site=T2_UK_SGrid_Bristol"
        result = self.dascache.genkey(query)
        import md5
        hash = md5.new()
        hash.update(query)
        expect = hash.hexdigest()
        self.assertEqual(expect, result)

    def test_result(self):                          
        """test DAS cache result method"""
        query  = "find dataset,admin,node where site=T2_UK_SGrid_Bristol"
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
