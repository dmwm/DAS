#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS cache module
"""

import unittest
from DAS.utils.utils import genkey
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.core.das_memcache import DASMemcache

class testDASMemcache(unittest.TestCase):
    """
    A test class for the DAS cache module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug    = 0
        config   = das_readconfig()
        logger   = DASLogger(verbose=debug, stdout=debug)
        config['logger']  = logger
        config['verbose'] = debug
        self.memcache = DASMemcache(config)

    def test_key(self):                          
        """test DAS cache key generator"""
        query  = "find site where site=T2_UK"
        result = genkey(query)
        import md5
        hash = md5.new()
        hash.update(query)
        expect = hash.hexdigest()
        self.assertEqual(expect, result)

    def test_result(self):                          
        """test DAS memcache result method"""
        self.memcache.delete_cache()
        query  = "find site where site=T2_UK"
        expire = 60
        expect = [0,1,2,3,4,5,6,7,8,9]
        expect = self.memcache.update_cache(query, expect, expire)
        expect = [i for i in expect]
        result = [i for i in self.memcache.get_from_cache(query)]
        result.sort()
        self.assertEqual(expect, result)

    def test_pagintation(self):                          
        """test DAS memcache result method with pagination"""
        self.memcache.delete_cache()
        query  = "find site where site=T2_UK"
        expire = 60
        expect = [0,1,2,3,4,5,6,7,8,9]
        expect = self.memcache.update_cache(query, expect, expire)
        expect = [i for i in expect]
        idx    = 1
        limit  = 3
        result = [i for i in self.memcache.get_from_cache(query, idx, limit)]
        result.sort()
        self.assertEqual(expect[idx:limit], result)
#
# main
#
if __name__ == '__main__':
    unittest.main()
