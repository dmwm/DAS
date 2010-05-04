#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS couchdb cache module
"""

import unittest
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.core.das_couchcache import DASCouchcache

class testDASCouchcache(unittest.TestCase):
    """
    A test class for the DAS cache module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug       = 0
        self.config = das_readconfig()
        logger      = DASLogger(verbose=debug, stdout=debug)
        self.config['logger']  = logger
        self.config['verbose'] = debug
        self.couchcache = DASCouchcache(self.config)

    def test_result(self):                          
        """test DAS couchdb cache result method"""
        self.couchcache.delete_cache('das')
        self.couchcache = DASCouchcache(self.config)
        query  = "find site where site=T2_UK"
        expire = 60
        record = [{'test':i} for i in range(0, 10)]
        expect = record
        expect = self.couchcache.update_cache(query, record, expire)
        expect = [i for i in expect]
        result = [i for i in self.couchcache.get_from_cache(query)]
        result.sort()
        self.assertEqual(expect, result)

    def test_pagination(self):                          
        """test DAS couchdb cache result method using pagination"""
        self.couchcache.delete_cache('das')
        self.couchcache = DASCouchcache(self.config)
        query  = "find site where site=T2_UK"
        expire = 60
        record = [{'test':i} for i in range(0, 10)]
        expect = record
        expect = self.couchcache.update_cache(query, record, expire)
        expect = [i for i in expect]
        idx    = 1
        limit  = 3
        result = [i for i in self.couchcache.get_from_cache(query, idx, limit)]
        result.sort()
        self.assertEqual(expect, result)
#
# main
#
if __name__ == '__main__':
    unittest.main()
