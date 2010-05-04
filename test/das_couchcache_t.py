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
        expect = [1,2,3,4]
        record = {'test':[1,2,3,4]}
        self.couchcache.update_cache(query, record, expire)
        result = self.couchcache.get_from_cache(query)
        result = result['test']
        result.sort()
        self.assertEqual(expect, result)
#
# main
#
if __name__ == '__main__':
    unittest.main()
