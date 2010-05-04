#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS filecache class
"""

import os
import unittest
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.core.das_filecache import DASFilecache

class testDASFilecache(unittest.TestCase):
    """
    A test class for the DAS filecache class
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug    = 0
        self.dir = os.path.join(os.getcwd(), 'testfilecache')
        if  os.path.isdir(self.dir):
            os.system('rm -rf %s' % self.dir)
        config   = das_readconfig()
        logger   = DASLogger(verbose=debug, stdout=debug)
        config['logger']  = logger
        config['verbose'] = debug
        config['filecache_dir'] = self.dir
        self.dasfilecache = DASFilecache(config)

    def test_result(self):                          
        """test DAS filecache result method"""
        query  = "find site where site=T2_UK"
        expire = 60
        expect = [1,2,3]
        self.dasfilecache.update_cache(query, expect, expire)
        result = self.dasfilecache.get_from_cache(query)
        result.sort()
        self.assertEqual(expect, result)
        os.system('rm -rf %s' % self.dir)
#
# main
#
if __name__ == '__main__':
    unittest.main()

