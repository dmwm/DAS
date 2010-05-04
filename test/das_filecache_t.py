#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS filecache class
"""

import os
import time
import unittest
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.core.das_filecache import DASFilecache
from DAS.core.das_filecache import next_number, clean_dirs
from DAS.core.das_filecache import create_dir, yyyymmdd, hour

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

    def test_create_dir(self):                          
        """test create_dir function"""
        topdir = '/tmp'
        system = 'das_test'
        try:
            clean_dirs(os.path.join(topdir, system))
            os.removedirs(os.path.join(topdir, system))
        except:
            pass

        base = '00'
        idir = create_dir(topdir, system, base=base, filesperdir=5)
        expect = '%s/%s/%s/%s/%s/%s' % \
                (topdir, system, yyyymmdd(), hour(), base, base)
        self.assertEqual(expect, idir)

        # create 5 files in our recent dir and check that new dir will be created
        for i in range(0, 6):
            fdesc = open(os.path.join(idir, str(i)), 'w')
            fdesc.write(str(i))
            fdesc.close()
        jdir = create_dir(topdir, system, base=base, filesperdir=5)
        expect2 = '%s/%s/%s/%s/%s/%s' % \
                (topdir, system, yyyymmdd(), hour(), base, next_number(base, base))
        self.assertEqual(expect2, jdir)
        for i in range(0, 6):
            os.remove(os.path.join(idir, str(i)))

        ndir = '%s/%s/%s/%s/%s' % (topdir, system, yyyymmdd(), hour(), base)
        clean_dirs(ndir)
        ndir = '%s/%s/%s/%s' % (topdir, system, yyyymmdd(), hour())
        clean_dirs(ndir)
        ndir = '%s/%s/%s' % (topdir, system, yyyymmdd())
        clean_dirs(ndir)
        ndir = '%s/%s' % (topdir, system)
        clean_dirs(ndir)
        os.removedirs(os.path.join(topdir, system))

    def test_next_number(self):                          
        """test next_number function"""
        base   = '000'
        expect = '001'
        result = next_number('000', base)
        self.assertEqual(expect, result)

        expect = '10'
        result = next_number('09')
        self.assertEqual(expect, result)

        self.assertRaises(Exception, next_number, '99')

    def test_result(self):                          
        """test DAS filecache result method"""
        query  = "find site where site=T2_UK"
        expire = 60
        expect = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        expect = self.dasfilecache.update_cache(query, expect, expire)
        expect = [i for i in expect]
        result = [i for i in self.dasfilecache.get_from_cache(query)]
        result.sort()
        self.assertEqual(expect, result)
        self.dasfilecache.delete_cache()

    def test_pagination(self):                          
        """test DAS filecache result method with pagination"""
        query  = "find site where site=T2_UK"
        expire = 60
        expect = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        expect = self.dasfilecache.update_cache(query, expect, expire)
        expect = [i for i in expect]
        idx    = 1
        limit  = 3
        result = [i for i in self.dasfilecache.get_from_cache(query, idx, limit)]
        result.sort()
        self.assertEqual(expect[idx:limit+1], result)
        self.dasfilecache.delete_cache()

    def test_incache(self):                          
        """test DAS filecache incache method"""
        query  = "find site where site=T2_UK"
        expire = 1
        expect = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        expect = self.dasfilecache.update_cache(query, expect, expire)
        expect = [i for i in expect]
        result = self.dasfilecache.incache(query)
        self.assertEqual(1, result)
        time.sleep(2)
        result = self.dasfilecache.incache(query)
        self.assertEqual(0, result)
        self.dasfilecache.delete_cache()
#
# main
#
if __name__ == '__main__':
    unittest.main()

