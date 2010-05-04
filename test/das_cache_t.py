#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS cache module
"""

import unittest
from DAS.utils.utils import genkey
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.core.das_cache import DASCache

class testDASCache(unittest.TestCase):
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
        self.dascache = DASCache(config)
#        self.cachemgr = DASCacheMgr()

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
        """test DAS cache result method"""
        query  = "find site where site=T2_UK"
        expire = 60
        expect = [1,2,3,4]
        expect = self.dascache.update_cache(query, expect, expire)
        expect = [i for i in expect]
        if  not expect:
            assert expect == [1,2,3,4]
        result = [i for i in self.dascache.get_from_cache(query)]
        result.sort()
        from itertools import groupby
        result = [k for k, g in groupby(result)]
        self.assertEqual(expect, result)
        self.dascache.delete_cache()

#    def test_cachemgr(self):
#        """test DASCacheMgr class functionality"""
#        cmgr = self.cachemgr
#        def worker(item):
#            """test worker"""
#            print item
#        thread.start_new_thread(cmgr.worker, (worker, ))
#        for i in range(0, 10):
#            query = 'find site where site=%s' % i
#            cmgr.add(query, 10)
#        time.sleep(2)
#        for i in range(11, 20):
#            query = 'find site where site=%s' % i
#            cmgr.add(query, 10)
#        time.sleep(10)
#        print "the end"
#
# main
#
if __name__ == '__main__':
    unittest.main()
