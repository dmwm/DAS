#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS view manager
"""

import unittest
from DAS.core.das_viewmanager import DASViewManager
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger

class testDAS(unittest.TestCase):
    """
    A test class for the DASViewManager class
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
        self.db = 'test_views.db'
        config['filecache_db_engine'] = 'sqlite:///%s' % self.db
        self.view = DASViewManager(config)

    def tearDown(self):
        """Invoke after each test"""
        try:
            os.remove(self.db)
        except:
            pass

    def test_view1(self):                          
        """test DAS view creation"""
        expect = 'find dataset, count(file)'
        query  = expect + ' where bla'
        self.view.create('dataset', query)
        result = self.view.get('dataset')
        self.assertEqual(expect, result)

    def test_view2(self):                          
        """test DAS view get"""
        self.assertRaises(Exception, self.view.get, 'lll')

    def test_view3(self):                          
        """test DAS view update"""
        query0 = 'find dataset, count(file)'
        expect = 'find dataset, count(file), sum(file.size)'
        query  = expect + ' where bla'
        # first we should get exception that view doesn't exists
        self.assertRaises(Exception, self.view.update, 'dataset')

        self.view.create('dataset', query0)
        self.view.update('dataset', query)
        result = self.view.get('dataset')
        self.assertEqual(expect, result)


#
# main
#
if __name__ == '__main__':
    unittest.main()


