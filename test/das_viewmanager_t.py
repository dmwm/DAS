#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS view manager
"""

import os
import unittest
from DAS.core.das_viewmanager import DASViewManager, strip_query
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
        self.db  = 'test_views.db'
        self.tearDown()
        debug    = 0
        config   = das_readconfig()
        logger   = DASLogger(verbose=debug, stdout=debug)
        config['logger']  = logger
        config['verbose'] = debug
        config['views_engine'] = 'sqlite:///%s' % self.db
        self.view = DASViewManager(config)

    def tearDown(self):
        """Invoke after each test"""
        try:
            os.remove(self.db)
        except:
            pass

    def test_view1(self):                          
        """test view creation"""
        expect = 'find dataset, count(file)'
        query  = expect + ' where bla'
        self.view.create('dataset', query)
        result = self.view.get('dataset')
        self.assertEqual(expect, result)

    def test_view2(self):                          
        """test view get"""
        self.assertRaises(Exception, self.view.get, 'lll')

    def test_view3(self):                          
        """test to check that we add the same view twice"""
        query = 'find dataset, count(file), run'
        self.assertRaises(Exception, self.view.create, ('dataset', query))

    def test_view4(self):
        """test view update"""
        query0 = 'find dataset, count(file), run'
        expect = 'find dataset, count(file), sum(file.size)'
        query  = expect + ' where bla'
        # first we should get exception that view doesn't exists
        self.assertRaises(Exception, self.view.update, 'dataset')

        self.view.create('dataset_test', query0)
        self.view.update('dataset_test', query)
        result = self.view.get('dataset_test')
        self.assertEqual(expect, result)

    def test_view5(self):
        """test to checkout all views"""
        query1 = u'find dataset, count(file) where bla'
        self.view.create('dataset1', query1)
        query2 = u'find dataset, count(file), sum(file) where bla'
        self.view.create('dataset2', query2)
        result = self.view.all()
        expect = {u'dataset1': strip_query(query1), 
                  u'dataset2': strip_query(query2)}
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()


