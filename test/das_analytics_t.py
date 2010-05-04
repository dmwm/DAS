#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS analyticsdb class
"""

import os
import time
import unittest
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.core.das_analytics_db import DASAnalytics

class testDASAnalyticsMgr(unittest.TestCase):
    """
    A test class for the DAS analyticsdb class
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug    = 0
        self.db  = 'test_analytics.db'
        config   = das_readconfig()
        logger   = DASLogger(verbose=debug, stdout=debug)
        config['logger']  = logger
        config['verbose'] = debug
        config['analytics_db_engine'] = 'sqlite:///%s' % self.db
        self.mgr = DASAnalytics(config)

    def tearDown(self):
        """Invoke after each test"""
        try:
            os.remove(self.db)
        except:
            pass

    def test_system(self):                          
        """test methods for system table"""
        self.mgr.create_db()
        self.mgr.add_system('dbs')
        res = self.mgr.list_systems()
        self.assertEqual(['dbs'], res)
        res = self.mgr.delete_system('dbs')
        self.assertEqual(True, res)

    def test_api(self):                          
        """test methods for api table"""
        self.mgr.create_db()
        system = 'dbs'
        self.mgr.add_system(system)

        api = 'listBlocks'
        params = {'apiversion':'DBS_2_0_8',
                  'block_name':'*', 'storage_element_name':'*',
                  'user_type':'NORMAL'}
        self.mgr.add_api(system, api, params)

        res = self.mgr.list_systems()
        self.assertEqual([system], res)

        res = self.mgr.list_apis(system)
        self.assertEqual([api], res)

        self.mgr.add_api(system, api, params)
        res = self.mgr.api_counter(api)
        self.assertEqual(2, res) # we invoke API twice, so should get 2

        res = self.mgr.api_params(api)
        self.assertEqual(params, res)

        res = self.mgr.delete_api(api)
        self.assertEqual(True, res)

        res = self.mgr.delete_system(system)
        self.assertEqual(True, res)

#
# main
#
if __name__ == '__main__':
    unittest.main()

