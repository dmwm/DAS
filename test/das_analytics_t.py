#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS analyticsdb class
"""

import os
import time
import unittest
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import PrintManager
from DAS.utils.utils import deepcopy
from DAS.core.das_analytics_db import DASAnalytics

class testDASAnalytics(unittest.TestCase):
    """
    A test class for the DAS analyticsdb class
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug    = 0
        self.db  = 'test_analytics.db'
        config   = deepcopy(das_readconfig())
        dburi    = config['mongodb']['dburi']
        logger   = PrintManager('TestDASAnalytics', verbose=debug)
        config['logger']  = logger
        config['verbose'] = debug
        config['analyticsdb'] = dict(dburi=dburi, history=5184000,
                dbname='test_analytics', collname='db')
        self.mgr = DASAnalytics(config)

    def tearDown(self):
        """Invoke after each test"""
        self.mgr.delete_db()

    def test_api(self):                          
        """test methods for api table"""
        self.mgr.delete_db()
        self.mgr.create_db()

        query = 'find block'
        dbs_api = 'listBlocks'
        dbs_params = {'apiversion':'DBS_2_0_8',
                  'block_name':'*', 'storage_element_name':'*',
                  'user_type':'NORMAL'}
        self.mgr.add_api('dbs', query, dbs_api, dbs_params)

        phedex_api = 'blockReplicas'
        phedex_params = {'node': '*', 'se': '*', 'block': '*'}
        self.mgr.add_api('phedex', query, phedex_api, phedex_params)

        res = self.mgr.list_systems()
        res.sort()
        self.assertEqual(['dbs', 'phedex'], res)

        res = self.mgr.list_apis('dbs')
        self.assertEqual([dbs_api], res)

        self.mgr.add_api('dbs', query, dbs_api, dbs_params)
        res = self.mgr.api_counter(dbs_api)
        self.assertEqual(1, res) # we invoke API twice, so should get 2

        self.mgr.update('dbs', query)
        res = self.mgr.api_counter(dbs_api)
        self.assertEqual(2, res) # we invoke API twice, so should get 2
        
        res = self.mgr.api_params(phedex_api)
        self.assertEqual([phedex_params], res)
#
# main
#
if __name__ == '__main__':
    unittest.main()

