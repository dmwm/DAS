#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS mappingdb class
"""

import os
import time
import unittest
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.core.das_mapping import DASMapping

class testDASMappingMgr(unittest.TestCase):
    """
    A test class for the DAS mappingdb class
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug    = 0
        self.db  = 'test_mapping.db'
        config   = das_readconfig()
        logger   = DASLogger(verbose=debug, stdout=debug)
        config['logger']  = logger
        config['verbose'] = debug
        config['mapping_db_engine'] = 'sqlite:///%s' % self.db
        self.mgr = DASMapping(config)

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
        daskeys = dict(block='block.name')
        api2das = [
                   ('block_name', 'block.name', ''),
                   ('storage_element_name', 'block.site.se', ''),
                  ]
        self.mgr.add_api(system, api, params, daskeys, api2das)

        res = self.mgr.list_systems()
        self.assertEqual([system], res)

        res = self.mgr.list_apis(system)
        self.assertEqual([api], res)

        res = self.mgr.list_daskeys(system)
        self.assertEqual(['block', 'block.name', 'block.site.se'], res)

        res = self.mgr.api_keys(api)
        self.assertEqual(['block', 'block.name', 'block.site.se'], res)

        res = self.mgr.api_params(api)
        self.assertEqual(params, res)

        res = self.mgr.delete_api(api)
        self.assertEqual(True, res)

        res = self.mgr.delete_system(system)
        self.assertEqual(True, res)

    def test_methods(self):                          
        """test methods for api table"""
        self.mgr.create_db()
        system = 'dbs'
        self.mgr.add_system(system)

        api = 'listBlocks'
        params = {'apiversion':'DBS_2_0_8',
                  'block_name':'*', 'storage_element_name':'*',
                  'user_type':'NORMAL'}
        daskeys = dict(block='block.name')
        api2das = [('block_name', 'block', ''),
                   ('block_name', 'block.name', ''),
                   ('storage_element_name', 'site', ''),
                   ('storage_element_name', 'site.se', ''),
                  ]
        self.mgr.add_api(system, api, params, daskeys, api2das)

        daskey = 'block'
        primkey = 'block.name'
        api_input = 'block_name'
        res = self.mgr.lookup_key(system, daskey)
        self.assertEqual(primkey, res)

        value = ''
        res = self.mgr.das2api(system, daskey, value)
        self.assertEqual([api_input], res)

        res = self.mgr.api2das(system, api_input)
        self.assertEqual([daskey, primkey], res)

        api_param = 'number_of_events'
        das_param = 'nevents'
        self.mgr.add_notation(system, api_param, das_param)
        res = self.mgr.notation2das(system, api_param)
        self.assertEqual(das_param, res)

        res = self.mgr.delete_api(api)
        self.assertEqual(True, res)

        res = self.mgr.delete_system(system)
        self.assertEqual(True, res)

#
# main
#
if __name__ == '__main__':
    unittest.main()

