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
from DAS.core.das_mapping_db import DASMapping

class testDASMapping(unittest.TestCase):
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
        config['mapping_dbhost'] = 'localhost'
        config['mapping_dbport'] = 27017
        config['mapping_dbname'] = 'test_mapping'
        self.mgr = DASMapping(config)

    def tearDown(self):
        """Invoke after each test"""
        self.mgr.delete_db()

    def test_api(self):                          
        """test methods for api table"""
        self.mgr.create_db()

        apiversion = 'DBS_2_0_8'
        rec = {'system':'dbs', 
        'api': {'name':'listBlocks', 
                'params' : {'apiversion': apiversion,
                            'block_name':'*', 'storage_element_name':'*',
                            'user_type':'NORMAL'}},
         'daskeys': [
                {'key':'block', 'map':'block.name', 'pattern':''},
                ],
         'api2das': [
                {'api_param':'storage_element_name', 
                 'das_key':'site', 
                 'pattern':"re.compile('([a-zA-Z0-9]+\.){2}')"},
                {'api_param':'storage_element_name', 
                 'das_key':'site.se', 
                 'pattern':"re.compile('([a-zA-Z0-9]+\.){2}')"},
                {'api_param':'block_name', 
                 'das_key':'block', 
                 'pattern':""},
                {'api_param':'block_name', 
                 'das_key':'block.name', 
                 'pattern':""},
                ]
        } 
        self.mgr.add(rec)

        system = 'dbs'
        api = 'listBlocks'
        daskey = 'block'
        primkey = 'block.name'
        api_input = 'block_name'

        res = self.mgr.list_systems()
        self.assertEqual(['dbs'], res)

        res = self.mgr.list_apis()
        self.assertEqual([api], res)

        res = self.mgr.lookup_keys(system, daskey)
        self.assertEqual([primkey], res)

        value = ''
        res = self.mgr.das2api(system, daskey, value)
        self.assertEqual([api_input], res)

        res = self.mgr.api2das(system, api_input)
        self.assertEqual([daskey, primkey], res)

        # adding notations
        notations = {'system':system, 
            'notations':[
                    {'api_param':'storage_element_name', 'das_name':'se'},
                    {'api_param':'number_of_events', 'das_name':'nevents'},
                        ]
        }
        self.mgr.add(notations)

        res = self.mgr.notation2das(system, 'number_of_events')
        self.assertEqual('nevents', res)

        # API keys
        res = self.mgr.api2daskey(system, api)
        self.assertEqual([daskey], res)

        # build service map
        smap = {api: {'keys': ['block'], 'api': {'api': api}, 
                'params': {'storage_element_name': '*', 
                           'block_name': '*', 'user_type': 'NORMAL', 
                           'apiversion': 'DBS_2_0_8'}
                     }
        }
        res = self.mgr.servicemap(system, implementation='javaservlet')
        self.assertEqual(smap, res)
#
# main
#
if __name__ == '__main__':
    unittest.main()

