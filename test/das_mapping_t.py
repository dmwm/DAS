#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS mappingdb class
"""

import os
import time
import unittest
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import PrintManager
from DAS.utils.utils import deepcopy
from DAS.core.das_mapping_db import DASMapping

from pymongo.connection import Connection

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
        config   = deepcopy(das_readconfig())
        dburi    = config['mongodb']['dburi']
        logger   = PrintManager('TestDASMapping', verbose=debug)
        config['logger']  = logger
        config['verbose'] = debug
        dbname   = 'test_mapping'
        collname = 'db'
        config['mappingdb'] = dict(dburi=dburi, dbname=dbname, collname=collname)
        # add some maps to mapping db
        conn = Connection(dburi)
        conn.drop_database(dbname)
        self.coll = conn[dbname][collname]
        self.pmap = {"presentation": {"block":[{"ui": "Block name", "das": "block.name"},
        {"ui": "Block size", "das": "block.size"}]}}
        self.coll.insert(self.pmap)

        self.mgr = DASMapping(config)

    def tearDown(self):
        """Invoke after each test"""
        self.mgr.delete_db()

    def test_api(self):
        """test methods for api table"""
        self.mgr.delete_db()
        self.mgr.init()

        apiversion = 'DBS_2_0_8'
        url     = 'http://a.com'
        dformat = 'JSON'
        expire  = 100

        api = 'listRuns'
        params = { 'apiversion':apiversion, 'path' : 'required', 'api':api}
        rec = {'system':'dbs', 'urn':api, 'format':dformat, 'url':url,
            'params': params, 'expire':expire, 'lookup': 'run', 'wild_card':'*',
            'das_map' : [dict(das_key='run', rec_key='run.run_number', api_arg='path')],
        }
        self.mgr.add(rec)
        smap = {api: {'url':url, 'expire':expire, 'keys': ['run'],
                'format': dformat, 'wild_card':'*', 'cert':None, 'ckey': None,
                'services': '', 'lookup': 'run',
                'params': {'path': 'required', 'api': api,
                           'apiversion': 'DBS_2_0_8'}
                     }
        }

        rec = {'system':'dbs', 'urn': 'listBlocks', 'format':dformat,
            'url':url, 'expire': expire, 'lookup': 'block',
            'params' : {'apiversion': apiversion, 'api': 'listBlocks',
                        'block_name':'*', 'storage_element_name':'*',
                        'user_type':'NORMAL'},
             'das_map': [
                 {'das_key':'block', 'rec_key':'block.name', 'api_arg':'block_name'},
                 {'das_key':'site', 'rec_key':'site.se', 'api_arg':'storage_element_name',
                  'pattern':"re.compile('([a-zA-Z0-9]+\.){2}')"},
                 ],
        }
        self.mgr.add(rec)


        system = 'dbs'
        api = 'listBlocks'
        daskey = 'block'
        rec_key = 'block.name'
        api_input = 'block_name'

        res = self.mgr.list_systems()
        self.assertEqual(['dbs'], res)

        res = self.mgr.list_apis()
#        self.assertEqual([api], res)
        res.sort()
        self.assertEqual(['listBlocks', 'listRuns'], res)

        res = self.mgr.lookup_keys(system, api, daskey)
        self.assertEqual([rec_key], res)

        value = ''
        res = self.mgr.das2api(system, api, rec_key, value)
        self.assertEqual([api_input], res)

        # adding another params which default is None
        res = self.mgr.das2api(system, api, rec_key, value)
        self.assertEqual([api_input], res)

        res = self.mgr.api2das(system, api_input)
        self.assertEqual([daskey], res)

        # adding notations
        notations = {'system':system,
            'notations':[
                    {'api_output':'storage_element_name', 'rec_key':'se', 'api':''},
                    {'api_output':'number_of_events', 'rec_key':'nevents', 'api':''},
                        ]
        }
        self.mgr.add(notations)

        res = self.mgr.notation2das(system, 'number_of_events')
        self.assertEqual('nevents', res)

        # API keys
        res = self.mgr.api2daskey(system, api)
        self.assertEqual(['block', 'site'], res)

        # build service map
        smap.update({api: {'url':url, 'expire':expire, 'cert':None, 'ckey': None,
                'keys': ['block', 'site'], 'format':dformat, 'wild_card':'*',
                'services': '', 'lookup': daskey,
                'params': {'storage_element_name': '*', 'api':api,
                           'block_name': '*', 'user_type': 'NORMAL',
                           'apiversion': 'DBS_2_0_8'}
                     }
        })
        res = self.mgr.servicemap(system)
        self.assertEqual(smap, res)

    def test_presentation(self):
        """test presentation method"""
        self.mgr.init()
        self.coll.insert(self.pmap)
        expect = self.pmap['presentation']['block']
        result = self.mgr.presentation('block')
        self.assertEqual(expect, result)

    def test_notations(self):
        """test notations method"""
        self.mgr.init()
        system = "test"
        rec = {'notations': [
        {"api_output": "site.resource_element.cms_name", "rec_key": "site.name", "api": ""},
        {"api_output": "site.resource_pledge.cms_name", "rec_key": "site.name", "api": ""},
        {"api_output": "admin.contacts.cms_name", "rec_key":"site.name", "api":""}
        ], "system": system}
        self.mgr.add(rec)
        expect = rec['notations']
        result = self.mgr.notations(system)[system]
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()

