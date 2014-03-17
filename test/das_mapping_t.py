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
from DAS.core.das_mapping_db import DASMapping, verification_token

from pymongo import MongoClient

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
        conn = MongoClient(dburi)
        conn.drop_database(dbname)
        self.coll = conn[dbname][collname]
        self.pmap = {"presentation": {"block":[{"ui": "Block name", "das": "block.name"},
            {"ui": "Block size", "das": "block.size"}]}, "type": "presentation"}
        self.coll.insert(self.pmap)

        url     = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/acquisitioneras/'
        dformat = 'JSON'
        system  = 'dbs3'
        expire  = 100
        rec = {'system':system, 'urn': 'acquisitioneras', 'format':dformat,
            'instances': ['prod/global'],
            'url':url, 'expire': expire, 'lookup': 'era',
            'params' : {},
             'das_map': [
                 {"das_key": "era", "rec_key":"era.name", "api_arg":"era"}
                 ],
             'type': 'service'
        }
        self.coll.insert(rec)

        ver_token = verification_token(self.coll.find(exhaust=True))
        rec = {'verification_token':ver_token, 'type':'verification_token'}
        self.coll.insert(rec)

        self.mgr = DASMapping(config)

    def tearDown(self):
        """Invoke after each test"""
        self.mgr.delete_db()

    def test_api(self):
        """test methods for api table"""
        self.mgr.delete_db()
        self.mgr.init()

        system  = 'dbs3'
        url     = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        dformat = 'JSON'
        expire  = 100
        instances = ["prod/global", "prod/phys01"]

        api = 'primarydatasets'
        params = {"primary_ds_name":"*"}
        rec = {'system':system, 'urn':api, 'format':dformat, 'url':url,
            'instances': instances,
            'params': params, 'expire':expire, 'lookup': 'primary_dataset', 'wild_card':'*',
            'das_map' : [dict(das_key='primary_dataset',
                              rec_key='primary_dataset.name',
                              api_arg='primary_dataset')],
            'type': 'service'
        }
        self.mgr.add(rec)
        smap = {api: {'url':url, 'expire':expire, 'keys': ['primary_dataset'],
                'format': dformat, 'wild_card':'*', 'cert':None, 'ckey': None,
                'services': '', 'lookup': 'primary_dataset',
                'params': params }
        }

        rec = {'system':system, 'urn': 'datasetaccesstypes', 'format':dformat,
            'instances': instances,
            'url':url, 'expire': expire, 'lookup': 'status',
            'params' : {'status':'*'},
             'das_map': [
                 {"das_key": "status", "rec_key":"status.name", "api_arg":"status"}
                 ],
             'type': 'service'
        }
        self.mgr.add(rec)


        api = 'datasetaccesstypes'
        daskey = 'status'
        rec_key = 'status.name'
        api_input = 'status'

        res = self.mgr.list_systems()
        self.assertEqual([system], res)

        res = self.mgr.list_apis()
        res.sort()
        self.assertEqual(['datasetaccesstypes', 'primarydatasets'], res)

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
        notations = {'system':system, 'type': 'notation',
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
        self.assertEqual(['status'], res)

        # build service map
        smap.update({api: {'url':url, 'expire':expire, 'cert':None, 'ckey': None,
                'keys': ['status'], 'format':dformat, 'wild_card':'*',
                'services': '', 'lookup': daskey,
                'params': {"status": "*"}
                     }
        })
        res = self.mgr.servicemap(system)
        self.assertEqual(smap, res)

    def test_presentation(self):
        """test presentation method"""
        self.mgr.init()
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
        ], "system": system, "type": "notation"}
        self.mgr.add(rec)
        expect = rec['notations']
        result = self.mgr.notations(system)[system]
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()

