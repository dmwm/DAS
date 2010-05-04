#!/usr/bin/env python
#pylint: disable-msg=c0301,c0103

"""
unit test for DAS mapping set of tools
"""

import unittest
from DAS.core.das_mapping import translate, jsonparser4key, json2das
from DAS.core.das_mapping import das2api, result2das

class testDASMapping(unittest.TestCase):
    """
    A test class for the DAS mapping
    """
    def test_mapconfig(self): 
        """test DAS translate routine"""
        result = das2api('phedex', 'site')
        expect = ['se']
        self.assertEqual(expect, result)

    def test_jsonparser4key(self): 
        """test DAS jsonparser routine"""
        rep1 = {'bytes':1, 'files':1, 'se':'T2'}
        rep2 = {'bytes':1, 'files':1, 'se':'T3'}
        rec1 = {'bytes':1, 'files':1, 'name':'ABC', 'replica':rep1}
        rec2 = {'bytes':2, 'files':2, 'name':'CDE', 'replica':[rep1, rep2]}
        jsondict = {'service': {'block': [rec1, rec2]},'timestamp':123}
        result = [i for i in jsonparser4key(jsondict, 'block.name')]
        expect = ['ABC', 'CDE']
        self.assertEqual(expect, result)

        result = [i for i in jsonparser4key(jsondict, 'block.bytes')]
        expect = [1, 2]
        self.assertEqual(expect, result)

        result = [i for i in jsonparser4key(jsondict, 'block.replica.se')]
        expect = ['T2', ['T2', 'T3']]
        self.assertEqual(expect, result)

    def test_json2das_sitedb(self): 
        """test DAS json2das routine with site db data"""
        jsondict = {'0': {'name': 'cmsrm-se01.roma1.infn.it'}}
        keylist  = ['admin', 'name']
        selkeys  = ['admin', 'site']
        result   = [i for i in json2das('sitedb', jsondict, keylist, selkeys)]
        result.sort()
        # NOTE: sitedb uses site for many purposes, in this case
        # the name maps to site.cename, site.sename, site.cmsname, site
        expect   = [{'admin':'', 'site': 'cmsrm-se01.roma1.infn.it'}]
        expect.sort()
        self.assertEqual(expect, result)

    def test_json2das_sitedb_admin(self): 
        """test DAS json2das routine with site db data for admins"""
        jsondict = {'admin': [{'title': 'Data Manager', 'surname': 'Rahatlou', 
                          'email': 'Shahram.Rahatlou@cern.ch', 'forename': 'Shahram'}, 
                    {'title': 'Data Manager', 'surname': 'M.Barone (Red Baron)', 
                          'email': 'luciano.barone@cern.ch', 'forename': 'Luciano'}]}
        keylist  = ['admin', 'name']
        selkeys  = ['admin', 'site']
        result   = [i for i in json2das('sitedb', jsondict, keylist, selkeys)]
        result.sort()
        expect = [{'admin': {'forename': 'Shahram', 'surname': 'Rahatlou', 'email': 'Shahram.Rahatlou@cern.ch', 'title': 'Data Manager'}, 'site':''}, {'admin': {'forename': 'Luciano', 'surname': 'M.Barone (Red Baron)', 'email': 'luciano.barone@cern.ch', 'title': 'Data Manager'}, 'site': ''}]
        expect.sort()
        self.assertEqual(expect, result)

    def test_json2das(self): 
        """test DAS json2das routine"""
        rep1 = {'bytes':1, 'files':1, 'se':'T2'}
        rep2 = {'bytes':1, 'files':1, 'se':'T3'}
        rec1 = {'bytes':1, 'files':1, 'name':'ABC', 'replica':rep1}
        rec2 = {'bytes':2, 'files':2, 'name':'CDE', 'replica':[rep1, rep2]}
        jsondict = {'service': {'block': [rec1, rec2]},'timestamp':123}
        keylist = ['block.name','block.bytes']
        selkeys = ['block', 'block.size']
        result = [i for i in json2das('phedex', jsondict, keylist, selkeys)]
        result.sort()
        expect = [{'block': 'ABC', 'block.size': 1}, 
                  {'block': 'CDE', 'block.size': 2}]
        expect.sort()
        self.assertEqual(expect, result)

        keylist = ['block.name','block.replica.se']
        selkeys = ['block', 'site']
        result = [i for i in json2das('phedex', jsondict, keylist, selkeys)]
        result.sort()
        expect = [{'block': 'ABC', 'site': 'T2'}, 
                  {'block': 'CDE', 'site': ['T2', 'T3']},
                 ]
        expect.sort()
        self.assertEqual(expect, result)

#        json={'phedex': {'request_date': '2009-04-15 19:18:02 UTC', 'request_timestamp': 1239823082.5613501, 'call_time': '0.00937', 'instance': 'prod', 'request_call': 'blockReplicas', 'request_url': 'http://cmsweb.cern.ch:7001/phedex/datasvc/json/prod/blockReplicas', 'request_version': '1.3.1', 'block': [{'files': '4', 'name': '/Cosmics/CRUZET4_v1_CRZT210_V1_SuperPointing_v1/RECO#96c1b23f-1d88-4aa5-96ed-966a73a38c2d', 'bytes': '291825111', 'replica': [{'files': '4', 'node': 'T1_US_FNAL_Buffer', 'group': 'null', 'complete': 'y', 'time_create': '1219412876.0661', 'custodial': 'n', 'bytes': '291825111', 'time_update': '1229179156.51997', 'node_id': '9', 'se': 'cmssrm.fnal.gov'}, {'files': '4', 'node': 'T1_US_FNAL_MSS', 'group': 'null', 'complete': 'y', 'time_create': '1219413379.70068', 'custodial': 'y', 'bytes': '291825111', 'time_update': '1229179156.51997', 'node_id': '10', 'se': 'cmssrm.fnal.gov'}, {'files': '4', 'node': 'T2_CH_CAF', 'group': 'null', 'complete': 'y', 'time_create': '1219413630.40003', 'custodial': 'n', 'bytes': '291825111', 'time_update': '1229179156.51997', 'node_id': '501', 'se': 'caf.cern.ch'}], 'is_open': 'n', 'id': '426474'}]}}
#        keylist = ['block.replica.complete', 'block.name', 'block.replica.se']
#        selkeys = ['block.complete', 'block', 'site']
#        result = json2das('phedex', json, keylist, selkeys)
#        expect = [1]
#        self.assertEqual(expect, result)
#
# main
#
if __name__ == '__main__':
    unittest.main()
