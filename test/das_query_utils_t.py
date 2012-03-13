#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS mongocache class
"""

import os
import re
import time
import unittest

from DAS.core.das_query import DASQuery
from DAS.utils.query_utils import encode_mongo_query, decode_mongo_query
from DAS.utils.query_utils import convert2pattern, compare_specs

class testDASMongocache(unittest.TestCase):
    """
    A test class for the DAS mongocache class
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug    = 0

    def test_encode_decode(self):
        """Test encode/decode_query functions"""
        query  = {'fields': None, 'spec': {'block.name':'aaa'}}
        result = encode_mongo_query(query)
        expect = decode_mongo_query(result)
        self.assertEqual(expect, query)

        query  = {'fields': ['block'], 'spec': {'block.size':{'$lt':10}}}
        result = encode_mongo_query(query)
        expect = decode_mongo_query(result)
        self.assertEqual(expect, query)

        pat = re.compile('/test.*')
        query  = {'fields': ['block'], 'spec': {'dataset.name': pat}}
        result = encode_mongo_query(query)
        expect = decode_mongo_query(result)
        self.assertEqual(expect['fields'], query['fields'])
        self.assertEqual(expect['spec']['dataset.name'].pattern,
                         query['spec']['dataset.name'].pattern)

    def test_compare_specs(self):
        """
        Test compare_specs funtion.
        """
        input = {'spec': {u'node': u'*', u'block': '*', u'se': u'*'}}
        exist = {'spec': {u'node': u'T1_CH_CERN', u'se': u'T1_CH_CERN', u'block': u'*'}}
        result = compare_specs(input, exist)
        self.assertEqual(False, result) # exist_query is a superset

        input_query = dict(fields=None, spec={'test':'T1_CH_CERN'})
        exist_query = dict(fields=['site'], spec={})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # exist_query is a superset

        input_query = dict(fields=None, spec={'test':'T1_CH_CERN'})
        exist_query = dict(fields=None, spec={'test':'T1_*'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(True, result) # exist_query is a superset

        input_query = dict(fields=None, spec={'test':'site'})
        exist_query = dict(fields=None, spec={'test':'site_ch'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # no pattern

        input_query = dict(fields=None, spec={'test':'site_ch'})
        exist_query = dict(fields=None, spec={'test':'site'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # no pattern

        input_query = dict(fields=None, spec={'test':'site*'})
        exist_query = dict(fields=None, spec={'test':'site_ch'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # exist_query is not a superset

        input_query = dict(fields=None, spec={'test':'site_ch'})
        exist_query = dict(fields=None, spec={'test':'site*'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(True, result) # exist_query is a superset

        input_query = dict(fields=['site','block'], spec={'test':'site*'})
        exist_query = dict(fields=['site'], spec={'test':'site_ch'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # exist_query is not a superset

        input_query = dict(fields=['site','block'], spec={'test':'site_ch'})
        exist_query = dict(fields=['site'], spec={'test':'site*'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # exist_query is a superset, but different fields

        input_query = dict(fields=['block'], spec={'test':'site*'})
        exist_query = dict(fields=['site'], spec={'test':'site*'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # different fields

        input_query = dict(fields=None, spec={'test':'T1_CH_*'})
        exist_query = dict(fields=None, spec={'test':'T1_CH_CERN'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # exist_query is not a superset 

        input_query = dict(fields=None, spec={'test':'T1_CH_CERN'})
        exist_query = dict(fields=None, spec={'test':'T1_CH_*'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(True, result) # exist_query is a superset

        input_query = dict(fields=None, spec={'test':{'$gt':10}})
        exist_query = dict(fields=None, spec={'test':{'$gt':11}})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # exist_query is not a superset 

        input_query = dict(fields=None, spec={'test':{'$gt':10}})
        exist_query = dict(fields=None, spec={'test':{'$gt':9}})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(True, result) # exist_query is a superset 

        input_query = dict(fields=None, spec={'test':{'$lt':10}})
        exist_query = dict(fields=None, spec={'test':{'$lt':9}})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # exist_query is not a superset 

        input_query = dict(fields=None, spec={'test':{'$lt':10}})
        exist_query = dict(fields=None, spec={'test':{'$lt':11}})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(True, result) # exist_query is a superset 

        input_query = dict(fields=None, spec={'test':{'$in':[1,2,3]}})
        exist_query = dict(fields=None, spec={'test':{'$in':[2,3]}})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # exist_query is not a superset 

        input_query = dict(fields=None, spec={'test':{'$in':[2,3]}})
        exist_query = dict(fields=None, spec={'test':{'$in':[1,2,3]}})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(True, result) # exist_query is a superset 

        input_query = dict(fields=None, spec={'test':'aaa'})
        exist_query = dict(fields=None, spec={'test':'aaa', 'test2':'bbb'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result) # different set of spec keys

        input_query = dict(fields=None, spec={'dataset.name':'/a/b/c*'})
        exist_query = dict(fields=['conf'], spec={'dataset.name':'/a/b/c'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result)

        input_query = dict(fields=None, spec={'dataset.name':'/a/b/c*'})
        exist_query = dict(fields=None, spec={'dataset.name':'/a/b/c'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(False, result)

        input_query = dict(fields=None, spec={'dataset.name':'/a/b/c'})
        exist_query = dict(fields=None, spec={'dataset.name':'/a/b/c*'})
        result = compare_specs(input_query, exist_query)
        self.assertEqual(True, result)

        input_query  = {'fields':None, 'spec':{'block.name':'ABCDEFG'}}
        exist_query  = {'fields':None, 'spec':{'block.name':'ABCDE*'}}
        result = compare_specs(input_query, exist_query)
        self.assertEqual(True, result)

        input_query = {'spec': {u'api':u'a', u'lfn':'test'}}
        exist_query = {'spec': {u'api':u'a', u'lfn':u'test'}}
        result = compare_specs(input_query, exist_query)
        self.assertEqual(True, result)

    def test_str_vs_unicode_compare_specs(self):
        """Test compare_specs with str/unicode dicts"""
        input_query = {u'fields': [u'zip'], 'spec': {u'zip.code': 10000}, u'filters': [u'zip.Placemark.address']}
        exist_query = {'fields': ['zip'], 'spec': {'zip.code': 10000}, 'filters': ['zip.Placemark.address']}
        result = compare_specs(input_query, exist_query)
        self.assertEqual(True, result)

    def test_compare_specs_with_patterns(self):
        """Test compare_specs with str/unicode dicts"""
        query1 = {'spec':{'dataset.name':'*Run201*RECO'}}
        query2 = {'spec':{'dataset.name':'*Run2011*RECO'}}
        query3 = {'spec':{'dataset.name':'*Run20*RECO'}}
        query4 = {'spec':{'dataset.name':'*Cosmics*Run20*RECO'}}
        query5 = {'spec':{'dataset.name':'*Run2011*PromptReco*RECO'}}

        result = compare_specs(query2, query1)
        self.assertEqual(True, result)

        result = compare_specs(query1, query3)
        self.assertEqual(True, result)

        result = compare_specs(query2, query3)
        self.assertEqual(True, result)

        result = compare_specs(query4, query3)
        self.assertEqual(True, result)

        result = compare_specs(query4, query1)
        self.assertEqual(False, result)

        result = compare_specs(query5, query2)
        self.assertEqual(True, result)

        query2 = {'spec':{'dataset.name':'*Run2011*RECO'}}
        query3 = {'spec':{'dataset.name':'*Run20*RECO*'}}
        query5 = {'spec':{'dataset.name':'*Run2011*PromptReco*RECO*'}}

        result = compare_specs(query5, query2)
        self.assertEqual(False, result)

        result = compare_specs(query5, query3)
        self.assertEqual(True, result)

        query6 = {'spec':{'dataset.name':'Run*RECO*RECO'}}
        query7 = {'spec':{'dataset.name':'Run*RECO1*RECO2'}}

        result = compare_specs(query7, query6)
        self.assertEqual(False, result)

        query8 = {'spec':{'dataset.name':'Run*0RECO*0RECO'}}
        query9 = {'spec':{'dataset.name':'Run*RECO*RECO*End'}}
        query10 = {'spec':{'dataset.name':'Run*1RECO*2RECO*3End'}}

        result = compare_specs(query8, query6)
        self.assertEqual(True, result)

        result = compare_specs(query10, query9)
        self.assertEqual(True, result)



    def test_convert2pattern(self):
        """
        Test how we convert mongo dict with patterns into spec
        with compiled patterns
        """
        spec   = {'test': 'city'}
        fields = None
        query  = dict(spec=spec, fields=fields)
        result, debug = convert2pattern(query)
        self.assertEqual(query, result)

        spec   = {'test': 'city*'}
        fields = None
        query  = dict(spec=spec, fields=fields)
        pat    = re.compile('^city.*')
        expect = dict(spec={'test': pat}, fields=fields)
        result, debug = convert2pattern(query)
        self.assertEqual(expect, result)
        expect = dict(spec={'test': '^city.*'}, fields=fields)
        self.assertEqual(expect, debug)

        spec   = {'test': {'name':'city*'}}
        fields = None
        query  = dict(spec=spec, fields=fields)
        pat    = re.compile('^city.*')
        expect = dict(spec={'test': {'name':pat}}, fields=fields)
        result, debug = convert2pattern(query)
        self.assertEqual(expect, result)
        expect = dict(spec={'test': {'name':'^city.*'}}, fields=fields)
        self.assertEqual(expect, debug)

        spec   = {"site.name": "T1_FR*"}
        fields = None
        query  = dict(spec=spec, fields=fields)
        pat    = re.compile('^T1_FR.*')
        expect = dict(spec={'site.name': pat}, fields=fields)
        result, debug = convert2pattern(query)
        self.assertEqual(expect, result)
        expect = dict(spec={'site.name':'^T1_FR.*'}, fields=fields)
        self.assertEqual(expect, debug)

        pat    = re.compile('^T1_FR.*')
        spec   = {"site.name": pat}
        fields = None
        query  = dict(spec=spec, fields=fields)
        expect = dict(spec={'site.name': pat}, fields=fields)
        result, debug = convert2pattern(query)
        self.assertEqual(expect, result)
        expect = dict(spec={'site.name':pat}, fields=fields)
        self.assertEqual(expect, debug)

#
# main
#
if __name__ == '__main__':
    unittest.main()

