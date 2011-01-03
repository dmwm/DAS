#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS mongocache class
"""

import os
import re
import time
import unittest

from pymongo.connection import Connection

from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.core.das_mongocache import DASMongocache, loose
from DAS.core.das_mongocache import encode_mongo_query, decode_mongo_query
from DAS.core.das_mongocache import update_item, convert2pattern, compare_specs

class testDASMongocache(unittest.TestCase):
    """
    A test class for the DAS mongocache class
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug    = 0
        config   = das_readconfig()
        logger   = DASLogger(verbose=debug)
        config['logger']  = logger
        config['verbose'] = debug
        dburi    = config['mongodb']['dburi']

        connection = Connection(dburi)
        connection.drop_database('das') 
        self.dasmongocache = DASMongocache(config)

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

    def test_loose(self):
        """Test loose function"""
        query  = {'fields': ['block'], 'spec': {}}
        result = loose(query)
        expect = query
        self.assertEqual(expect, result)

    def test_update_item(self):
        """test update_item method"""
        expect = {'test':1, 'block' : {'name' : '/a/b/c#123'}}

        row = {'test':1}
        key = 'block.name'
        val = '/a/b/c#123'
        update_item(row, key, val)
        self.assertEqual(expect, row)

        row = {'test':1}
        key = 'block.name'
        val = {'$gte' : '/a/b/c#123'}
        update_item(row, key, val)
        # upon update we create a list of values for given key: block.name
        expect = {'test':1, 'block' : {'name' : ['/a/b/c#123']}}
        self.assertEqual(expect, row)

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
        self.assertEqual(True, result) # exist_query is a superset

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
        self.assertEqual(True, result) # exist_query is a superset

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
        self.assertEqual(True, result)

        input_query  = {'fields':None, 'spec':{'block.name':'ABCDEFG'}}
        exist_query  = {'fields':None, 'spec':{'block.name':'ABCDE*'}}
        result = compare_specs(input_query, exist_query)
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

    def test_similar_queries(self):                          
        """test similar_queries method of DASMongoCache"""
        query  = {'fields':None, 'spec':{'dataset.name':'ABC'}}
        self.dasmongocache.col.insert({"query":encode_mongo_query(query)})
        query  = {'fields':['dataset'], 'spec':{'dataset.name':'ABC'}}
        result = self.dasmongocache.similar_queries(query)
        self.assertEqual(True, result)
        self.dasmongocache.delete_cache()

        query  = {'fields':None, 'spec':{'block.name':'ABCDE*'}}
        self.dasmongocache.col.insert({"query":encode_mongo_query(query)})
        query  = {'fields':None, 'spec':{'block.name':'ABCDEFG'}}
        result = self.dasmongocache.similar_queries(query)
        self.assertEqual(True, result)
        self.dasmongocache.delete_cache()

        query  = {'fields':None, 'spec':{'block.name':'ABCDEFG'}}
        self.dasmongocache.col.insert({"query":encode_mongo_query(query)})
        query  = {'fields':None, 'spec':{'block.name':'ABCDE*'}}
        result = self.dasmongocache.similar_queries(query)
        self.assertEqual(False, result)
        self.dasmongocache.delete_cache()

        query  = {'fields':None, 'spec':{'run.number':20853}}
        self.dasmongocache.col.insert({"query":encode_mongo_query(query)})
        query  = {'fields':None, 'spec':{'run.number': {'$gte':20853, '$lte':20859}}}
        result = self.dasmongocache.similar_queries(query)
        self.assertEqual(False, result)
        self.dasmongocache.delete_cache()

        query  = {'fields':None, 'spec':{'run.number': {'$gte':20853, '$lte':20859}}}
        self.dasmongocache.col.insert({"query":encode_mongo_query(query)})
        query  = {'fields':None, 'spec':{'run.number':20853}}
        result = self.dasmongocache.similar_queries(query)
        self.assertEqual(True, result)
        self.dasmongocache.delete_cache()

#    def test_result(self):                          
#        """test DAS mongocache result method"""
#        query  = "find site where site=T3_CU"
#        data   = {'system':'sitedb', 'site':'T3_CU', 'se': 'www.cornell.edu'}
#        expect = [dict(data)]
#        expire = 60
#        res = self.dasmongocache.update_cache(query, data, expire)
#        for i in res: # update_cache is generator
#            pass
#        result = [i for i in self.dasmongocache.get_from_cache(query)]
#        result.sort()
#        self.assertEqual(expect, result)
#        self.dasmongocache.delete_cache()






#    def test_pagination(self):                          
#        """test DAS mongocache result method with pagination"""
#        query  = "find site where site=T2_UK"
#        expire = 60
#        expect = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
#        expect = self.dasmongocache.update_cache(query, expect, expire)
#        expect = [i for i in expect]
#        idx    = 1
#        limit  = 3
#        result = [i for i in self.dasmongocache.get_from_cache(query, idx, limit)]
#        result.sort()
#        self.assertEqual(expect[idx:limit+1], result)
#        self.dasmongocache.delete_cache()

#    def test_sorting(self):                          
#        """test DAS mongocache result method with sorting"""
#        query  = "find site where site=T2_UK"
#        expire = 60
#        data = [
#            {'id':0, 'data':'a', 'run':1},
#            {'id':1, 'data':'b', 'run':3},
#            {'id':2, 'data':'c', 'run':2},
#        ]
#        gen = self.dasmongocache.update_cache(query, data, expire)
#        res = [i for i in gen]
#        skey = 'run'
#        order = 'desc'
#        result = [i for i in \
#            self.dasmongocache.get_from_cache(query, skey=skey, order=order)]
#        expect = [
#            {'id':1, 'data':'b', 'run':3},
#            {'id':2, 'data':'c', 'run':2},
#            {'id':0, 'data':'a', 'run':1},
#        ]
#        self.assertEqual(expect, result)
#        skey = 'run'
#        order = 'asc'
#        result = [i for i in \
#            self.dasmongocache.get_from_cache(query, skey=skey, order=order)]
#        expect = [
#            {'id':0, 'data':'a', 'run':1},
#            {'id':2, 'data':'c', 'run':2},
#            {'id':1, 'data':'b', 'run':3},
#        ]
#        self.assertEqual(expect, result)
#        self.dasmongocache.delete_cache()

#    def test_incache(self):                          
#        """test DAS mongocache incache method"""
#        query  = "find site where site=T2_UK"
#        expire = 1
#        expect = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
#        expect = self.dasmongocache.update_cache(query, expect, expire)
#        expect = [i for i in expect]
#        result = self.dasmongocache.incache(query)
#        self.assertEqual(1, result)
#        time.sleep(2)
#        result = self.dasmongocache.incache(query)
#        self.assertEqual(0, result)
#        self.dasmongocache.delete_cache()
#
# main
#
if __name__ == '__main__':
    unittest.main()

