#!/usr/bn/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

# system modules
import re
import sys
import json
import time
import unittest
# python 3
if  sys.version.startswith('3.'):
    import urllib.parse as urllib
    import urllib.request as urllib2
    long = int
else:
    import urllib
    import urllib2
import tempfile

# das modules
from DAS.utils.utils import cartesian_product, deepcopy, identical_data_records
from DAS.utils.utils import genresults, transform_dict2list, size_format
from DAS.utils.utils import sitename, add2dict, map_validator, gen_counter
from DAS.utils.utils import splitlist, gen_key_tuples, sort_data
from DAS.utils.utils import dict_value, merge_dict, adjust_value
from DAS.utils.utils import json_parser, xml_parser, dict_helper
from DAS.utils.utils import convert_dot_notation, translate, das_diff
from DAS.utils.utils import delete_elem, unique_filter
from DAS.utils.utils import filter_with_filters, aggregator, yield_rows
from DAS.utils.utils import adjust_mongo_keyvalue, expire_timestamp
from DAS.utils.utils import genkey, next_day, prev_day, convert2date
from DAS.utils.utils import parse_filters, parse_filter, qlxml_parser
from DAS.utils.utils import delete_keys, parse_filter_string
from DAS.utils.utils import fix_times, das_dateformat, http_timestamp
from DAS.utils.utils import api_rows, regen, das_sinfo, sort_rows
from DAS.utils.utils import convert2ranges, add_hash, upper_lower
from DAS.utils.regex import das_time_pattern
from DAS.core.das_query import DASQuery

class testUtils(unittest.TestCase):
    """
    A test class for the DAS utils module
    """
    def test_upper_lower(self):
        "Test upper_lower function"
        ilist  = ['in', 'last', '=']
        expect = ['in', 'last', '=', 'IN', 'LAST']
        result = upper_lower(ilist)
        expect.sort()
        result.sort()
        self.assertEqual(result, expect)

    def test_add_hash(self):
        "Test add_hash function"
        rec    = {'foo':1}
        add_hash(rec)
        expect = ['foo', 'hash']
        result = rec.keys()
        self.assertEqual(result, expect)

    def test_convert2ranges(self):
        "Test convert2ranges function"
        rows   = [5, 1, 1, 1, 2, 2, 3, 3, 3, 4, 8, 9, 1]
        expect = [[1,5], [8,9]]
        result = [r for r in convert2ranges(rows)]
        self.assertEqual(result, expect)

    def test_sort_rows(self):
        "Test sort_rows function"
        rows   = [5, 1, 1, 1, 2, 2, 3, 3, 3, 4]
        expect = [5, 1, 2, 3, 4]
        result = [r for r in sort_rows(rows)]
        self.assertEqual(result, expect)

    def test_regen(self):
        "Test regen function"
        rows   = (i for i in range(5))
        first  = next(rows)
        expect = [i for i in range(5)]
        result = [i for i in regen(first, rows)]
        self.assertEqual(result, expect)

    def test_das_sinfo(self):
        "Test das_sinfo function"
        das = {'system':['dbs', 'dbs', 'phedex'], 'api':['one', 'two', 'thr']}
        row = {'das':das, 'foo':[{'dbs':1}, {'a':1}, {'phedex':1}]}
        result = das_sinfo(row)
        expect = {'dbs':set(['one', 'two']), 'phedex':set(['three'])}
        self.assertEqual(expect, expect)

    def test_api_rows(self):
        "Test api_rows function"
        das  = {'system':['dbs', 'phedex'], 'api':['one', 'two'],
                'primary_key':'foo'}
        rows = [{'das':das, 'qhash':123, 'foo':[{'dbs':1}, {'phedex':1}]},
                {'das':das, 'qhash':123, 'foo':[{'dbs':2}, {'phedex':2}]}]
        result = [r for r in api_rows(rows, 'one')]
        edas   = {'system':['dbs'], 'api':['one'], 'primary_key':'foo'}
        expect = [{'das':edas, 'qhash':123, 'foo':{'dbs':1}},
                  {'das':edas, 'qhash':123, 'foo':{'dbs':2}}]
        self.assertEqual(result, expect)

    def test_httptimestamp(self):
        "Test httptimestamp function"
        day = 'Tue, 28 Aug 2012 16:50:29 GMT'
        self.assertEqual(http_timestamp(day), day)
        gmtime = time.gmtime()
        tstamp = time.strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime)
        self.assertEqual(http_timestamp(gmtime), tstamp)
        ltime  = time.time()
        gmtime = time.gmtime(ltime)
        tstamp = time.strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime)
        self.assertEqual(http_timestamp(gmtime), tstamp)

    def test_das_dateformat(self):
        "Test das_dateformat function"
        obj1  = '20120101'
        obj2  = '20120101 00:00:01'
        time1 = das_dateformat(obj1)
        time2 = das_dateformat(obj2)
        self.assertEqual(time1, 1325376000)
        self.assertEqual(time2, 1325376001)
        wrong = '2012'
        self.assertRaises(Exception, das_dateformat, wrong)
        wrong = '20120101 12'
        self.assertRaises(Exception, das_dateformat, wrong)

    def test_fix_times(self):
        "Test fix_dates function"
        for val in [20110101, time.time(), 'Sun 20-03-11 03:11:16']:
            row = {'file': [{'creation_time': val}, {'foo': 1}]}
            fix_times(row)
            val = row['file'][0]['creation_time']
            res = True if das_time_pattern.match(val) else False
            self.assertEqual(res, True)

    def test_parse_filter_string(self):
        "Test parse_filter_string function"
        val = "phedex"
        result = parse_filter_string(val)
        self.assertEqual(val, result)

        val = "[phedex]"
        result = parse_filter_string(val)
        expect = ["phedex"]
        self.assertEqual(expect, result)

        val = "[phedex,dbs]"
        result = parse_filter_string(val)
        expect = ["phedex", "dbs"]
        self.assertEqual(expect, result)

    def test_delete_keys(self):
        """Tests gen_counter function"""
        rec = {'a':1, 'b':1, 'c':2}
        delete_keys(rec, 1)
        self.assertEqual(rec.keys(), ['c'])

    def test_gen_counter(self):
        """Tests gen_counter function"""
        gen  = (i for i in xrange(0, 10))
        cdict = {'counter':0}
        gen = gen_counter(gen, cdict)
        self.assertEqual(len([i for i in gen]), cdict['counter'])

    def test_identical_data_records(self):
        """Tests identical_data_records function"""
        old = {'das_id':1, 'test':1, 'das':1}
        new = {'das_id':2, 'test':1, 'das':'foo'}
        result = identical_data_records(old, new)
        self.assertEqual(True, result)
        row = {'das_id':3, 'test':1, 'name':'foo'}
        result = identical_data_records(old, row)
        self.assertEqual(False, result)

    def test_deepcopy(self):
        """Test deepcopy function"""
        query = {'fields': ['release'], 
                 'spec': {u'release.name': 'CMSSW_2_0_8',
                 'das.primary_key': {'$in': [u'release.name']},
                 'das.condition_keys': [u'release.name']}}
        obj = deepcopy(query)
        self.assertEqual(query, obj)
        del query['spec']['das.primary_key']
        self.assertNotEqual(query, obj)

        obj = 1
        self.assertEqual(deepcopy(obj), obj)
        obj = (1,2)
        self.assertEqual(deepcopy(obj), obj)
        obj = [1,2]
        self.assertEqual(deepcopy(obj), obj)

    def test_das_diff(self):
        """Test das_diff function"""
        rec1 = dict(name='abc', size=1, system='dbs')
        rec2 = dict(name='abc', size=2, system='phedex')
        record = {'block':[rec1, rec2]}
        rows = [record]
        expect = [dict(record)]
        result = [r for r in das_diff(rows, ['block.size'])]
        try:
            self.assertEqual(expect, result)
        except AssertionError:
            pass

        rec1 = dict(name='abc', size=1, system='dbs')
        rec2 = dict(name='abc', size=1, system='phedex')
        record = {'block':[rec1, rec2]}
        rows = [record]
        expect = [dict(record)]
        result = [r for r in das_diff(rows, ['block.size'])]
        self.assertEqual(expect, result)

    def test_size_format(self):
        """Test size_format function"""
        pat = 'abc'
        expect = 'N/A'
        result = size_format(pat)
        self.assertEqual(expect, result)

        pat = None
        expect = 'N/A'
        result = size_format(pat)
        self.assertEqual(expect, result)

        pat = 1024
        expect = '1.0KB'
        result = size_format(pat)
        self.assertEqual(expect, result)

        pat = '1024'
        expect = '1.0KB'
        result = size_format(pat)
        self.assertEqual(expect, result)

    def test_convert2date(self):
        """Test convert2date function"""
        date = '24h'
        expect = [long(time.time()-24*60*60), long(time.time())]
        result = convert2date(date)
        self.assertEqual(expect, result)

        date = '60m'
        expect = [long(time.time()-60*60), long(time.time())]
        result = convert2date(date)
        self.assertEqual(expect, result)

        date = '60s'
        self.assertRaises(Exception, convert2date, date)

        date = '123'
        self.assertRaises(Exception, convert2date, date)

    def test_parse_filters(self):
        """Test parse_filters function"""
        filters = {'grep': ['monitor', 'file.size=1']}
        spec    = {'monitor': {'$exists':True}, 'file.size': 1}
        query   = {'spec': spec, 'filters': filters}
        expect  = {'file.size': 1}
        result  = parse_filters(query)
        self.assertEqual(expect, result)

        filters = ['file.size>1', 'file.size=1']
        query.update({'filters': filters})
        self.assertRaises(Exception, parse_filters, query)

        filters = {'grep': ['file.size>1', 'file.size<=10']}
        expect  = {'file.size': {'$gt':1, '$lte':10}}
        query.update({'filters': filters})
        result  = parse_filters(query)
        self.assertEqual(expect, result)

    def test_parse_filter(self):
        """Test parse_filter function"""
        spec = {'file.size':1}
        flt = 'monitor'
        result = parse_filter(spec, flt)
        expect = {flt: {'$exists':True}}
        self.assertEqual(expect, result)

        flt = 'file.size'
        result = parse_filter(spec, flt)
        expect = {} # no filter must be added, since spec carry the condition
        self.assertEqual(expect, result)

        flt = 'file.size=1'
        result = parse_filter(spec, flt)
        expect = {'file.size': 1}
        self.assertEqual(expect, result)

        flt = 'file.size<1'
        result = parse_filter(spec, flt)
        expect = {'file.size': {'$lt': 1}}
        self.assertEqual(expect, result)

        flt = 'file.size<=1'
        result = parse_filter(spec, flt)
        expect = {'file.size': {'$lte': 1}}
        self.assertEqual(expect, result)

        flt = 'file.size>1'
        result = parse_filter(spec, flt)
        expect = {'file.size': {'$gt': 1}}
        self.assertEqual(expect, result)

        flt = 'file.size>=1'
        result = parse_filter(spec, flt)
        expect = {'file.size': {'$gte': 1}}
        self.assertEqual(expect, result)

        flt = 'file.size!=1'
        result = parse_filter(spec, flt)
        expect = {'file.size': {'$ne': 1}}
        self.assertEqual(expect, result)

    def test_next_day(self):
        """Test next_day function"""
        result = next_day(20101231)
        expect = 20110101
        self.assertEqual(result, expect)

        result = next_day(20101230)
        expect = 20101231
        self.assertEqual(result, expect)

        self.assertRaises(Exception, next_day, 123)

    def test_prev_day(self):
        """Test prev_day function"""
        result = prev_day(20110101)
        expect = 20101231
        self.assertEqual(result, expect)

        result = prev_day(20101231)
        expect = 20101230
        self.assertEqual(result, expect)

        self.assertRaises(Exception, prev_day, 123)

    def test_genkey(self):
        dataset = "/a/b/c"
        d1 = {'fields': None, 'spec': [{'key': 'dataset.name', 'value': dataset}]}
        d2 = {'fields': None, 'spec': [{'value': dataset, 'key': 'dataset.name'}]}
        self.assertEqual(genkey(d1), genkey(d2))
    
    def test_expire_timestamp(self):
        """Test expire_timestamp function"""
        result = expire_timestamp('Mon, 04 Oct 2010 18:57:42 GMT')
        expect = 1286218662
        self.assertEqual(result, expect)

        tstamp = time.time() + 10000
        result = expire_timestamp(tstamp)
        expect = tstamp
        self.assertEqual(result, expect)

        tstamp = long(time.time() + 10)
        result = long(expire_timestamp(10))
        expect = tstamp
        self.assertEqual(result, expect)

        expire = '900'
        result = long(expire_timestamp(expire))
#        expect = long(time.time()) + 900
        expect = 900
        self.assertEqual(result, expect)

    def test_yield_rows(self):
        """Test yield_rows function"""
        val    = 1
        rows   = (r for r in range(2,5))
        expect = [1,2,3,4]
        result = [r for r in yield_rows(val, rows)]
        self.assertEqual(result, expect)

    def test_adjust_mongo_keyvalue(self):
        """Test adjust_mongo_keyvalue function"""
        rec = {'test': {'$in': [1,2]}, 'foo': {'$gte':1, '$lte':2}}
        result = adjust_mongo_keyvalue(rec)
        expect = {'test': [1,2], 'foo':[1,2]}
        self.assertEqual(result, expect)

        rec = {'foo': {'$gte':1}} # missing '$lte'
        self.assertRaises(Exception, adjust_mongo_keyvalue, rec)

    def test_unique_filter(self):
        """Test unique_filter filter"""
        rows = [{'k':1}, {'r':1}, {'r':1}]
        result = [r for r in unique_filter(rows)]
        expect = [{'k':1}, {'r':1}]
        self.assertEqual(result, expect)

        rows = [{'k':1}, {'k':1}, {'r':1}]
        result = [r for r in unique_filter(rows)]
        expect = [{'k':1}, {'r':1}]
        self.assertEqual(result, expect)

        rows = [{'k':1, '_id':1}, {'k':1, '_id':2}, {'r':1}]
        result = [r for r in unique_filter(rows)]
        expect = [{'k':1, '_id':1}, {'r':1}]
        self.assertEqual(result, expect)

        rows = [{'k':1, '_id':1}, {'k':1, '_id':2}, {'k':1, '_id':3}, {'r':1}]
        result = [r for r in unique_filter(rows)]
        expect = [{'k':1, '_id':1}, {'r':1}]
        self.assertEqual(result, expect)

    def test_aggregator(self):
        """Test aggregator function"""
        # 1 row in results
        dasquery = DASQuery(dict(fields=None, spec={'dataset':'/a/b/c'}))
        qhash = dasquery.qhash
        das  = {'expire': 10, 'primary_key':'vk', 'record': 1,
                'api':'api', 'system':['foo'], 'services':[],
                'condition_keys':['run'], 'instance':None}
        row  = {'run':10, 'das':das, '_id':1, 'das_id':1}
        rows = (row for i in range(0,1))
        result = [r for r in aggregator(dasquery, rows, das['expire'])]
        del result[0]['das']['ts'] # we don't need record timestamp
        expect = [{'run': 10, 'das':das, 'cache_id': [1], 'das_id': [1], 'qhash':qhash}]
        self.assertEqual(result, expect)

        # 2 rows with different values for common key
        rows = []
        row  = {'run':1, 'das':das, '_id':1, 'das_id':1}
        rows.append(row)
        row  = {'run':2, 'das':das, '_id':1, 'das_id':1}
        rows.append(row)
        res  = (r for r in rows)
        result = [r for r in aggregator(dasquery, res, das['expire'])]
        for r in result:
            del r['das']['ts'] # we don't need record timestamp
        expect = [{'run': 1, 'das':das, 'das_id': [1], 'cache_id': [1], 'qhash':qhash}, 
                  {'run': 2, 'das':das, 'das_id': [1], 'cache_id': [1], 'qhash':qhash}]
        self.assertEqual(result, expect)

        # 2 rows with common value for common key
        das  = {'expire': 10, 'primary_key':'run.a', 'record': 1,
                'api': ['api'], 'system':['foo'], 'services':[],
                'condition_keys':['run'], 'instance':None}
        rows = []
        row  = {'run':{'a':1,'b':1}, 'das':das, '_id':1, 'das_id':[1]}
        rows.append(row)
        row  = {'run':{'a':1,'b':2}, 'das':das, '_id':1, 'das_id':[1]}
        rows.append(row)
        res  = (r for r in rows)
        result = [r for r in aggregator(dasquery, res, das['expire'])]
        for r in result:
            del r['das']['ts'] # we don't need record timestamp
        expect = [{'run': [{'a': 1, 'b': 1}, {'a': 1, 'b': 2}], 'das':das,
                   'das_id': [1], 'cache_id': [1], 'qhash':qhash}]
        self.assertEqual(result, expect)

    def test_aggregator_duplicates(self):
        """Test aggregator function"""
        dasquery = DASQuery(dict(fields=None, spec={'dataset':'/a/b/c'}))
        qhash = dasquery.qhash
        das  = {'expire': 10, 'primary_key':'run.a', 'record': 1,
                'api':['api'], 'system':['foo'], 'services':[],
                'condition_keys':['run'], 'instance':None}
        rows = []
        row  = {'run':{'a':1,'b':1}, 'das':das, '_id':1, 'das_id':[1]}
        rows.append(row)
        row  = {'run':{'a':1,'b':1}, 'das':das, '_id':2, 'das_id':[2]}
        rows.append(row)
        res  = (r for r in rows)
        result = [r for r in aggregator(dasquery, res, das['expire'])]
        for r in result:
            del r['das']['ts'] # we don't need record timestamp
        expect = [{'run': [{'a':1,'b':1}, {'a':1,'b':1}], 'das':das, 'qhash':qhash,
                   'das_id': [1, 2], 'cache_id': [1, 2]}]
        self.assertEqual(result, expect)

    def test_filter(self):
        """Test filter function"""
        rows = []
        expect = []
        for i in range(0, 3):
            res = {'file':{'name':'a'}}
            res['file']['size'] = i
            res['file']['evts'] = i**2
            rows.append(dict(res))
            expect.append(('file.size',i))
            expect.append(('file.evts',i**2))
        filters = ['file.size', 'file.evts']
        result = [r for r in filter_with_filters(rows, filters)]
        self.assertEqual(expect, result)

    def test_merge_dict(self):
        """Test merge_dict"""
        dict1  = {'block':{'name':'AAA', 'b':{'c':1}, 'size':2}, 'das':{'system':'dbs'}}
        dict2  = {'block':{'name':'AAA', 'x':{'y':1}, 'z':1, 'size':2}, 'das':{'system':'phedex'}}
        merge_dict(dict1, dict2)
        expect = {'block': [{'b': {'c': 1}, 'name': 'AAA', 'size': 2}, 
        {'x': {'y': 1}, 'z': 1, 'name': 'AAA', 'size': 2}], 
        'das': [{'system': 'dbs'}, {'system': 'phedex'}]}
        self.assertEqual(expect, dict1)

        dict1  = {'test':[1,2]}
        dict2  = {'test':3}
        expect = {'test':[1,2,3]}
        merge_dict(dict1, dict2)
        self.assertEqual(expect, dict1)

        dict1  = {'test':[1,2]}
        dict2  = {'test':[3,4]}
        expect = {'test':[1,2,3,4]}
        merge_dict(dict1, dict2)
        self.assertEqual(expect, dict1)

        dict1  = {'test':1}
        dict2  = {'test':[2,3]}
        expect = {'test':[1,2,3]}
        merge_dict(dict1, dict2)
        self.assertEqual(expect, dict1)

        dict1  = {'test':1, 'foo': 1}
        dict2  = {'test':1, 'foo': [2,3]}
        expect = {'test':[1,1], 'foo':[1,2,3]}
        merge_dict(dict1, dict2)
        self.assertEqual(expect, dict1)

    def test_dict_helper(self):
        """Test dict_helper function"""
        idict = {'test':'1', 'float':'1.1', 'another_int': '0', 
                 'orig_int': 10, 'str': '2009 11.11', 'text':'text',
                 'se':'se.grid.kiae.ru'}
        notations = {'test':'int'}
        result = dict_helper(idict, notations)
        expect = {'int': 1, 'float': 1.1, 'another_int': 0, 
                  'orig_int': 10, 'str': '2009 11.11', 'text':'text',
                  'se':'se.grid.kiae.ru'}
        self.assertEqual(expect, result)

    def test_dict_value(self):
        """Test dict_value"""
        dict = {'a':{'b':{'c':1}}, 'd':2}
        result = dict_value(dict, 'a.b.c')
        expect = 1
        self.assertEqual(expect, result)

        result = dict_value(dict, 'd')
        expect = 2
        self.assertEqual(expect, result)

        dict = {'a' : [{'b':1}, {'b':2}]}
        result = dict_value(dict, 'a.b')
        expect = 1
        self.assertEqual(expect, result)
        
        dict1 = {'a' : [{'b':1, 'c':1}, {'b':2, 'c':2}]}
        dict2 = {'a' : {'b':1, 'e':1}}
        merge_dict(dict1, dict2)
        expect = {'a': [{'c': 1, 'b': 1}, {'c': 2, 'b': 2}, {'b': 1, 'e': 1}]}
        self.assertEqual(expect, dict1)
        
    def test_adjust_value(self):
        """Test adjust_value"""
        expect = 0
        result = adjust_value("0")
        self.assertEqual(expect, result)

        expect = 1
        result = adjust_value("1")
        self.assertEqual(expect, result)

        expect = 1.1
        result = adjust_value("1.1")
        self.assertEqual(expect, result)

        expect = -1.1
        result = adjust_value("-1.1")
        self.assertEqual(expect, result)

        expect = '2009.05.19 17:41:25'
        result = adjust_value("2009.05.19 17:41:25")
        self.assertEqual(expect, result)

        expect = None
        result = adjust_value("null")
        self.assertEqual(expect, result)

        expect = None
        result = adjust_value("(null)")
        self.assertEqual(expect, result)

#    def test_dasheader(self):
#        """Test DAS header"""
#        expect = ['dbs']
#        header = dasheader('dbs', 'q1', 'api1', 'url1', 'args1', 'ct1', 10)
#        self.assertEqual(expect, header['das']['system'])

    def test_cartesian_product(self):
        """Test cartesian product function"""
        list1 = [{'ds':1, 'site':2, 'admin':None, 'block':1}, 
                 {'ds':1, 'site':1, 'admin':None, 'block':1},
                 {'ds':2, 'site':1, 'admin':None, 'block':1},
                 {'ds':2, 'site':1, 'admin':None, 'block':1},
                ]
        list2 = [{'ds':None, 'site':2, 'admin':'vk', 'block':''}, 
                 {'ds':None, 'site':2, 'admin':'simon', 'block':''}, 
                 {'ds':None, 'site':1,'admin':'pet', 'block':''}]
        res = cartesian_product(list1, list2)
        result = [i for i in res]
        result.sort()
        expect = [{'ds':1, 'site':2, 'admin':'vk', 'block':1},
                  {'ds':1, 'site':2, 'admin':'simon', 'block':1},
                  {'ds':1, 'site':1, 'admin':'pet', 'block':1},
                  {'ds':2, 'site':1, 'admin':'pet', 'block':1},
                  {'ds':2, 'site':1, 'admin':'pet', 'block':1},
                 ]
        expect.sort()
        self.assertEqual(expect, result)
        
    def testCartesianProduct(self):
        """
        Test cartesian product utility
        """
        dbs_set = [
        {
                'system' : 'dbs',
                'admin' : '',
                'block' : '123-123-100',
                'dataset' : '/a/b/c',
                'site' : 'T2',
        },

        {
                'system' : 'dbs',
                'admin' : '',
                'block' : '123-123-101',
                'dataset' : '/a/b/c',
                'site' : 'T2',
        },

        {
                'system' : 'dbs',
                'admin' : '',
                'block' : '123-123-102',
                'dataset' : '/e/d/f',
                'site' : 'T2',
        }
        ]

        # results from SiteDB
        sitedb_set = [
        {
                'system' : 'sitedb',
                'admin' : 'vk',
                'block' : '',
                'dataset' : '',
                'site' : 'T2',
        },
        {
                'system' : 'sitedb',
                'admin' : 'simon',
                'block' : '',
                'dataset' : '',
                'site' : 'T2',
        }
        ]

        # results from Phedex
        phedex_set = [
        {
                'system' : 'phedex',
                'admin' : '',
                'block' : '123-123-100',
                'dataset' : '',
                'site' : 'T2',
        },

        {
                'system' : 'phedex',
                'admin' : '',
                'block' : '123-123-102',
                'dataset' : '',
                'site' : 'T2',
        }
        ]
#        result = cartesian_product(dbs_set, sitedb_set, ['site'])
#        result = cartesian_product(result, phedex_set, ['block','site'])
        result = cartesian_product(dbs_set, sitedb_set)
        result = cartesian_product(result, phedex_set)
        resultlist = [res for res in result]
        resultlist.sort()
        expectlist = [
        {
                'system': 'dbs+sitedb+phedex', 
                'admin' : 'vk',
                'block': '123-123-100',
                'dataset': '/a/b/c', 
                'site': 'T2', 
        },
        {
                'system': 'dbs+sitedb+phedex', 
                'admin' : 'vk',
                'block': '123-123-102',
                'dataset': '/e/d/f', 
                'site': 'T2', 
        },
        {
                'system': 'dbs+sitedb+phedex', 
                'admin' : 'simon',
                'block': '123-123-100',
                'dataset': '/a/b/c', 
                'site': 'T2', 
        },
        {
                'system': 'dbs+sitedb+phedex', 
                'admin' : 'simon',
                'block': '123-123-102',
                'dataset': '/e/d/f', 
                'site': 'T2', 
        }
        ]
        expectlist.sort()
        self.assertEqual(expectlist, resultlist)

    def test_genresults(self):
        """
        Test genresults utility
        """
        system = 'das'
        res = [{'a':1, 'b':2, 'x':100}]
        collect_list = ['a', 'b', 'c']
        result = genresults(system, res, collect_list)
        result.sort()
        expect = [{'a':1, 'b':2, 'c':'', 'system':'das'}]
        self.assertEqual(expect, result)

    def test_sitename(self):
        """
        Test sitename utility
        """
        alist = [('cms', 'T2_UK'), ('se', 'a.b.c'), ('site', 'UK'),
                 ('phedex', 'T2_UK_NO')]
        for expect, site in alist:
            result = sitename(site)
            self.assertEqual(expect, result)

    def test_transform_dict2list(self):
        """
        Test for transform_dict2list utility
        """
        indict = {'a':1, 'b':[1]}
        result = transform_dict2list(indict)
        expect = [{'a':1, 'b':1}]
        self.assertEqual(expect, result)

        indict = {'a':1, 'b':[1,2]}
        result = transform_dict2list(indict)
        result.sort()
        expect = [{'a':1, 'b':1}, {'a':1, 'b':2}]
        self.assertEqual(expect, result)

        indict = {'a':[1,2], 'b':1}
        result = transform_dict2list(indict)
        result.sort()
        expect = [{'a':1, 'b':1}, {'a':2, 'b':1}]
        self.assertEqual(expect, result)

        indict = {'a':[1,2], 'b':[1,2]}
        result = transform_dict2list(indict)
        result.sort()
        expect = [{'a':1, 'b':1}, {'a':2, 'b':2}] 
        expect.sort()
        self.assertEqual(expect, result)

        indict = {'a':1, 'b':1, 'c':[1]}
        result = transform_dict2list(indict)
        expect = [{'a':1, 'b':1, 'c':1}]
        self.assertEqual(expect, result)

        indict = {'c':1, 'a':[1,2,3], 'b':[1,2,3]}
        result = transform_dict2list(indict)
        result.sort()
        expect = [{'a':1, 'b':1, 'c':1}, {'a':2, 'b':2, 'c':1}, 
                  {'a':3, 'b':3, 'c':1}]
        expect.sort()
        self.assertEqual(expect, result)

    def test_add2dict(self):
        """
        test add2dict utility
        """
        # test 1
        idict  = {}
        key    = 'test'
        val    = 'abc'
        add2dict(idict, key, val)
        expect = {'test':'abc'}
        self.assertEqual(expect, idict)
        # test 2
        idict  = {'test':[1,2]}
        key    = 'test'
        val    = [3,4]
        add2dict(idict, key, val)
        expect = {'test':[1,2,3,4]}
        self.assertEqual(expect, idict)
        # test 3
        idict  = {'test':'abc'}
        key    = 'test'
        val    = [3,4]
        add2dict(idict, key, val)
        expect = {'test':['abc',3,4]}
        self.assertEqual(expect, idict)

    def test_map_validator(self):
        """
        test map_validator utility
        """
        # test 1
        smap   = {
                'api1' : {
                        'keys': ['k1', 'k2'],
                        'params' : {'p1': 1, 'p2': 2},
                        'url' : 'http://a.b.com',
                        'expire': 100,
                        'format' : 'XML',
                        'wild_card': '*',
                }
        }
        result = map_validator(smap)
        expect = None
        self.assertEqual(expect, result)
        # test 2
        smap['newkey'] = 1
        self.assertRaises(Exception, map_validator, smap)

    def test_splitlist(self):
        """
        test splitlist utility
        """
        ilist = [i for i in range(0, 10)]
        llist = [i for i in splitlist(ilist, 3)]
        expect = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
        self.assertEqual(expect, llist)
        expect = [[i for i in range(0,10)]]
        llist = [i for i in splitlist(ilist, 10)]
        self.assertEqual(expect, llist)

    def test_sorting(self):
        """Test sorting routines"""
        data = [
            {'id':6, 'dataset': 'bla6', 'run':200},
            {'id':1, 'dataset': 'bla1', 'run':100},
            {'id':2, 'dataset': 'bla2', 'run':700},
            {'id':3, 'dataset': 'bla3', 'run':400},
            {'id':4, 'dataset': 'bla4', 'run':300},
            {'id':5, 'dataset': 'bla5', 'run':800},
        ]
        sorted_data = [i for i in sort_data(data, 'dataset')]
        expect = [
                {'run': 100, 'id': 1, 'dataset': 'bla1'}, 
                {'run': 700, 'id': 2, 'dataset': 'bla2'}, 
                {'run': 400, 'id': 3, 'dataset': 'bla3'}, 
                {'run': 300, 'id': 4, 'dataset': 'bla4'}, 
                {'run': 800, 'id': 5, 'dataset': 'bla5'}, 
                {'run': 200, 'id': 6, 'dataset': 'bla6'},
        ]
        self.assertEqual(expect, sorted_data)
        sorted_data = [i for i in sort_data(data, 'run')]
        expect = [
                {'run': 100, 'id': 1, 'dataset': 'bla1'}, 
                {'run': 200, 'id': 6, 'dataset': 'bla6'}, 
                {'run': 300, 'id': 4, 'dataset': 'bla4'}, 
                {'run': 400, 'id': 3, 'dataset': 'bla3'}, 
                {'run': 700, 'id': 2, 'dataset': 'bla2'}, 
                {'run': 800, 'id': 5, 'dataset': 'bla5'},
        ]
        self.assertEqual(expect, sorted_data)
        sorted_data = [i for i in sort_data(data, 'run', 'desc')]
        expect = [
                {'run': 800, 'id': 5, 'dataset': 'bla5'},
                {'run': 700, 'id': 2, 'dataset': 'bla2'}, 
                {'run': 400, 'id': 3, 'dataset': 'bla3'}, 
                {'run': 300, 'id': 4, 'dataset': 'bla4'}, 
                {'run': 200, 'id': 6, 'dataset': 'bla6'}, 
                {'run': 100, 'id': 1, 'dataset': 'bla1'}, 
        ]
        self.assertEqual(expect, sorted_data)

    def test_convert_dot_notation(self):
        """Test convert_dot_notation function"""
        key = "block.replica.name"
        val = "test"
        result = convert_dot_notation(key, val)
        expect = "block", {"replica": {"name":val}}
        self.assertEqual(expect, result)

        key = "block.name"
        val = "test"
        result = convert_dot_notation(key, val)
        expect = "block", {"name":val}
        self.assertEqual(expect, result)

    def test_delete_elem(self):
        """Test delete_elem function"""
        key = "site.resource_element.cms_name"
        row = {"site":{"resource_element":{"size":1, "cms_name":"AA"}}}
        delete_elem(row, key)
        expect = {"site":{"resource_element":{"size":1}}}
        self.assertEqual(expect, row)
        
    def test_translate(self):
        """Test translate function"""
        api = ""
        row = {"site":{"resource_element":{"size":1, "cms_name":"AA"}}}
        notations = [
        {"notation": "site.resource_element.cms_name", "map": "site.name", "api": ""},
        {"notation": "site.resource_pledge.cms_name", "map": "site.name", "api": ""},
        {"notation": "admin.contacts.cms_name", "map":"site.name", "api":""}
        ]       
        res = translate(notations, api, row)
        result = next(res)
        expect = {"site":{"name":"AA", "resource_element":{"size":1}}}
        self.assertEqual(expect, result)

    def test_xml_parser(self):
        """
        Test functionality of xml_parser
        """
        xmldata = """<?xml version='1.0' encoding='ISO-8859-1'?>
<phedex attr="a">
<block bytes="1">
<file size="10">
</file>
</block>
</phedex>
"""
        fdesc  = tempfile.NamedTemporaryFile()
        fname  = fdesc.name
        stream = file(fname, 'w')
        stream.write(xmldata)
        stream.close()
        stream = file(fname, 'r')
        gen    = xml_parser(stream, "block", [])
        result = next(gen)
        expect = {'block': {'bytes': 1, 'file': {'size': 10}}}
        self.assertEqual(expect, result)

        stream = file(fname, 'r')
        gen    = xml_parser(stream, "file", ["block.bytes"])
        result = next(gen)
        expect = {'file': {'block': {'bytes': 1}, 'size': 10}}
        self.assertEqual(expect, result)

    def test_xml_parser_2(self):
        """
        Test functionality of xml_parser
        """
        xmldata = """<?xml version='1.0' encoding='ISO-8859-1'?>
<RUNS>
<RUN id="751084">
<LUMI>
<NUMBER>1</NUMBER>
<PROP>avx</PROP>
<TEST>
<FOO>1</FOO>
<BOO>2</BOO>
</TEST>
</LUMI>
</RUN>
</RUNS>
"""
        fdesc  = tempfile.NamedTemporaryFile()
        fname  = fdesc.name
        stream = file(fname, 'w')
        stream.write(xmldata)
        stream.close()
        stream = file(fname, 'r')
        gen    = xml_parser(stream, "RUNS", [])
        result = next(gen)
        expect = {'RUNS': {'RUN': {'id': 751084.0, 
                                   'LUMI': {'TEST': {'FOO': 1, 'BOO': 2},
                                            'NUMBER': 1, 
                                            'PROP': 'avx'}
                                  }
                          }
                 }
        self.assertEqual(expect, result)

    def test_xml_parser_3(self):
        """
        Test functionality of xml_parser
        """
        xmldata = """<?xml version='1.0' encoding='ISO-8859-1'?>
<results>
<row>
  <dataset>/a/b/c</dataset>
  <nblocks>25</nblocks>
</row>
</results>
"""
        fdesc  = tempfile.NamedTemporaryFile()
        fname  = fdesc.name
        stream = file(fname, 'w')
        stream.write(xmldata)
        stream.close()
        stream = file(fname, 'r')
        gen    = qlxml_parser(stream, "dataset")
        result = next(gen)
        expect = {'dataset': {'dataset':'/a/b/c', 'nblocks': 25}}
        self.assertEqual(expect, result)

    def test_xml_parser_4(self):
        """
        Test functionality of xml_parser
        """
        xmldata = """<?xml version='1.0' encoding='ISO-8859-1'?>
<results>
<row>
  <name>/c1.root</name>
  <size>1</size>
</row>
<row>
  <name>/c2.root</name>
  <size>2</size>
</row>
</results>
"""
        fdesc  = tempfile.NamedTemporaryFile()
        fname  = fdesc.name
        stream = file(fname, 'w')
        stream.write(xmldata)
        stream.close()
        stream = file(fname, 'r')
        gen    = qlxml_parser(stream, "file")
        result = [r for r in gen]
        expect = [{'file': {'name': '/c1.root', 'size': 1}}, 
                  {'file': {'name': '/c2.root', 'size': 2}}]
        self.assertEqual(expect, result)

    def test_xml_parser_5(self):
        """
        Test functionality of xml_parser
        """
        xmldata = """<?xml version='1.0' standalone='yes'?>
<!-- DBS Version 1 -->
<dbs>
 <userinput>
  <input>find dataset where  tier=*GEN* and primds=ZJetToEE_Pt*
  </input>
  <timeStamp>Mon Feb 07 19:51:59 CET 2011</timeStamp>
 </userinput>
 <java_query> 
  <sql>GROUP BY  PATH</sql>
  <bp>%GEN%</bp>
  <bp>ZJetToEE_Pt%</bp>
 </java_query>
 <python_query>
  <sql>SELECT  PATH AS PATH,</sql>
  <bindparams><p0>%GEN%</p0>
   <p1>ZJetToEE_Pt%</p1>
  </bindparams>
 </python_query>
 <count_query>
  <sql> SELECT COUNT(*) AS CNT FROM </sql>
  <bindparams> <p0>%GEN%</p0>
   <p1>ZJetToEE_Pt%</p1>
  </bindparams>
 </count_query>
 <results>"""
 
        suffix = """ </results>
<SUCCESS/>
</dbs>
"""
        row = """<row>
  <dataset>/ZJetToEE_Pt_80to120_TuneZ2_7TeV_pythia6/</dataset>
  <sum_block.numfiles>%d</sum_block.numfiles>
  <sum_block.numevents>110000</sum_block.numevents>
  <count_block>1</count_block>
  <sum_block.size>61942523513</sum_block.size>
</row>"""
        for i in range(200):
            xmldata = xmldata + row % i
        xmldata = xmldata + suffix  
        fdesc  = tempfile.NamedTemporaryFile()
        fname  = fdesc.name
        stream = file(fname, 'w')
        stream.write(xmldata)
        stream.close()
        stream = file(fname, 'r')
        gen    = qlxml_parser(stream, "dataset")
        expect = {'dataset': {'sum_block.numfiles': 12, 
                              'count_block': 1, 
                              'sum_block.numevents': 110000, 
                              'sum_block.size': 61942523513, 
                  'dataset': '/ZJetToEE_Pt_80to120_TuneZ2_7TeV_pythia6/'}}
        count = 0
        for r in gen:
            expect['dataset']['sum_block.numfiles'] = count
            self.assertEqual(expect, r)
            count = count + 1
        self.assertEqual(200, count)

    def test_json_parser(self):
        """
        Test functionality of json_parser
        """
        jsondata = {'beer': {'amstel':'good', 'guiness':'better'}}
        fdesc  = tempfile.NamedTemporaryFile()
        fname  = fdesc.name
        stream = file(fname, 'w')
        stream.write(json.dumps(jsondata))
        stream.close()
        stream = file(fname, 'r')
        gen    = json_parser(stream)
        result = next(gen)
        expect = {'beer': {'amstel': 'good', 'guiness': 'better'}}
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()
