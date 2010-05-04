#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import json
import unittest
import urllib2, urllib
import tempfile
from DAS.utils.utils import cartesian_product, dasheader
from DAS.utils.utils import genresults, transform_dict2list
from DAS.utils.utils import sitename, add2dict, map_validator
from DAS.utils.utils import splitlist, gen_key_tuples, sort_data
from DAS.utils.utils import dict_value, merge_dict, adjust_value
from DAS.utils.utils import json_parser, xml_parser, dict_helper
from DAS.utils.utils import convert_dot_notation, translate
from DAS.utils.utils import delete_elem, plist_parser
from DAS.utils.utils import dotdict, filter, aggregator, yield_rows

class testUtils(unittest.TestCase):
    """
    A test class for the DAS utils module
    """
    def test_yield_rows(self):
        """Test yield_rows function"""
        val    = 1
        rows   = (r for r in range(2,5))
        expect = [1,2,3,4]
        result = [r for r in yield_rows(val, rows)]
        self.assertEqual(result, expect)

    def test_aggregator(self):
        """Test aggregator function"""
        # 1 row in results
        das  = {'expire': 10}
        row  = {'das_primary_key':'vk', 'run':10, 'das':1, '_id':1}
        rows = (row for i in range(0,1))
        result = [r for r in aggregator(rows, das['expire'])]
        expect = [{'run': 10, 'das':das}]
        self.assertEqual(result, expect)

        # 2 rows with different values for common key
        rows = []
        row  = {'das_primary_key':'vk', 'run':1, 'das':1, '_id':1}
        rows.append(row)
        row  = {'das_primary_key':'vk', 'run':2, 'das':1, '_id':1}
        rows.append(row)
        res  = (r for r in rows)
        result = [r for r in aggregator(res, das['expire'])]
        expect = [{'run': 1, 'das':das}, {'run': 2, 'das':das}]
        self.assertEqual(result, expect)

        # 2 rows with common value for common key
        rows = []
        row  = {'das_primary_key':'run.a', 'run':{'a':1,'b':1}, 'das':1, '_id':1}
        rows.append(row)
        row  = {'das_primary_key':'run.a', 'run':{'a':1,'b':2}, 'das':1, '_id':1}
        rows.append(row)
        res  = (r for r in rows)
        result = [r for r in aggregator(res, das['expire'])]
        expect = [{'run': [{'a': 1, 'b': 1}, {'a': 1, 'b': 2}], 'das':das}]
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
        result = [r for r in filter(rows, filters)]
        self.assertEqual(expect, result)

    def test_dotdict(self):
        """Test dotdict class"""
        res = {'a':{'b':{'c':10}, 'd':10}}
        ddict = dotdict(res)
        ddict._set('x.y.z', 10)
        expect = {'a':{'b':{'c':10}, 'd':10}, 'x':{'y':{'z':10}}}
        self.assertEqual(expect, ddict)

        ddict._set('a.b.k.m', 10)
        expect = {'a':{'b':{'c':10, 'k':{'m':10}}, 'd':10}, 'x':{'y':{'z':10}}}
        self.assertEqual(expect, ddict)
        expect = 10
        result = ddict._get('a.b.k.m')
        self.assertEqual(expect, result)

        res = {'a':{'b':{'c':10}, 'd':[{'x':1}, {'x':2}]}}
        ddict = dotdict(res)
        expect = 1
        result = ddict._get('a.d.x')
        self.assertEqual(expect, result)
        expect = None
        result = ddict._get('a.M.Z')
        self.assertEqual(expect, result)

        res = {'a': {'b': {'c':1, 'd':2}}}
        ddict = dotdict(res)
        expect = {'a': {'b': {'c':1}}}
        ddict._delete('a.b.d')
        self.assertEqual(expect, ddict)

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

        expect = '2009.05.19 17:41:25'
        result = adjust_value("2009.05.19 17:41:25")
        self.assertEqual(expect, result)

        expect = None
        result = adjust_value("null")
        self.assertEqual(expect, result)

        expect = None
        result = adjust_value("(null)")
        self.assertEqual(expect, result)

    def test_dasheader(self):
        """Test DAS header"""
        expect = ['dbs']
        header = dasheader('dbs', 'q1', 'api1', 'url1', 'args1', 'ct1', 10)
        self.assertEqual(expect, header['das']['system'])

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
        result = res.next()
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
        result = gen.next()
        expect = {'block': {'bytes': 1, 'file': {'size': 10}}}
        self.assertEqual(expect, result)

        stream = file(fname, 'r')
        gen    = xml_parser(stream, "file", ["block.bytes"])
        result = gen.next()
        expect = {'file': {'block': {'bytes': 1}, 'size': 10}}
        self.assertEqual(expect, result)

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
        result = gen.next()
        expect = {'beer': {'amstel': 'good', 'guiness': 'better'}}
        self.assertEqual(expect, result)

    def test_plist_parser(self):
        """
        Test functionality of plist_parser
        """
        plistdata = """<?xml version='1.0' encoding='ISO-8859-1'?>
<!DOCTYPE plist PUBLIC "-//Apple Computer//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
        <key>beer</key>
        <dict>
                <key>amstel</key>
                <string>good</string>
                <key>guiness</key>
                <string>better</string>
        </dict>
</dict>
</plist>
"""
        fdesc  = tempfile.NamedTemporaryFile()
        fname  = fdesc.name
        stream = file(fname, 'w')
        stream.write(plistdata)
        stream.close()
        stream = file(fname, 'r')
        gen    = plist_parser(stream)
        result = gen.next()
        expect = {'beer': {'amstel': 'good', 'guiness': 'better'}}
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()
