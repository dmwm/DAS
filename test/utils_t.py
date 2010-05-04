#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import unittest
from DAS.utils.utils import cartesian_product, dasheader
from DAS.utils.utils import genresults, transform_dict2list
from DAS.utils.utils import sitename, add2dict, map_validator
from DAS.utils.utils import splitlist, gen_key_tuples, sort_data
from DAS.utils.utils import dict_value, merge_dict, adjust_value
from DAS.utils.utils import json_parser, xml_parser, dict_helper

class testUtils(unittest.TestCase):
    """
    A test class for the DAS utils module
    """
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

    def test_xml_parser(self):
        """
        Test functionality of xml_parser
        """
        notations = {}

        dataset = '/Njet_4j_160_200-alpgen/CMSSW_1_6_7-CSA07-1201630335/RECO'
        uid = '04e2c867-3031-40a0-ac14-9fe57af33794'
        block = dataset + '#' + uid
        url = 'http://cmsweb.cern.ch/phedex/datasvc/xml/prod/blockReplicas'
        params = {'block':block}
        import urllib2, urllib
        print
        print "Check Phedex"
        print "%s?%s" % (url, urllib.urlencode(params, doseq=True))
        data = urllib2.urlopen(url, urllib.urlencode(params, doseq=True))
        gen = xml_parser(notations, data, "block")
        for item in gen:
            print item

        params = {'storage_element_name': '*', 'block_name':block,
                  'api': 'listBlocks', 'user_type': 'NORMAL', 
                  'apiversion': 'DBS_2_0_8'}
        print
        print "Check DBS"
        print "%s?%s" % (url, urllib.urlencode(params, doseq=True))
        url = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
        data = urllib2.urlopen(url, urllib.urlencode(params, doseq=True))
        gen = xml_parser(notations, data, "block")
        for item in gen:
            print item

#        params = {'run_number': '', 'data_tier_list': '', 
#                  'analysis_dataset_name': '', 'processed_dataset': '', 
#                  'detail': 'True', 'apiversion': 'DBS_2_0_8', 
#                  'retrive_list': '', 'block_name': block,
#                  'api': 'listFiles', 'pattern_lfn': '', 
#                  'path': '', 'primary_dataset': '', 'other_detail': 'True'}
#        print
#        print "Check DBS"
#        print "%s?%s" % (url, urllib.urlencode(params, doseq=True))
#        url = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
#        data = urllib2.urlopen(url, urllib.urlencode(params, doseq=True))
#        gen = xml_parser(notations, data, "file")
#        for item in gen:
#            print item

        result = None
        expect = None
        self.assertEqual(expect, result)
#
# main
#
if __name__ == '__main__':
    unittest.main()
