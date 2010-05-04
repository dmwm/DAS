#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import unittest
from utils.utils import cartesian_product, query_params
from utils.utils import genresults, transform_dict2list
from utils.utils import sitename, add2dict

class testUtils(unittest.TestCase):
    """
    A test class for the DAS utils module
    """
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
        result = cartesian_product(dbs_set, sitedb_set, ['site'])
        result = cartesian_product(result, phedex_set, ['block','site'])
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

    def test_query_params(self):
        """
        Test query_params utility which split query into set of parameters and
        selected keys.
        """
        queries = ['find a,b,c where d=2', 'find a,b,c where d not like 2',
                   'find a,b,c']
        selkeys = ['a', 'b', 'c']
        elist   = [(selkeys, {'d':('=', '2')}), 
                   (selkeys, {'d':('not like','2')}), (selkeys, {})]
        for idx in range(0, len(queries)):
            query  = queries[idx]
            expect = elist[idx]
            result = query_params(query)
            self.assertEqual(expect, result)

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
        expect = [{'a':1, 'b':1}, {'a':1, 'b':2}, 
                  {'a':2, 'b':1}, {'a':2, 'b':2}]
        expect.sort()
        self.assertEqual(expect, result)

        indict = {'a':1, 'b':1, 'c':[1]}
        result = transform_dict2list(indict)
        expect = [{'a':1, 'b':1, 'c':1}]
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

#
# main
#
if __name__ == '__main__':
    unittest.main()
