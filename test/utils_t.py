#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import unittest
from utils.utils import cartesian_product, query_params
from utils.utils import transform_dict2list

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
                '_system_' : 'dbs',
                'dataset' : '/a/b/c',
                'run' : '100',
                'bfield' : '',
                'site' : 'T2',
                'block' : '123-123-100',
        },

        {
                '_system_' : 'dbs',
                'dataset' : '/a/b/c',
                'run' : '101',
                'bfield' : '',
                'site' : 'T2',
                'block' : '123-123-101',
        },

        {
                '_system_' : 'dbs',
                'dataset' : '/a/b/c',
                'run' : '102',
                'bfield' : '',
                'site' : 'T2',
                'block' : '123-123-102',
        }
        ]

        # results from SiteDB
        sitedb_set = [
        {
                '_system_' : 'sitedb',
                'dataset' : '',
                'run' : '',
                'bfield' : '',
                'site' : 'T2',
                'block' : '',
        }
        ]

        # results from RunSum
        runsum_set = [
        {
                '_system_' : 'runsum',
                'dataset' : '',
                'run' : '101',
                'bfield' : '0.2',
                'site' : '',
                'block' : '',
        },

        {
                '_system_' : 'runsum',
                'dataset' : '',
                'run' : '102',
                'bfield' : '0.3',
                'site' : '',
                'block' : '',
        }
        ]

        # results from Phedex
        phedex_set = [
        {
                '_system_' : 'phedex',
                'dataset' : '',
                'run' : '',
                'bfield' : '',
                'site' : '',
                'block' : '123-123-100',
        },

        {
                '_system_' : 'phedex',
                'dataset' : '',
                'run' : '',
                'bfield' : '',
                'site' : '',
                'block' : '123-123-102',
        }
        ]
        result = cartesian_product(dbs_set, sitedb_set)
        result = cartesian_product(result, runsum_set)
        result = cartesian_product(result, phedex_set)
        resultlist = [res for res in result]
        expectlist = [
        {
                'bfield': '0.3', 
                'run': '102', 
                '_system_': 'dbs+sitedb+runsum+phedex', 
                'site': 'T2', 
                'dataset': '/a/b/c', 
                'block': '123-123-102'
        }
        ]
#        print "expectlist", expectlist
#        print "resultlist", resultlist
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

#
# main
#
if __name__ == '__main__':
    unittest.main()
