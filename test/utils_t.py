#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import unittest
from utils.utils import cartesian_product

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

#
# main
#
if __name__ == '__main__':
    unittest.main()
