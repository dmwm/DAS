#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS core module
"""

import unittest
from core.das_core import DASCore
from core.qlparser import dasqlparser

class testDAS(unittest.TestCase):
    """
    A test class for the DAS core module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.das = DASCore()

    def test_queryanalyzer(self):                          
        """test DAS QL analyzer routine"""
        query = "find dataset, block, admin where site=T2 and run=12 and storage=1 or run=2 and dataset=1"
        qldict = dasqlparser(query)
        result = self.das.queryanalyzer(qldict)
        expect = {'qlqueries': 
                        {'q1': 'find dataset,block,admin where run=12', 
                         'q0': 'find dataset,block,admin where site=T2', 
                         'q3': 'find dataset,block,admin where run=2', 
                         'q2': 'find dataset,block,admin where storage=1', 
                         'q4': 'find dataset,block,admin where dataset=1'}, 
                  'sellist': ['dataset', 'block', 'admin'], 
                  'qlquery': 'find dataset, block, admin where q0 and q1 and q2 or q3 and q4', 
                  'condlist': {'q1': 'run=12', 'q0': 'site=T2', 'q3': 'run=2', 
                               'q2': 'storage=1', 'cond1': 'storage=1', 
                               'q4': 'dataset=1', 'cond2': 'run=2', 
                               'cond0': 'site=T2 and run=12'}, 
                  'query': 'find dataset, block, admin where cond0 and cond1 or cond2 and q4', 
                  'queries': {'cond1': 'find dataset, block, admin where storage=1', 
                              'q4': 'find dataset,block,admin where dataset=1', 
                              'cond2': 'find dataset, block, admin where run=2', 
                              'cond0': 'find dataset, block, admin where site=T2 and run=12'}, 
                  'input': 'find dataset, block, admin where site=T2 and run=12 and storage=1 or run=2 and dataset=1'}
        self.assertEqual(expect, result)

    def test_findmappedservices(self):
        """test service mapper routine"""
        key = "block"
        result = [x for x in self.das.findmappedservices(key)]
        result.sort()
#        expect = (x for x in ['dbs','phedex'])
        expect = ['dbs','phedex']
        self.assertEqual(expect, result)

    def test_findservices(self):
        """test services routine"""
        query = "find dataset, admin where site = T2_UK"
        result = self.das.findservices(query)
        result.sort()
        expect = ['dbs','sitedb']
        self.assertEqual(expect, result)

#    def test_call(self):                          
#        """test call routine"""
#        query = "find dataset, admin where site = T2_UK"
#        result = self.das.call(query)
#        resultlist = [res for res in result]
#        rd = {'storage': 1, '_system_': 'dbs+phedex', 'block': 1, 'dataset': 1}
#        expectlist = [rd]
#        self.assertEqual(expectlist, resultlist)

#
# main
#
if __name__ == '__main__':
    unittest.main()

