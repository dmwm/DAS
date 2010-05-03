#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import unittest
from DAS.core.qlparser import dasqlparser, findbracketobj, antrlparser

class testQLParser(unittest.TestCase):
    """
    A test class for the DAS qlparser
    """
    def setUp(self):
        """
        set up data used in the tests.
        setUp is called before each test function execution.
        """
        self.i1 = "find dataset, run, bfield where site = T2 and admin=VK and storage=castor"
        self.i2 = "  find dataset, run where (run=1 or run=2) and storage=castor or site = T2"

    def testantrlparser(self):
        """test parser based on ANTRL"""
        q = "find dataset, run where site = T2"
        res = antrlparser(q)
        r = {'ORDERING': [], 'FIND_KEYWORDS': ['dataset', 'run'], 'ORDER_BY_KEYWORDS': [], 'WHERE_CONSTRAINTS': [{'value': 'T2', 'key': 'site', 'op': '='}, {'bracket': 'T2'}]}
        self.assertEqual(res, r)
        
    def testSelectionList(self):                          
        """test user input queries"""
        q = "find dataset, run, bfield where site = T2 and admin=VK and storage=castor"
        r = ['dataset', 'run', 'bfield']
        res = dasqlparser(q)
        self.assertEqual(res['sellist'], r)
        q = "find dataset, run, bfield"
        res = dasqlparser(q)
        self.assertEqual(res['sellist'], r)

    def testBracketObj(self):                          
        """test search for bracket objects"""
        testlist = [
("vk or test ((test or test) or (another obj)) or test2", "((test or test) or (another obj))"),
("((test or test)) and (test or test)","((test or test))"),
]
        for q, r in testlist:
            obj = findbracketobj(q)
            self.assertEqual(obj, r)

    def testFalseBracketObj(self):                          
        """false test for bracket objects"""
        q = "test (test1 (test2 or test3)"
        self.assertRaises(Exception, findbracketobj, q)

    def testQueries(self):                          
        """test user input queries"""
        q = "find dataset,run,bfield where site = T2 and admin=VK and storage=castor"
        querylist = [
"find dataset,run,bfield where site = T2",
"find dataset,run,bfield where admin=VK",
"find dataset,run,bfield where storage=castor"
]
        querylist.sort()
        res = dasqlparser(q)
        res_queries = res['queries'].values()
        res_queries.sort()
        self.assertEqual(res_queries, querylist)

    def testQueries2(self):                          
        """test #2 user input queries"""
        q = "find dataset,admin where site = T2"
        querylist = [
"find dataset,admin where site = T2"
]
        querylist.sort()
        res = dasqlparser(q)
        res_queries = res['queries'].values()
        res_queries.sort()
        self.assertEqual(res_queries, querylist)
#
# main
#
if __name__ == '__main__':
    unittest.main()
