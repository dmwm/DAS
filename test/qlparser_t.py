#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import unittest
#from DAS.core.qlparser import antrlparser
from DAS.core.qlparser import findbracketobj
from DAS.core.qlparser import getconditions
from DAS.core.qlparser import QLParser

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

#    def testantrlparser(self):
#        """test parser based on ANTRL"""
#        q = "find dataset, run where site = T2"
#        res = antrlparser(q)
#        r = {'ORDERING': [], 'FIND_KEYWORDS': ['dataset', 'run'], 'ORDER_BY_KEYWORDS': [], 'WHERE_CONSTRAINTS': [{'value': 'T2', 'key': 'site', 'op': '='}, {'bracket': 'T2'}]}
#        self.assertEqual(res, r)
        
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

    def test_qlparser(self):
        """test QLParser class"""
        imap = {'dbs':['dataset', 'run', 'site', 'block'],
                'phedex':['block', 'replica', 'site'], 
                'sitedb': ['admin', 'site']}
        q = "find dataset where ((run=1 or run=2) or dataset=bla) and site=cern order by block"
        ql = QLParser(imap)

        result = ql.selkeys(q)
        expect = ['dataset']
        self.assertEqual(result, expect)

        result = ql.conditions(q)
        expect = ['(', '(', {'value': '1', 'key': 'run', 'op': '='}, 
                  'or', {'value': '2', 'key': 'run', 'op': '='}, ')', 
                  'or', {'value': 'bla', 'key': 'dataset', 'op': '='}, ')', 
                  'and', {'value': 'cern', 'key': 'site', 'op': '='}]
        self.assertEqual(result, expect)

        result = ql.allkeys(q)
        result.sort()
        expect = ['block', 'dataset', 'run', 'site']
        expect.sort()
        self.assertEqual(result, expect)

        q = "find a,b,c where r=1)"
        self.assertRaises(Exception, ql.params, q)

        q = "find a,v whhhere"
        self.assertRaises(Exception, ql.params, q)
        
        q = "finds a,v"
        self.assertRaises(Exception, ql.params, q)
        
        q = "find dataset,admin,replica where site=123 or site=345 order by site desc"
        result = ql.params(q)
        expect = {'order_by_list': ['site'], 
                  'selkeys': ['admin', 'dataset', 'replica'], 
                  'unique_services': ['dbs', 'phedex', 'sitedb'], 
                  'order_by_order': 'desc', 
                  'services': {'sitedb': ['admin', 'site'], 
                               'dbs': ['dataset', 'site'], 
                               'phedex': ['replica', 'site']}, 
                  'daslist': [{'sitedb': 'find admin,dataset,replica,site,block where site = 123', 
                               'dbs': 'find admin,dataset,replica,site,block where site = 123', 
                               'phedex': 'find admin,dataset,replica,site,block where site = 123'}, 
                              {'sitedb': 'find admin,dataset,replica,site,block where site = 345', 
                               'dbs': 'find admin,dataset,replica,site,block where site = 345', 
                               'phedex': 'find admin,dataset,replica,site,block where site = 345'}],
                  'conditions': [{'value': '123', 'key': 'site', 'op': '='}, 
                                 'or', 
                                 {'value': '345', 'key': 'site', 'op': '='}], 
                  'allkeys': ['admin', 'dataset', 'replica', 'site'], 
                  'unique_keys': ['admin', 'block', 'dataset', 'replica', 'site']}
        self.assertEqual(result, expect)


#
# main
#
if __name__ == '__main__':
    unittest.main()
