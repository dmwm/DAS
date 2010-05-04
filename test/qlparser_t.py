#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import unittest
#from DAS.core.qlparser import antrlparser
from DAS.core.qlparser import findbracketobj, mongo_exp
from DAS.core.qlparser import getconditions, query_params
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

    def test_qlparser_init(self):
        """test initalization of QLParser class"""
        imap = {'dbs':['dataset', 'run', 'site', 'block'],
                'phedex':['block', 'replica', 'site'], 
                'sitedb': ['admin', 'site'],
                'dq': ['runs', 'DQFlagList']}
        params = {}
        self.assertRaises(Exception, QLParser, (imap, params))

    def test_query_params(self):
        """
        Test query_params utility which split query into set of parameters and
        selected keys.
        """
        queries = ['find a,b,c where d=2', 'find a,b,c where d not like 2',
                   'find a,b,c', 'find a,b,c where d=2 and e=1']
        selkeys = ['a', 'b', 'c']
        elist   = [(selkeys, {'d':('=', '2')}), 
                   (selkeys, {'d':('not like', '2')}), 
                   (selkeys, {}),
                   (selkeys, {'d':('=', '2'), 'e':('=', '1')}),
                  ]
        for idx in range(0, len(queries)):
            query  = queries[idx]
            expect = elist[idx]
            result = query_params(query)
            self.assertEqual(expect, result)

    def test_qlparser(self):
        """test QLParser class"""
        imap = {'dbs':['dataset', 'run', 'file', 'block'],
                'phedex':['block', 'file', 'site'], 
                'sitedb': ['admin', 'site'],
                'dq': ['runs', 'DQFlagList'],
                'dashboard': ['jobsummary']}
        params = {'dbs':['dataset', 'run', 'file', 'block'], 
                'phedex':['block', 'file', 'site'], 
                'sitedb':['admin', 'site'], 
                'dq':['run','dataset'],
                'dashboard':['site']}

        ql = QLParser(imap, params)

        q = "find runs where run>1 and run <= 200 "
        result = mongo_exp(ql.conditions(q))
        expect = {'run': {'$gt' : '1', '$lte' : '200'}}
        self.assertEqual(result, expect)

        q = "find runs where dataset=/a/b/c and DQFlagList=Tracker_Global=GOOD&Tracker_Local1=1"
        result = ql.params(q)['conditions']
        expect = [{'value': '/a/b/c', 'key': 'dataset', 'op': '='}, 'and', 
        {'value': 'Tracker_Global=GOOD&Tracker_Local1=1', 'key': 'DQFlagList', 'op': '='}]
        self.assertEqual(result, expect)

        q = "find runs where DQFlagList=Tracker_Global=GOOD&Tracker_Local1<=1"
        result = ql.params(q)['conditions']
        expect = [{'value': 'Tracker_Global=GOOD&Tracker_Local1<=1', 'key': 'DQFlagList', 'op': '='}]
        self.assertEqual(result, expect)

        q = "find dataset where ((run=1 or run=2) or dataset=bla) and site=cern order by block"
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
        
        q = "find phedex:block where block=bla"
        result = ql.params(q)
        expect = {'functions': {}, 'selkeys': ['block'], 
                'unique_services': ['dbs', 'phedex'], 'order_by_list': [], 
                'order_by_order': None, 
                'services': {'phedex': ['block'], 'dbs': ['block']}, 
                'query' : 'find block where block=bla',
                'dasqueries': {'phedex': ['find block where block = bla'],
                               'dbs': ['find block where block = bla']}, 
                'conditions': [{'value': 'bla', 'key': 'block', 'op': '='}], 
                'allkeys': ['block'], 'unique_keys': ['block', 'file']}
        self.assertEqual(result, expect)

        q = "find dbs:block where block=bla"
        result = ql.params(q)
        expect = {'functions': {}, 'selkeys': ['block'], 
                'unique_services': ['dbs', 'phedex'], 'order_by_list': [], 
                'dasqueries': {'dbs': ['find block where block = bla'], 
                               'phedex': ['find block where block = bla']}, 
                'order_by_order': None, 
                'query': 'find block where block=bla',
                'services': {'dbs': ['block'], 'phedex': ['block']}, 
                'conditions': [{'value': 'bla', 'key': 'block', 'op': '='}], 
                'allkeys': ['block'], 'unique_keys': ['block', 'file']}
        self.assertEqual(result, expect)

        q = "find jobsummary where date last 25h"
        self.assertRaises(Exception, ql.params, q)

        q = "find jobsummary where date last 61m"
        self.assertRaises(Exception, ql.params, q)

        q = "find jobsummary where date last 61s"
        self.assertRaises(Exception, ql.params, q)

#
# main
#
if __name__ == '__main__':
    unittest.main()
