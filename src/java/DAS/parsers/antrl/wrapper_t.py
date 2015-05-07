#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for ANTLR parser
"""
from __future__ import absolute_import

import unittest

def antrlparser(uinput):
    """
    Parser based on ANTRL. It returns the following dict for
    query = find dataset, run where site = T2
    {'ORDERING': [], 
     'FIND_KEYWORDS': ['dataset', 'run'], 
     'ORDER_BY_KEYWORDS': [], 
     'WHERE_CONSTRAINTS': [{'value': 'T2', 'key': 'site', 'op': '='}, {'bracket': 'T2'}]}
    """
    from .Wrapper import Wrapper
    parserobj = Wrapper()
    tokens = parserobj.parseQuery(uinput)
    return tokens

class testANTLRParser(unittest.TestCase):
    """
    A test class for ANTLR parser
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
        q = "find dataset, run where site = T2_UK"
        res = antrlparser(q)
        r = {'ORDERING': [], 'FIND_KEYWORDS': ['dataset', 'run'], 
             'ORDER_BY_KEYWORDS': [], 
             'WHERE_CONSTRAINTS': [{'value': 'T2_UK', 'key': 'site', 'op': '='}]}
        self.assertEqual(res, r)
        
#
# main
#
if __name__ == '__main__':
    unittest.main()

