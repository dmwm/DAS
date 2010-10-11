#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS PLY parser/lexer
"""

import time
import ply.yacc
import unittest
import traceback
from   DAS.utils.das_db import db_connection, make_uri
from   DAS.utils.das_config import das_readconfig

class testDASPLY(unittest.TestCase):
    """
    A test class for the DAS PLY parser/lexer
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.debug = 0
        dasconfig = das_readconfig()
        self.dbhost = dasconfig['mongodb']['dbhost']
        self.dbport = dasconfig['mongodb']['dbport']

    def test_make_uri(self):
        """Test DAS PLY lexer"""
        pairs  = [('localhost', 27017), ('a.b.com', 8888)]
        result = make_uri(pairs)
        expect = 'mongodb://localhost:27017,a.b.com:8888'
        self.assertEqual(expect, result)

        pairs  = [('localhost', 27017)]
        result = make_uri(pairs)
        expect = 'mongodb://localhost:27017'
        self.assertEqual(expect, result)

        pairs = [('localhost', 1.1)]
        self.assertRaises(Exception, make_uri, pairs)

        pairs = [('localhost', 10)]
        self.assertRaises(Exception, make_uri, pairs)

    def test_db_connection(self):
        """Test db_connection"""
        result = db_connection(self.dbhost, self.dbport)
        expect = result.instance

        result = db_connection(self.dbhost, self.dbport)
        self.assertEqual(expect, result.instance)

#
# main
#
if __name__ == '__main__':
    unittest.main()
