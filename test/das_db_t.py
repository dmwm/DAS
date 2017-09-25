#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS db methods
"""

import time
import unittest
import traceback
from   DAS.utils.das_db import db_connection, make_uri, db_gridfs
from   DAS.utils.das_config import das_readconfig
from DAS.utils.utils import deepcopy

class testDAS_DB(unittest.TestCase):
    """
    A test class for the DAS db methods
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.debug  = 0
        dasconfig   = deepcopy(das_readconfig())
        self.dburi  = dasconfig['mongodb']['dburi']
        self.dbhost = 'localhost'
        self.dbport = 27017

    def test_make_uri(self):
        """Test DAS PLY lexer"""
        pairs  = [(self.dbhost, self.dbport), ('a.b.com', 8888)]
        result = make_uri(pairs)
        expect = 'mongodb://%s:%s,a.b.com:8888' % (self.dbhost, self.dbport)
        self.assertEqual(expect, result)

        pairs  = [(self.dbhost, self.dbport)]
        result = make_uri(pairs)
        expect = 'mongodb://%s:%s' % (self.dbhost, self.dbport)
        self.assertEqual(expect, result)

        pairs = [('localhost', 1.1)]
        self.assertRaises(Exception, make_uri, pairs)

        pairs = [('localhost', 10)]
        self.assertRaises(Exception, make_uri, pairs)

    def test_db_connection(self):
        """Test db_connection"""
        result = db_connection(self.dburi)
        expect = result.instance

        result = db_connection(self.dburi)
        self.assertEqual(expect, result.instance)

    def test_db_gridfs(self):
        """Test db_gridfs"""
        fsinst  = db_gridfs(self.dburi)
        doc     = 'hello world!'
        fid     = fsinst.put(doc)
        content = fsinst.get(fid).read()
        self.assertEqual(doc, content)
        fsinst.delete(fid)

        rec     = {'test': 1}
        self.assertRaises(Exception, fsinst.put, rec)
#
# main
#
if __name__ == '__main__':
    unittest.main()
