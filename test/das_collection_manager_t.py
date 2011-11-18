#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DASCollectionManager class
"""

import os
import re
import time
import unittest

from pymongo.connection import Connection
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import PrintManager
from DAS.core.das_collection_manager import make_dbname, DASCollectionManager

class testDASCollectionManager(unittest.TestCase):
    """
    A test class for the DASCollectionManager class
    """
    def setUp(self):
        """
        set up DAS
        """
        debug    = 0
        config   = das_readconfig()
        logger   = PrintManager('TestDASCollectionManager', verbose=debug)
        config['logger']  = logger
        config['verbose'] = debug
        dburi    = config['mongodb']['dburi']

        self.conn = Connection(dburi)
        self.conn.drop_database('das') 
        self.coll_mgr = DASCollectionManager(config)

    def test_dbname(self):
        """Test make_dbname function"""
        query  = {'fields': None, 'spec': {'block.name':'a'}}
        result = make_dbname(query)
        expect = 'block'
        self.assertEqual(expect, result)

        query  = {'fields': None, 'spec': {'block.name':'a', 'dataset.name':1}}
        result = make_dbname(query)
        expect = 'block_dataset'
        self.assertEqual(expect, result)

        query  = {'fields': ['dataset'], 'spec': {'dataset.name':'a', 'block.name':1}}
        result = make_dbname(query)
        expect = 'block_dataset'
        self.assertEqual(expect, result)

    def test_collections(self):
        """Test collection creation methods"""
        query  = {'fields': None, 'spec': {'block.name':'aaa'}}
        coll   = self.coll_mgr.cache(query)
        dbname = coll.database.name
        expect = set(self.conn.database_names()) & set([dbname])
        self.assertEqual(expect, set([dbname]))

        coll   = self.coll_mgr.merge(query)
        dbname = coll.database.name
        expect = set(self.conn.database_names()) & set([dbname])
        self.assertEqual(expect, set([dbname]))

#
# main
#
if __name__ == '__main__':
    unittest.main()

