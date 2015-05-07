#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS mongocache class
"""

import os
import re
import time
import unittest

from pymongo import MongoClient

from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import PrintManager
from DAS.utils.utils import deepcopy
from DAS.core.das_query import DASQuery
from DAS.core.das_mapping_db import DASMapping
from DAS.core.das_mongocache import DASMongocache
from DAS.core.das_mongocache import update_query_spec

class testDASMongocache(unittest.TestCase):
    """
    A test class for the DAS mongocache class
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug    = 0
        config   = deepcopy(das_readconfig())
        logger   = PrintManager('TestDASMongocache', verbose=debug)
        config['logger']  = logger
        config['verbose'] = debug
        dburi    = config['mongodb']['dburi']

        connection = MongoClient(dburi)
        connection.drop_database('das') 
        dasmapping = DASMapping(config)
        config['dasmapping'] = dasmapping
        self.dasmongocache = DASMongocache(config)

    def test_update_query_spec(self):
        "Test update_query_spec function"
        spec   = {'a':1}
        result = update_query_spec(spec, {'a':10})
        expect = {'$and': [{'a':1}, {'a':10}]}
        self.assertEqual(expect, spec)

        result = update_query_spec(spec, {'a':100})
        expect = {'$and': [{'a':1}, {'a':10}, {'a':100}]}
        self.assertEqual(expect, spec)

#
# main
#
if __name__ == '__main__':
    unittest.main()

