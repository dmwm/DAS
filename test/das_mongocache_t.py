#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS mongocache class
"""

import os
import re
import time
import unittest

from pymongo.connection import Connection

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

        connection = Connection(dburi)
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

    def test_similar_queries_2(self):                          
        """test similar_queries method of DASMongoCache"""
        query1 = DASQuery({'fields':None, 'spec':{'block.name':'ABC'}})
        self.dasmongocache.col.insert({"query":query1.storage_query, "qhash":query1.qhash})
        query2 = DASQuery({'fields':None, 'spec':{'block.name':'ABC*'}})
        result = self.dasmongocache.similar_queries(query2)
        self.assertEqual(False, result)
        self.dasmongocache.delete_cache()

        query1 = DASQuery({'fields':None, 'spec':{'block.name':'ABC*'}})
        self.dasmongocache.col.insert({"query":query1.storage_query, "qhash":query1.qhash})
        query2 = DASQuery({'fields':None, 'spec':{'block.name':'ABC'}})
        result = self.dasmongocache.similar_queries(query2)
        self.assertEqual(False, result)
        self.dasmongocache.delete_cache()

        query1 = DASQuery('block=ABC')
        self.dasmongocache.col.insert({"query":query1.storage_query, "qhash":query1.qhash})
        query2 = DASQuery('block=ABC | grep block.name')
        result = self.dasmongocache.similar_queries(query2)
        self.assertEqual(query1, result)
        self.dasmongocache.delete_cache()

    def test_similar_queries(self):                          
        """test similar_queries method of DASMongoCache"""
        query1 = DASQuery({'fields':None, 'spec':{'block.name':'ABC#123'}})
        self.dasmongocache.col.insert({"query":query1.storage_query, "qhash":query1.qhash})
        query2 = DASQuery({'fields':None, 'spec':{'block.name':'ABC'}})
        result = self.dasmongocache.similar_queries(query2)
        self.assertEqual(False, result)
        self.dasmongocache.delete_cache()

        query1 = DASQuery({'fields':None, 'spec':{'dataset.name':'ABC'}})
        self.dasmongocache.col.insert({"query":query1.storage_query, "qhash":query1.qhash})
        query2 = DASQuery({'fields':['dataset'], 'spec':{'dataset.name':'ABC'}})
        result = self.dasmongocache.similar_queries(query2)
        self.assertEqual(False, result)
        self.dasmongocache.delete_cache()

        query1 = DASQuery({'fields':None, 'spec':{'block.name':'ABCDE*'}})
        self.dasmongocache.col.insert({"query":query1.storage_query, "qhash":query1.qhash})
        query2 = DASQuery({'fields':None, 'spec':{'block.name':'ABCDEFG'}})
        result = self.dasmongocache.similar_queries(query2)
        self.assertEqual(False, result)
        self.dasmongocache.delete_cache()

        query1 = DASQuery({'fields':None, 'spec':{'block.name':'ABCDEFG'}})
        self.dasmongocache.col.insert({"query":query1.storage_query, "qhash":query1.qhash})
        query2 = DASQuery({'fields':None, 'spec':{'block.name':'ABCDE*'}})
        result = self.dasmongocache.similar_queries(query2)
        self.assertEqual(False, result)
        self.dasmongocache.delete_cache()

        query1 = DASQuery({'fields':None, 'spec':{'run.number':20853}})
        self.dasmongocache.col.insert({"query":query1.storage_query, "qhash":query1.qhash})
        query2 = DASQuery({'fields':None, 'spec':{'run.number': {'$gte':20853, '$lte':20859}}})
        result = self.dasmongocache.similar_queries(query2)
        self.assertEqual(False, result)
        self.dasmongocache.delete_cache()

        query1 = DASQuery({'fields':None, 'spec':{'run.number': {'$gte':20853, '$lte':20859}}})
        self.dasmongocache.col.insert({"query":query1.storage_query, "qhash":query1.qhash})
        query2 = DASQuery({'fields':None, 'spec':{'run.number':20853}})
        result = self.dasmongocache.similar_queries(query2)
        self.assertEqual(False, result)
        self.dasmongocache.delete_cache()
#
# main
#
if __name__ == '__main__':
    unittest.main()

