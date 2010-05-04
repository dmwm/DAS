#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import unittest
from DAS.core.qlparser import findbracketobj, mongo_exp
from DAS.core.qlparser import getconditions
from DAS.core.qlparser import MongoParser
from DAS.utils.logger import DASLogger
from DAS.utils.das_config import das_readconfig
from DAS.core.das_mapping_db import DASMapping
from DAS.core.das_analytics_db import DASAnalytics

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

        debug   = 0
        config  = das_readconfig()
        logger  = DASLogger(verbose=debug, stdout=debug)
        config['logger']  = logger
        config['verbose'] = debug
        config['mapping_dbhost'] = 'localhost'
        config['mapping_dbport'] = 27017
        config['mapping_dbname'] = 'mapping'
        config['dasmapping'] = DASMapping(config)
        config['dasanalytics'] = DASAnalytics(config)
        self.parser = MongoParser(config)

    def testBracketObj(self):                          
        """test search for bracket objects"""
        testlist = [
("vk or test ((test or test) or (another obj)) or test2", "((test or test) or (another obj))"),
("((test or test)) and (test or test)","((test or test))"),
]
        for q, r in testlist:
            obj = findbracketobj(q)
            self.assertEqual(obj, r)

    def test_parser(self):
        """Test Mongo parser"""
        cond   = {'site.se':'a.b.c', 'block.name':'bla'}
        query  = dict(spec=cond, fields='block')
        expect = ['sitedb', 'phedex', 'dbs', 'dbs3', 'phedex3']
        expect.sort()
        result = self.parser.services(query)
        result.sort()
        self.assertEqual(expect, result)

        expect = {'services': expect,
                  'selkeys': 'block', 'conditions': cond}
        result = self.parser.params(query)
        services = result['services']
        services.sort()
        result['services'] = services
        self.assertEqual(expect, result)

    def test_requestquery(self):
        """
        Test requestquery function.
        """
        query = 'block site=T1_CH_CERN'
        expect = {'fields': None, 
                  'spec': {'site.name': 'T1_CH_CERN', 'block.name':'*'}}
        result = self.parser.requestquery(query)
        self.assertEqual(expect, result)

        query = 'block site=a.b.c'
        expect = {'fields': None, 
                  'spec': {'site.se': 'a.b.c', 'block.name':'*'}}
        result = self.parser.requestquery(query)
        self.assertEqual(expect, result)

        query = 'file,site block=bla'
        expect = {'fields': None, 'spec': {'block.name': 'bla', 
                        'file.name':'*', 'site.name':'*', 'site.se':'*'}}
        result = self.parser.requestquery(query)
        self.assertEqual(expect, result)

        query = '{"fields": null, "spec": {"_id": "4aeef071e2194e3794000007"}}'
        expect = {'fields': None, 'spec': {'_id': '4aeef071e2194e3794000007'}}
        result = self.parser.requestquery(query)
        self.assertEqual(expect, result)

    def testFalseBracketObj(self):                          
        """false test for bracket objects"""
        q = "test (test1 (test2 or test3)"
        self.assertRaises(Exception, findbracketobj, q)

#
# main
#
if __name__ == '__main__':
    unittest.main()
