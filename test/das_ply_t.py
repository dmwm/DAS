#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS PLY parser/lexer
"""

import time
import ply.yacc
import unittest
import traceback
from   DAS.core.das_ply import DASPLY, ply2mongo

class testDASPLY(unittest.TestCase):
    """
    A test class for the DAS PLY parser/lexer
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.debug = 0
        daskeys = ['dataset', 'file', 'block', 'run', 'site', 
                   'latitude', 'longitude', 'city', 'ip', 'date', 'system', 'zip']

        self.dasply = DASPLY(daskeys, self.debug)
#        args = {'debug':self.debug, 'errorlog' : ply.yacc.NullLogger()}
        args = {'errorlog' : ply.yacc.NullLogger()}
        self.dasply.build(**args)

        self.queries = {}

        query = "zip=10000 | grep zip.Placemark.address | count(zip.Placemark.address)"
        mongo = {'fields': None, 'spec': {'zip': 10000}, 
                 'filters': ['zip.Placemark.address'],
                 'aggregators': [('count', 'zip.Placemark.address')] }
        self.queries[query] = mongo

        query = "city=Ithaca"
        mongo = {'fields': None, 'spec': {'city': 'Ithaca'}}
        self.queries[query] = mongo

        query = "zip=14850"
        mongo = {'fields': None, 'spec': {'zip': 14850}}
        self.queries[query] = mongo

        query = "ip=137.138.141.145 | grep ip.City"
        mongo = {'fields': None, 'spec': {'ip': '137.138.141.145'}, 'filters': ['ip.City']}
        self.queries[query] = mongo

        query = 'latitude=11.1 longitude=-72'
        mongo = {'fields': None, 'spec':{'latitude':11.1, 'longitude': -72}}
        self.queries[query] = mongo

        query = "site=T1_CH_CERN"
        mongo = {'fields': None, 'spec': {'site': 'T1_CH_CERN'}}
        self.queries[query] = mongo

        query = "dataset"
        mongo = {'fields': ['dataset'], 'spec': {'dataset': '*'}}
        self.queries[query] = mongo

        query = "run=20853"
        mongo = {'fields': None, 'spec': {'run': 20853}}
        self.queries[query] = mongo

        query = "run in [20853,20859]"
        mongo = {'fields': None, 'spec': {'run': {'$in': [20853, 20859]}}}
        self.queries[query] = mongo

        query = "run between [20853,20859]"
        mongo = {'fields': None, 'spec': {'run': {'$gte': 20853, '$lte': 20859}}}
        self.queries[query] = mongo

        query = "file block=123 | grep file.size | sum(file.size)"
        mongo = {'fields': ['file'], 'spec': {'block': 123}, 'filters': ['file.size'],
                 'aggregators': [('sum', 'file.size')]}
        self.queries[query] = mongo

        query = "block=/a/b/RECO#9f5c396b-b6a1"
        mongo = {'fields': None, 'spec': {'block': '/a/b/RECO#9f5c396b-b6a1'}}
        self.queries[query] = mongo

        query = "block dataset=/W/a_2/RECO"
        mongo = {'fields': ['block'], 'spec': {'dataset': '/W/a_2/RECO'}}
        self.queries[query] = mongo

        query = "run date last 24h"
        date1 = time.time() - 24*60*60
        date2 = time.time()
        mongo = {'fields': ['run'], 'spec': {'date': {'$in': [long(date1), long(date2)]}}}
        self.queries[query] = mongo

        query = "dataset file=/a/b run in [1,2] | grep file.name, file.age | unique | sum(file.size),max(file.size)"
        mongo = {'fields': ['dataset'], 'spec': {'run': {'$in': [1, 2]}, 'file': '/a/b'}, 
                 'filters': ['file.name', 'file.age'],
                 'aggregators': [('sum', 'file.size'), ('max', 'file.size')]}
        self.queries[query] = mongo


    def test_lexer(self):
        """Test DAS PLY lexer"""
        for query, expect in self.queries.items():
            if  self.debug:
                print "\n%s" % query
            self.dasply.test_lexer(query)

    def test_parser(self):
        """Test DAS PLY parser"""
        for query, expect in self.queries.items():
            try:
                ply_query = self.dasply.parser.parse(query)
            except:
                print "Input query:", query
                raise
            if  self.debug:
                print
                print "input query", query
                print "ply query  ", ply_query
            result = ply2mongo(ply_query)
            self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()
