#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS PLY parser/lexer
"""
from __future__ import print_function

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
        dassystems = ['dbs', 'sitedb', 'phedex', 'google_maps', 
                      'postalcode', 'ip_services']
        daskeys = ['dataset', 'file', 'block', 'run', 'site', 
                   'latitude', 'longitude', 'city', 'ip', 'date', 'system', 'zip']
        parserdir = '/tmp'

        self.dasply = DASPLY(parserdir, daskeys, dassystems, verbose=self.debug)
        args = {'errorlog' : ply.yacc.NullLogger()}
        self.dasply.build(**args)

        self.queries = {}

        query = "queries"
        mongo = {'fields': ['queries'], 'spec': {'queries': '*'}}
        self.queries[query] = mongo

        query = "popular queries"
        mongo = {'fields': ['popular', 'queries'], 'spec': {'queries': '*'}}
        self.queries[query] = mongo

        query = "popular queries date last 24h"
        mongo = {'fields': ['popular', 'queries'], 'spec': {'date': '24h'}}
#        date1 = time.time() - 24*60*60
#        date2 = time.time()
#        mongo = {'fields': ['popular', 'queries'], 
#                 'spec': {'date': {'$gte': long(date1), '$lte': long(date2)}}}
        self.queries[query] = mongo

        query = "records"
        mongo = {'fields': ['records'], 'spec': {'records': '*'}}
        self.queries[query] = mongo

        query = "records site=T1_CH_CERN"
        mongo = {'fields': ['records'], 'spec': {'site': 'T1_CH_CERN'}}
        self.queries[query] = mongo

        query = "site=T1_CH_CERN"
        mongo = {'fields': ['site'], 'spec': {'site': 'T1_CH_CERN'}}
        self.queries[query] = mongo

        query = "site site=T1_CH_CERN"
        mongo = {'fields': ['site'], 'spec': {'site': 'T1_CH_CERN'}}
        self.queries[query] = mongo

        query = "site site=srm-cms.cern.ch"
        mongo = {'fields': ['site'], 'spec': {'site': 'srm-cms.cern.ch'}}
        self.queries[query] = mongo

        query = "site site=cmssrm.fnal.gov"
        mongo = {'fields': ['site'], 'spec': {'site': 'cmssrm.fnal.gov'}}
        self.queries[query] = mongo

        query = "site=T1_CH_CERN site"
        mongo = {'fields': ['site'], 'spec': {'site': 'T1_CH_CERN'}}
        self.queries[query] = mongo

        query = "dataset=/a/b/c run=123 | grep dataset.size"
        mongo = {'filters': {'grep': ['dataset.size']}, 'fields': None, 
                 'spec': {'dataset': '/a/b/c', 'run': 123}}
        self.queries[query] = mongo

        query = "site=T1_CH_CERN system=sitedb"
        mongo = {'fields': ['site'], 'spec': {'site': 'T1_CH_CERN'}, 'system': 'sitedb'}
        self.queries[query] = mongo

        query = "zip=10000 | grep zip.Placemark.address | count(zip.Placemark.address)"
        mongo = {'fields': ['zip'], 'spec': {'zip': 10000}, 
                 'filters': {'grep': ['zip.Placemark.address']},
                 'aggregators': [('count', 'zip.Placemark.address')] }
        self.queries[query] = mongo

        query = "city=Ithaca"
        mongo = {'fields': ['city'], 'spec': {'city': 'Ithaca'}}
        self.queries[query] = mongo

        query = "zip=14850"
        mongo = {'fields': ['zip'], 'spec': {'zip': 14850}}
        self.queries[query] = mongo

        query = "ip=137.138.141.145 | grep ip.City"
        mongo = {'fields': ['ip'], 'spec': {'ip': '137.138.141.145'},
                 'filters': {'grep': ['ip.City']}}
        self.queries[query] = mongo

        query = 'latitude=11.1 longitude=-72'
        mongo = {'fields': None, 'spec':{'latitude':11.1, 'longitude': -72}}
        self.queries[query] = mongo

        query = "site=T1_CH_CERN"
        mongo = {'fields': ['site'], 'spec': {'site': 'T1_CH_CERN'}}
        self.queries[query] = mongo

        query = "run=20853"
        mongo = {'fields': ['run'], 'spec': {'run': 20853}}
        self.queries[query] = mongo

        query = "run between [20853,20859]"
        mongo = {'fields': ['run'], 'spec': {'run': {'$gte': 20853, '$lte': 20859}}}
        self.queries[query] = mongo

        query = "file block=123 | grep file.size | sum(file.size)"
        mongo = {'fields': ['file'], 'spec': {'block': 123},
                 'filters': {'grep': ['file.size']},
                 'aggregators': [('sum', 'file.size')]}
        self.queries[query] = mongo

        query = "block=/a/b/RECO#9f5c396b-b6a1"
        mongo = {'fields': ['block'], 'spec': {'block': '/a/b/RECO#9f5c396b-b6a1'}}
        self.queries[query] = mongo

        query = "block dataset=/W/a_2/RECO"
        mongo = {'fields': ['block'], 'spec': {'dataset': '/W/a_2/RECO'}}
        self.queries[query] = mongo

        query = "run date last 24h"
#        date1 = time.time() - 24*60*60
#        date2 = time.time()
#        mongo = {'fields': ['run'], 'spec': {'date': {'$gte': long(date1), '$lte': long(date2)}}}
        mongo = {'fields': ['run'], 'spec': {'date': '24h'}}
        self.queries[query] = mongo

        date1 = 20101201
        date2 = 20101202
        query = "run date between [%s, %s]" % (date1, date2)
        mongo = {'fields': ['run'], 'spec': {'date': {'$lte': long(1291248000), '$gte': long(1291161600)}}}
        self.queries[query] = mongo

        query = "dataset file=/a/b run between [1,2] | grep file.name, file.age | unique | sum(file.size),max(file.size)"
        mongo = {'fields': ['dataset'], 'spec': 
                        {'run': {'$lte': 2, '$gte': 1}, 'file': '/a/b'}, 
                 'filters': {'grep': ['file.name', 'file.age'], 'unique': 1},
                 'aggregators': [('sum', 'file.size'), ('max', 'file.size')]}
        self.queries[query] = mongo
        
        query = "city = camelCase"
        mongo = {'fields': ['city'], 'spec':{'city': 'camelCase'}}
        self.queries[query] = mongo
        
        query = "city = lowercase"
        mongo = {'fields': ['city'], 'spec':{'city': 'lowercase'}}
        self.queries[query] = mongo
        
        query = "city = 'two words'"
        mongo = {'fields': ['city'], 'spec':{'city': 'two words'}}
        self.queries[query] = mongo
        
        query = 'city = "two words"'
        mongo = {'fields': ['city'], 'spec':{'city': 'two words'}}
        self.queries[query] = mongo
        
        #query=DASKEY
        query = 'city = dataset'
        mongo = {'fields': ['city'], 'spec':{'city': 'dataset'}}
        self.queries[query] = mongo
        
        #query=DASKEYtext
        query = 'city = datasetPostfix'
        mongo = {'fields': ['city'], 'spec':{'city': 'datasetPostfix'}}
        self.queries[query] = mongo
        
        #query=OPERATORtext (I don't expect query=OPERATOR to ever work)
        query = 'city = betweenPostfix'
        mongo = {'fields': ['city'], 'spec':{'city': 'betweenPostfix'}}
        self.queries[query] = mongo

        # query w/ filter which contains a key/value pair
        query = 'block=/a/b/c | grep site=T1 '
        mongo = {'fields': ['block'], 'spec': {'block': '/a/b/c'},
                 'filters': {'grep': ['site=T1']}}
        self.queries[query] = mongo

        # query w/ filter which contains a filter conditions
        query = 'run dataset=/a/b/c | grep run.run_number>1, run.run_number<10 '
        mongo = {'fields': ['run'], 'spec': {'dataset': '/a/b/c'}, 
                 'filters': {'grep': ['run.run_number>1', 'run.run_number<10']}}
        self.queries[query] = mongo

        # query w/ filter which contains a filter conditions
        query = 'run dataset=/a/b/c | grep run.a>0, run.b>=0, run.c<=0'
        mongo = {'fields': ['run'], 'spec': {'dataset': '/a/b/c'}, 
                 'filters': {'grep': ['run.a>0', 'run.b>=0', 'run.c<=0']}}
        self.queries[query] = mongo

        # query with DASKEY, date=value
        query = 'dataset date=20110124'
        mongo = {'fields': ['dataset'], 'spec': {'date': 1295827200}}
        self.queries[query] = mongo
        
        # query with DASKEY, date between [value1, value2]
        query = 'dataset date between [20110124,20110126]'
        mongo = {'fields': ['dataset'], 'spec': {'date': {'$gte': 1295827200, '$lte': 1296000000}}}
        self.queries[query] = mongo


        query = 'file=abcdeasdf'
        mongo = {'fields': ['file'], 'spec': {'file': 'abcdeasdf'}}
        self.queries[query] = mongo

        query = 'file=abcdeasdf dataset=abcdes'
        mongo = {'fields': None, 'spec': {'file': 'abcdeasdf', 'dataset': 'abcdes'}}
        self.queries[query] = mongo

        query = 'dataset date = 20080201'
        mongo = {'fields': ['dataset'], 'spec': {'date': 1201824000}}
        self.queries[query] = mongo

        query = 'file dataset date = 20080201'
        mongo = {'fields': ['file', 'dataset'], 'spec': {'date': 1201824000}}
        self.queries[query] = mongo

        query = 'dataset dataset=abcdes date = 20080201'
        mongo = {'fields': ['dataset'], 'spec': {'date': 1201824000, 'dataset': 'abcdes'}}
        self.queries[query] = mongo

        query = 'file dataset dataset=abcdes date = 20080201'
        mongo = {'fields': ['file', 'dataset'], 'spec': {'date': 1201824000, 'dataset': 'abcdes'}}
        self.queries[query] = mongo

        query = 'file=abcdeasdf file dataset dataset=abcdes date = 20080201'
        mongo = {'fields': ['file', 'dataset'], 'spec': {'date': 1201824000, 'file': 'abcdeasdf', 'dataset': 'abcdes'}}
        self.queries[query] = mongo

        query = 'file dataset=bla | grep file.creation_time<20080201'
        mongo = {'fields': ['file'], 'spec': {'dataset': 'bla'},
                 'filters': {'grep': ['file.creation_time<1201824000']}}
        self.queries[query] = mongo

    def test_instance(self):
        """Test appearance of instance in a DAS query"""
        query = 'dataset=/a/b/c instance=global'
        mongo = {'fields': ['dataset'], 'spec': {'dataset':'/a/b/c'}, 'instance':'global'}
        ply_query = self.dasply.parser.parse(query)
        result = ply2mongo(ply_query)
        self.assertEqual(mongo, result)

    def test_lexer(self):
        """Test DAS PLY lexer"""
        for query, expect in self.queries.items():
            if  self.debug:
                print("\n%s" % query)
            self.dasply.test_lexer(query)

    def test_parser(self):
        """Test DAS PLY parser"""
        for query, expect in self.queries.items():
            try:
                ply_query = self.dasply.parser.parse(query)
            except:
                self.dasply.parser.parse(query, debug=1)
                print("Input query:", query)
                raise
            if  self.debug:
                print()
                print("input query", query)
                print("ply query  ", ply_query)
            result = ply2mongo(ply_query)
            self.assertEqual(expect, result)

    def test_parser_negate(self):
        """Test DAS PLY parser with negative results"""
        mongo = {}
        queries = {}

        query = 'run last 24h'
        queries[query] = mongo

        query = 'run last dataset'
        queries[query] = mongo

        query = 'dateset in 2010'
        queries[query] = mongo

        query = 'date in 24h'
        queries[query] = mongo

        query = 'date last [20101010,20101012]'
        queries[query] = mongo

        query = 'dataset in [/a/b/c,/c/d/e]'
        queries[query] = mongo

        query = 'detaset = /a/b/c'
        queries[query] = mongo

        query = 'detaset = /a/b/c dataset.size'
        queries[query] = mongo

        query = """dataset date in [20110124,20110126]"""
        queries[query] = mongo

        query = "run in [20853,20859]"
        self.queries[query] = mongo

        query = "dataset" # prevent usage of single keys
        self.queries[query] = mongo

        for query, expect in queries.items():
            self.assertRaises(Exception, self.dasply.parser.parse, query)

#
# main
#
if __name__ == '__main__':
    unittest.main()
