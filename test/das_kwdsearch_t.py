#!/usr/bin/env python
# -*- coding: utf-8 -*-
#pylint: disable-msg=C0103, W0703, C0111, W0511

"""
tests for dataset wildcard processing
"""
# system modules

import unittest
import doctest

# DAS modules

from   DAS.core.das_query import DASQuery
from DAS.core.das_process_dataset_wildcards import get_global_dbs_mngr
from DAS.keywordsearch import search as kwdsearch_module
from DAS.keywordsearch.search import search, init as init_kws, keyword_schema_weights, keyword_value_weights

from DAS.core.das_core import DASCore


#global n_queries, n_queries_passed_at_1, n_queries_passed_at
n_queries = 0
n_queries_passed_at_1 = 0
n_queries_passed_at = {}

from pprint import pformat

class TestDASDatasetWildcards(unittest.TestCase):
    global_dbs_inst = False

    def setUp(self):
        """
        sets up dbs manager instance
        """


        # set up only once
        if not self.global_dbs_inst:
            print 'setUp: getting dbs manager to access current datasets ' \
                  '(and fetching them if needed)'
            self.global_dbs_inst = get_global_dbs_mngr(update_required=False)

            self.dascore = DASCore()
            init_kws(self.dascore)





    def assertQueryResult(self, query, expected_result, query_complexity = 'general'):
        '''
        run a test query, and gather statistics
        '''
        global n_queries, n_queries_passed_at_1, n_queries_passed_at
        n_queries += 1

        results = search(query, dbsmngr=self.global_dbs_inst)
        first_result = results[0]['result']


        if first_result == expected_result:
            n_queries_passed_at_1 += 1

        # count queries that contained expected answer not lower than at i-th position
        passed_at_i = False
        for i in xrange(0, 10):
            if i < len(results) and results[i]['result'] == expected_result:
                passed_at_i = True

            if passed_at_i:
                n_queries_passed_at[i] = n_queries_passed_at.get(i, 0) + 1


        # TODO: print distribution

        print 'Test: ', query, '. Result: ', first_result == expected_result
        print 'Queries so far:', n_queries, 'Passed at #1: ', n_queries_passed_at_1, 'Passed at i-th:', pformat(n_queries_passed_at)


        self.assertEquals(first_result, expected_result)


    def test_doctests(self):
        """
        runs doctests defined in DAS.core.das_process_dataset_wildcards
        """
        # pass dbs manager

        glob = kwdsearch_module.__dict__.copy()
        glob['dbsmgr'] = self.global_dbs_inst

        # run the tests
        (n_failures, n_tests) = \
            doctest.testmod(globs = glob, verbose=True, m=kwdsearch_module)

        self.assertEquals(n_failures, 0)


    def test_matching(self):
        if False:
            # we can see difflib by itself is quite bad. we must penalize mutations, insertions, etc. containment is the best
            print keyword_schema_weights('configuration') # shall be close to config
            print '! time:'
            print keyword_schema_weights('time')
            print '! location:'
            print keyword_schema_weights('location')
            # e.g. 'location of /Smf/smf/smf (dataset)
            print keyword_schema_weights('email')

            # TODO: use either presentation map, or entity.attribute
            print 'file name:', keyword_schema_weights('file name')


            #print search('vidmasze@cern.ch')
            print 'expect email:', keyword_value_weights('vidmasze@cern.ch')
            print 'expect dataset:', keyword_value_weights('/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO')
            print 'expect file:', keyword_value_weights('/store/backfill/1/T0TEST_532p1Run2012C_BUNNIES/DoubleMu/RAW-RECO/Zmmg-PromptSkim-v1/000/196/363/00000/DEBD64D4-A4C0-E111-A042-002618943826.root')






    def test_operators(self):
        if False:
            self.assertQueryResult('unique lumi flags in run 176304',
                'lumi  run=176304 | grep lumi.flag | unique')

            # TODO: Pattern: op, op, op entity(PK)|entity.field
            self.assertQueryResult('min, max, avg lumis in run 176304',
                'lumi  run=176304 | min(lumi.number), max(lumi.number), avg(lumi.number)')
            # TODO: even I made a mistake, by adding grep to min,max...

            'file dataset=/HT/Run2011B-v1/RAW run=176304 lumi=80'
            #fails:
            'lumi dataset=/HT/Run2011B-v1/RAW run=176304 lumi=80'

            'lumi  run=176304 lumi=80'


    def test_numeric_params(self):
        # values closer to the field name shall be preferred
        self.assertQueryResult('lumis in run 176304', 'lumi run=176304')

        # TODO: field 'is' value --> a good pattern?
        self.assertQueryResult('files in /HT/Run2011B-v1/RAW where run is 176304 lumi is 80',
                'file dataset=/HT/Run2011B-v1/RAW run=176304 lumi=80')



    def test_operators(self):
        # operators are not implemented yet

        if False:
            self.assertQueryResult('total number of files in Zmm',
                'file dataset=/a/b/c | count(file.name)')

            self.assertQueryResult('count of files in /DoubleMuParked25ns/*/*',
                'file dataset=/DoubleMuParked25ns/*/* | count(file.name)')
            self.assertQueryResult('count of conflicting files in /DoubleMuParked25ns/*/*',
                'file dataset=/DoubleMuParked25ns/*/* | count(das.conflict)')

    def test_das_QL(self):
        self.assertQueryResult('files of dataset=DoubleMuParked25ns',
            'file dataset=/DoubleMuParked25ns/*/*')

        #assert search('files in /DoubleMuParked25ns/*/* | count(das.conflict')[0] == 'file dataset=/DoubleMuParked25ns/*/* | count(das.conflict)'

    def test_das_key_synonyms(self):
        self.assertQueryResult('location of *Run2012*PromptReco*/AOD',
                               'site dataset=*Run2012*PromptReco*/AOD')


    def test_dataset_wildcards(self):
        # make sure 'dataset' is matched into entity but not its value (dataset=*dataset*)
        self.assertQueryResult('location of dataset *Run2012*PromptReco*/AOD',
            'site dataset=*Run2012*PromptReco*/AOD')

        # automatically adding wildcards
        self.assertQueryResult('location of Zmm',
        'site dataset=*Zmm*')



    def test_value_based(self):
        self.assertQueryResult(u'datasets at T1_CH_CERN',
            'dataset site=T1_CH_CERN')
        self.assertQueryResult(u'datasets at T1_CH_*',
            'dataset site=T1_CH_*')

    def test_interesting_queries(self):
        if False:
            self.assertQueryResult('when  was run=20853 taken?',
                   'run=20853 | grep run.start_time, run.end_time')

            self.assertQueryResult('magnetic field of run=20853',
                   'run=20853 | grep run.bfield' )

            self.assertQueryResult('administrator email of all T1 sites',
                                   'site site=T1* | grep site.admin.email, site.name')

            # TODO: requires ontology run reco_status=1
            # reconstructed runs (reco_status=1) ...  (1800 results only?)

            # run.start_time, run.end_time


            'administrator email of T1_CH_CERN' # works
            'administrator email of all T1_* sites'


    def test_prefer_filtering_input(self):
        # TODO: currently we are overranking the result filters the top result is:
        # summary run=150619 | grep summary.dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO
        self.assertQueryResult('summary dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO  run 150619',
                               'summary dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO run=150619')



    def test_inputs_vs_postfilters(self):
        # Result #5 currently
        self.assertQueryResult('files of /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO  located at site T1_*',
                               'file dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO site=T1_*')


        # currently #4
        self.assertQueryResult('files of /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO  at site T1_*',
                               'file dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO site=T1_*')


        # the query is quite ambigous...
        self.assertQueryResult('lumis in run 176304',  'lumi run=176304')

    def test_inputs_non_existing_dataset(self):
        self.assertQueryResult('/DoubleMu/Run2012A-Zmmg-13Jul2012-v1xx/RAW-RECO',
                               'dataset dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1xx/RAW-RECO')


    def test_postfilters(self):
        # 1
        self.assertQueryResult('Zmmg magnetic field>3.5',
                               'run dataset=*Zmmg* | grep run.run_number, run.bfield>3.5')

    def test_result_field_selections(self):

        self.assertQueryResult('Zmmg magnetic field',
                               'run dataset=*Zmmg* | grep run.bfield, run.run_number')

        self.assertQueryResult('Zmmg custodial file replicas',
                               'file dataset=*Zmmg* | grep file.replica.custodial, file.name')

        self.assertQueryResult('Zmmg custodial block replicas',
                               'block dataset=*Zmmg* | grep block.replica.custodial, block.name')

        self.assertQueryResult('number of lumis in run 176304',
                               'summary run=176304 | grep summary.nlumis')

    def test_result_field_selections_harder(self):
        #2nd
        self.assertQueryResult('delivered lumimosity in run 176304',
                               'run run=176304 | grep run.delivered_lumi')



    def test_imperative(self):
        # 5
        self.assertQueryResult('tell me where is dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO located',
                               'site dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO')


    def test_wh_words(self):
        self.assertQueryResult(
               'where is dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO located',
               'site dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO'
        )

        self.assertQueryResult(
            'where are Zmmg',
            'site dataset=*Zmmg*'
        )




    def test_basic_queries(self):
        self.assertQueryResult(
            'configuration /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO',
            'config dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO')

        self.assertQueryResult('configuration of /*Zmm*/*/*',
                     'config dataset=/*Zmm*/*/*')


        self.assertQueryResult('/*Zmm*/*/*', 'dataset dataset=/*Zmm*/*/*')

        self.assertQueryResult('name of vidmasze@cern.ch', '')

        self.assertQueryResult('last name of vidmasze@cern.ch', '')


if __name__ == '__main__':
    unittest.main()
