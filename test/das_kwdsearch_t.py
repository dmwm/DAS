#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
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
from DAS.keywordsearch.search import *


class TestDASDatasetWildcards(unittest.TestCase):
    global_dbs_inst = False

    def setUp(self):
        """
        sets up dbs manager instance
        """

        print 'setUp: getting dbs manager to access current datasets '\
              '(and fetching them if needed)'
        # set up only once
        if not self.global_dbs_inst:
            self.global_dbs_inst = get_global_dbs_mngr(update_required=False)

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



    def test_dasquery(self):
        """
        checks integration with DASQuery
        """

        # no dataset matching
        # more than more interpretation available
        # only one interpretation (standard query execution)

        # multiple interpretations
        msg = ''
        try:
            DASQuery('dataset Zmm', active_dbsmgr=self.global_dbs_inst)
        except Exception, exc:
            msg = str(exc)


        self.assertTrue('dataset dataset=*Zmm*' in msg)

        # TODO: check automatic loading of first best result



    def test_basic_queries(self):
        pass


    def test_operators(self):
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

        # TODO: why lumi do not work? a) there is no such API? b) lumi is PER RUN! c) RUN is lower?
        if False: self.assertQueryResult(
            'files /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO   run 12345 lumi 666702',
            'file dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO run=12345')

        # TODO: field 'is' value --> a good pattern?
        self.assertQueryResult('files in /HT/Run2011B-v1/RAW where run is 176304 lumi is 80',
                'file dataset=/HT/Run2011B-v1/RAW run=176304 lumi=80')


    def assertQueryResult(self, query, expected_result):
            self.assertEquals(search(query)[0][0], expected_result)


    def test_operators(self):
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

    def test_dataset_wildcards(self):
        self.assertQueryResult('location of *Run2012*PromptReco*/AOD',
            'site dataset=*Run2012*PromptReco*/AOD')

        # make sure 'dataset' is matched into entity but not its value (dataset=*dataset*)
        self.assertQueryResult('location of dataset *Run2012*PromptReco*/AOD',
            'site dataset=*Run2012*PromptReco*/AOD')

        # automatically adding wildcards
        self.assertQueryResult('location of Zmm',
        'site dataset=*Zmm*')

    def test_aggregation_with_wildcards(self):
        pass

    def test_selection_with_wildcards(self):
        pass


    def test_value_based(self):
        self.assertQueryResult('datasets at T1_CH_CERN',
            'dataset site=T1_CH_CERN')
        self.assertQueryResult('datasets at T1_CH_*',
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


            "administrator of T1_CH_CERN" # do not work


            'administrator email of T1_CH_CERN' # works
            'administrator email of all T1_* sites'


    def test_preffer_filtering_input(self):
        # TODO: currently we are overranking the result filters the top result is:
        # summary run=150619 | grep summary.dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO
        self.assertQueryResult('summary dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO  run 150619',
                               'summary dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO run=150619')


    def test_simple_crap(self):
        # TODO: can we use word sense disambiguation
        print search('configuration of dataset /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO which location is at T1_* ')
        # TODO: semantic similarity between different parts of speech (e.g. site, located)
        #give me config of dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO site=T1_*
        print search('configuration /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO')

        print search('configuration of /*Zmm*/*/*')

        print search('files of /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO  located at site T1_* ')

        # jobsummary  last 24h  --> jobsummary date last 24h
        # infer date

        """
        Not working queries:
        /*Zmm*/*/*
        /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO
        """

        print search('/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO')
        print search('/*Zmm*/*/*')

        # use stopwords
        # TODO: luminosity value is not mapped
        print search('files /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO   run 12345 lumi 666702')


        print search('name of vidmantas.zemleris@cern.ch')
        print search('username of vidmantas.zemleris@cern.ch')

        # statistics for Run/lumi

        # TODO: IT IS QUITE SAFE TO DISPLAY RESULTS OF A QUERY THAT JUST FINDS AN ENTITY
        # (especially if only one api param which is same as the result)

        # TODO: allow combining keyword query and structured query


        # TODO: we may wish to be able to interpret the semantics behind the dataset...
        """
        the only numbers in preconditions are lumi and run

        old the others are post-conditions...
        """

        print search(query='configuration of  Zmmg-13Jul2012-v1 location=T1_*')


if __name__ == '__main__':
    unittest.main()
