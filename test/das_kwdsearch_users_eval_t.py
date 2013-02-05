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




    def test_result_attributes(self):
        self.assertQueryResult('/SingleElectron/Run2011A-WElectron-PromptSkim-v6/RAW-RECO status',
            'dataset dataset=/SingleElectron/Run2011A-WElectron-PromptSkim-v6/RAW-RECO | grep dataset.status')


    def test_numeric_params(self):
        # values closer to the field name shall be preferred
        self.assertQueryResult(
            'files /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO   run 12345 lumi 666702',
            'file dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO run=12345')


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

            # TODO: requires ontology run reco_status=1
            # reconstructed runs (reco_status=1) ...  (1800 results only?)

            # run.start_time, run.end_time


            "administrator of T1_CH_CERN"


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
