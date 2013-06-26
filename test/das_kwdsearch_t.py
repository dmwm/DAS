#!/usr/bin/env python
# -*- coding: utf-8 -*-
#pylint: disable-msg=C0103, W0703, C0111, W0511

"""
tests for keyword search
"""
# system modules

import unittest
import doctest

# DAS modules

from DAS.core.das_process_dataset_wildcards import get_global_dbs_mngr
from DAS.keywordsearch import search as kwdsearch_module
from DAS.keywordsearch.search import search, init as init_kws
#keyword_schema_weights, keyword_value_weights

from DAS.core.das_core import DASCore


#globals
n_queries = 0
n_queries_passed_at_1 = 0
n_queries_passed_at = {}
queries_passed_at = [[] for i in range(0, 50)]
queries_not_passed_at =[[] for i in range(0, 50)]
times = []
qstatus = {}

# default requirement for the result to appear before k-th item
REQUIRE_TO_PASS_BEFORE_DEFAULT = 4

# shall tests fail on non-implemented stuff? in integration surely not!
DO_NOT_FAIL_ON_NON_IMPLEMENTED = True

SYNONYMS_NOT_IMPLEMENTED = True

from pprint import pformat

import time

class Timer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

class TestDASKeywordSearch(unittest.TestCase):
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




    def result_is_correct(self, result, expected_results):
        if isinstance(expected_results, str) or isinstance(expected_results, unicode):
            return result == expected_results

        return result in expected_results


    def assertQueryResult(self, query, expected_results = None,
                          exclude_for_all_results=None,  query_type = 'general',
                          pass_at_k_th = REQUIRE_TO_PASS_BEFORE_DEFAULT,
                          non_implemented=False):
        '''
        run a test query, and gather statistics
        '''
        global n_queries, n_queries_passed_at_1,\
            n_queries_passed_at, times, qstatus, queries_passed_at, queries_not_passed_at
        n_queries += 1

        with Timer() as t:
            results = search(query, dbsmngr=self.global_dbs_inst)

        times.append((t.interval, query))

        first_result = results[0]['result']
        results_str_list = map(lambda item: item['result'], results)


        qstatus[query] = 0
        if self.result_is_correct(first_result, expected_results):
            n_queries_passed_at_1 += 1

            qstatus[query] = 1


        test_passed = False
        # count queries that contained expected answer not lower than at i-th position
        passed_at_i = False
        for i in xrange(0, 30):
            if i < len(results) and self.result_is_correct(results[i]['result'], expected_results):
                # TODO: recall (how many of the correct ones have been seen)
                passed_at_i = True

                if i+1 <= pass_at_k_th:
                    test_passed = True


            if passed_at_i:
                n_queries_passed_at[i] = n_queries_passed_at.get(i, 0) + 1
                queries_passed_at[i].append( {'query': query,  'res': expected_results})
            else:
                queries_not_passed_at[i].append( {'query': query,  'res': expected_results})


        # TODO: print distribution

        print 'Test: ', query, '. Result: ', self.result_is_correct(first_result, expected_results)
        print 'Queries so far:', n_queries, 'Passed at #1: ', n_queries_passed_at_1, 'Passed at i-th:', pformat(n_queries_passed_at)
        print 'Running times:', times
        print 'Query statuses:', qstatus
        
        print 'Queries passed up to i=4', pformat(queries_passed_at[:4])
        
        print 'Queries NOT passed up to i=4', pformat(queries_not_passed_at[:4])
        
        




        msg = '''
        Query: %s
        Got First: %s
        Expected: %s
        ''' % (query, first_result, str(expected_results))

        # require pass up to K-th result
        if not test_passed  and  \
                not (DO_NOT_FAIL_ON_NON_IMPLEMENTED and non_implemented):
            if isinstance(expected_results, list):
                self.assertIn(first_result, expected_results, msg=msg)
            else:
                self.assertEquals(first_result, expected_results,  msg=msg)


        # TODO: exclusion ( is that needed?)
        if exclude_for_all_results:
            for bad in exclude_for_all_results:
                self.assertNotIn(bad, results)


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
        """
        this was a manual test
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
        """
        pass





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


    def test_numeric_params_1(self):
        # values closer to the field name shall be preferred
        self.assertQueryResult('lumis in run 176304', 'lumi run=176304', query_type='numeric')

    def test_numeric_params_2(self):
        # TODO: field 'is' value --> a good pattern?
        self.assertQueryResult('files in /HT/Run2011B-v1/RAW where run is 176304 lumi is 80',
                'file dataset=/HT/Run2011B-v1/RAW run=176304 lumi=80', query_type='numeric+value')



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
        # TODO: can we process wildcards for the top results ?
        self.assertQueryResult('files of dataset=DoubleMuParked25ns',
            'file dataset=*DoubleMuParked25ns*')

        #assert search('files in /DoubleMuParked25ns/*/* | count(das.conflict')[0] == 'file dataset=/DoubleMuParked25ns/*/* | count(das.conflict)'

    def test_das_key_synonyms(self):
        self.assertQueryResult('location of *Run2012*PromptReco*/AOD',
                               'site dataset=*Run2012*PromptReco*/AOD',
                               query_type='nl',
                               non_implemented=SYNONYMS_NOT_IMPLEMENTED)


    def test_dataset_wildcards_1(self):
        # make sure 'dataset' is matched into entity but not its value (dataset=*dataset*)
        self.assertQueryResult('location of dataset *Run2012*PromptReco*/AOD',
            'site dataset=*Run2012*PromptReco*/AOD', query_type='nl_schema_term')

    def test_dataset_wildcards_2_synonyms(self):
        # automatically adding wildcards and synonyms
        self.assertQueryResult('location of Zmm',
            'site dataset=*Zmm*', query_type='wildcard')

    def test_value_based(self):
        self.assertQueryResult(u'datasets at T1_CH_CERN',
            'dataset site=T1_CH_CERN', query_type='nl')

    def test_value_based_1(self):
        self.assertQueryResult(u'datasets at T1_CH_*',
            'dataset site=T1_CH_*', query_type='nl_wildcard')

    def test_interesting_queries(self):
        self.assertQueryResult('magnetic field of run=20853',
                               'run run=20853 | grep run.bfield' )

        self.assertQueryResult('when  was run=20853 taken?',
                               ['run=20853 | grep run.start_time',
                                'run=20853 | grep run.creation_time'])

        self.assertQueryResult('administrator email of all T1 sites',
                               'site site=*T1* | grep site.admin.email, site.name',
                               query_type='nl_complex')



    def test_prefer_filtering_input(self):
        # TODO: currently we are overranking the result filters the top result is:
        # summary run=150619 | grep summary.dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO
        self.assertQueryResult('summary dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO  run 150619',
                               'summary dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO run=150619')



    def test_inputs_vs_postfilters(self):
        # Result #5 currently
        self.assertQueryResult('files of /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO  located at site T1_*',
                               'file dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO site=T1_*',
                               query_type='nl')

    def test_inputs_vs_postfilters_1(self):

        # currently #4
        self.assertQueryResult('files of /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO  at site T1_*',
                               'file dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO site=T1_*',
                               query_type='nl')

    def test_inputs_vs_postfilters_2(self):

        # the query is quite ambigous...
        self.assertQueryResult('lumis in run 176304',  'lumi run=176304',
                               query_type='ambigous')

    def test_inputs_non_existing_dataset(self):
        self.assertQueryResult('/DoubleMu/Run2012A-Zmmg-13Jul2012-v1xx/RAW-RECO',
                               'dataset dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1xx/RAW-RECO',
                               query_type='value')


    def test_postfilters(self):
        # 1
        self.assertQueryResult('Zmmg magnetic field>3.5',
                               'run dataset=*Zmmg* | grep run.run_number, run.bfield>3.5',
                               query_type='postfilter')

    def test_result_field_selections(self):

        self.assertQueryResult('Zmmg magnetic field',
                               'run dataset=*Zmmg* | grep run.bfield, run.run_number',
                               query_type='projection')
    def test_result_field_selections_2(self):

        self.assertQueryResult('Zmmg custodial file replicas',
                               'file dataset=*Zmmg* | grep file.replica.custodial, file.name',
                               query_type='projection')
    def test_result_field_selections_3(self):

        self.assertQueryResult('Zmmg custodial block replicas',
                               'block dataset=*Zmmg* | grep block.replica.custodial, block.name',
                               query_type='projection')

    def test_result_field_selections_4(self):
        # TODO: this is example of field that is named like an aggregator
        # number of smf... sum, count()...
        self.assertQueryResult('number of lumis in run 176304',
                               'summary run=176304 | grep summary.nlumis',
                               query_type='projection')

    def test_result_field_selections_stem(self):
        self.assertQueryResult('Zmmg event number',
                               ['file dataset=*Zmmg* | grep lumi.number, file.name',
                                'dataset dataset=*Zmmg* | grep dataset.nevents, dataset.name',
                                'block dataset=*Zmmg* | grep block.nevents, block.name'],
                               query_type='projection+ambigous+stem')

    def test_result_field_selections_stem_phrase(self):
        self.assertQueryResult('Zmmg "event number"',
                               ['file dataset=*Zmmg* | grep lumi.number, file.name',
                                'dataset dataset=*Zmmg* | grep dataset.nevents, dataset.name',
                                'block dataset=*Zmmg* | grep block.nevents, block.name'],
                               query_type='projection_ambigous_phrase')


    def test_result_field_selections_harder(self):
        #2nd
        self.assertQueryResult('delivered lumimosity in run 176304',
                               'run run=176304 | grep run.delivered_lumi',
                               query_type='projection')



    def test_imperative(self):
        # 5
        self.assertQueryResult('tell me where is dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO located',
                               'site dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO',
                               query_type='nl_imperative')


    def test_wh_words(self):
        self.assertQueryResult(
               'where is dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO located',
               'site dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO',
               query_type='nl_wh'
        )

    def test_wh_words_2(self):
        self.assertQueryResult(
            'where are Zmmg',
            'site dataset=*Zmmg*',
            query_type='nl_wh'
        )





    def test_basic_queries(self):
        self.assertQueryResult(
            'configuration /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO',
            'config dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO',
            query_type='basic',
            non_implemented=SYNONYMS_NOT_IMPLEMENTED)

    def test_basic_queries_2(self):
        self.assertQueryResult('configuration of /*Zmm*/*/*',
                     'config dataset=/*Zmm*/*/*',
                     query_type='basic')

    def test_basic_queries_3(self):


        self.assertQueryResult('/*Zmm*/*/*', 'dataset dataset=/*Zmm*/*/*',
                               query_type='basic')

    def test_basic_queries_1(self):
        # it is actually fine, because name is very common term, so we don't want (and we dont get a filter)
        self.assertQueryResult('name of vidmasze@cern.ch',
                            ['user user=vidmasze@cern.ch | grep user.name',
                             'user user=vidmasze@cern.ch | grep user.surname'],
                            query_type='projection_ambiguous')

    def test_basic_queries_2(self):
        self.assertQueryResult('last name of vidmasze@cern.ch', 'user user=vidmasze@cern.ch | grep user.surname',
                               query_type='projection')


    def test_complex_no_impossible_results(self):

        self.assertQueryResult(
            'file block=/MinimumBias/Run2011A-ValSkim-08Nov2011-v1/RAW-RECO#f424e3d4-0f05-11e1-a8b1-00221959e72f run=175648  site=T1_US_FNAL_MSS &&',
            'file block=/MinimumBias/Run2011A-ValSkim-08Nov2011-v1/RAW-RECO#f424e3d4-0f05-11e1-a8b1-00221959e72f run=175648 | grep file.replica.site=T1_US_FNAL_MSS',
            exclude_for_all_results = [
                # these are completetly non-valid
                'file run=175648 | grep file.replica.site=T1_US_FNAL_MSS, file.block_name=/MinimumBias/Run2011A-ValSkim-08Nov2011-v1/RAW-RECO#f424e3d4-0f05-11e1-a8b1-00221959e72f',
                'file run=175648'
            ]
        )


    def test_schema_terms_vs_field_names_1(self):
        # fairly easy (token)
        self.assertQueryResult(
            '"number of files" dataset=*DoubleMuParked25ns*',
            ['dataset dataset=*DoubleMuParked25ns* | grep dataset.nfiles, dataset.name',
             'block dataset=*DoubleMuParked25ns* | grep block.nfiles, block.name'],
            query_type='projection_phrase_ambiguos')
    def test_schema_terms_vs_field_names_2(self):
        # more ambiguous
        self.assertQueryResult(
            'number of files dataset=*DoubleMuParked25ns*',
            ['dataset dataset=*DoubleMuParked25ns* | grep dataset.nfiles, dataset.name',
             'block dataset=*DoubleMuParked25ns* | grep block.nfiles, block.name'],
            query_type='projection_phrase_ambiguos')

    def test_schema_terms_vs_field_names_3(self):
        # here entity shall be preferred
        self.assertQueryResult(
            'files in dataset=*DoubleMuParked25ns*',
            'file dataset=*DoubleMuParked25ns*',
            query_type='basic')



if __name__ == '__main__':
    unittest.main()
