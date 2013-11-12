#!/usr/bin/env python
# -*- coding: utf-8 -*-
#pylint: disable-msg=C0103, W0703, C0111, W0511

"""
tests for keyword search
"""
# system modules

import unittest

# DAS modules

from DAS.keywordsearch.search import KeywordSearch
from DAS.utils.das_config import das_readconfig

#globals
from DAS.web.dbs_daemon import initialize_global_dbs_mngr
from DAS.web.dbs_daemon import get_global_dbs_inst


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
PRINT_QUERY_RES_DICT = False
PROFILING_ENABLED = False

from pprint import pformat

import time

class Timer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start


class KwsTesterMetaClass(type):
    """
    a metaclass to avoid expensive setUp for each test.

    TODO: in py2.7 however it can be more clean:
    http://stackoverflow.com/questions/423483/python-unittest-with-expensive-setup
    """
    def __init__(cls, name, bases, d):
        type.__init__(cls, name, bases, d)

        # set up only once
        if hasattr(cls, 'global_dbs_inst') and cls.global_dbs_inst:
            return

        print 'setUp in metaclass: getting dbs manager '\
              '(and fetching datasets if needed)'
        cls.global_dbs_mngr = initialize_global_dbs_mngr(update_required=False)
        cls.global_dbs_inst = get_global_dbs_inst()
        cls.kws = KeywordSearch(dascore=None)
        dasconfig = das_readconfig()
        cls.timeout = dasconfig['keyword_search']['timeout']



class KeywordSearchAbstractTester(unittest.TestCase):
    global_dbs_inst = False

    __metaclass__ = KwsTesterMetaClass

    def setUp(self):
        """
        sets up dbs manager instance reusing it from metaclass
        """
        #print "setUp: quick, reusing from metaclass"
        cls = KeywordSearchAbstractTester
        self.kws = cls.kws
        self.global_dbs_inst = cls.global_dbs_inst
        self.timeout = cls.timeout

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
        global n_queries, n_queries_passed_at_1, n_queries_passed_at, times,\
            qstatus, queries_passed_at, queries_not_passed_at

        n_queries += 1

        with Timer() as t:
            err, results = self.kws.search(query, dbs_inst=self.global_dbs_inst,
                                           timeout=self.timeout)

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


        if PRINT_QUERY_RES_DICT:
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
            if not expected_results:
                pass
            elif isinstance(expected_results, list):
                # TODO: in py2.7 will be able to use assertIn
                self.assertTrue(first_result in expected_results, msg=msg)
            else:
                self.assertEquals(first_result, expected_results,  msg=msg)

        # TODO: exclusion ( is that needed?)
        if exclude_for_all_results:
            for bad in exclude_for_all_results:
                self.assertTrue(bad not in results)



class TestDASKeywordSearch(KeywordSearchAbstractTester):
    def _test_numeric_params_1(self):
        # TODO: lumis can not be quiries by run anymore...
        # values closer to the field name shall be preferred
        self.assertQueryResult('lumis in run 176304', 'lumi run=176304', query_type='numeric')

    def _test_numeric_params_2(self):
        # TODO: lumi is not valid input param anymore?
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

    # this result is not supported
    #def test_das_key_synonyms(self):
    #    self.assertQueryResult('location of *Run2012*PromptReco*/AOD',
    #                           'site dataset=*Run2012*PromptReco*/AOD*',
    #                           query_type='nl',
    #                           non_implemented=SYNONYMS_NOT_IMPLEMENTED)



    def test_dataset_synonyms(self):
        self.assertQueryResult('where is /ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM',
            'site dataset=/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM', query_type='synonym')

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
                                'run=20853 | grep run.creation_time'],
                               non_implemented=True)

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

    def _test_inputs_vs_postfilters_2(self):
        # TODO: lumis can not be queried by run!
        # the query is quite ambigous...
        self.assertQueryResult('lumis in run 176304',  'lumi run=176304',
                               query_type='ambigous')

    def test_inputs_non_existing_dataset(self):
        self.assertQueryResult('/DoubleMu/Run2012A-Zmmg-13Jul2012-v1xx/RAW-RECO',
                               'dataset dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1xx/RAW-RECO',
                               query_type='value')


    def test_postfilters(self):
        # 1
        self.assertQueryResult('/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO magnetic field>3.5',
                               'run dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO | grep run.bfield>3.5',
                               query_type='postfilter')

    def test_result_field_selections(self):
        self.assertQueryResult('/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO magnetic field',
                               'run dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO | grep run.bfield',
                               query_type='projection')

    def test_result_field_selections_pk(self):
        if not DO_NOT_FAIL_ON_NON_IMPLEMENTED:
            # TODO: shall PK grep filters be allowed
            self.assertQueryResult('/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO magnetic field and run number',
                               'run dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO | grep run.bfield, run.run_number',
                               query_type='projection')

    def _test_result_field_selections_2(self):
        self.assertQueryResult('Zmmg complete file replicas',
                               'file dataset=*Zmmg* | grep file.replica.complete, file.name',
                               query_type='projection',
                               non_implemented=True)

    def test_result_field_selections_3(self):

        self.assertQueryResult('Zmmg complete block replicas',
                               'block dataset=*Zmmg* | grep block.replica.complete, block.name',
                               query_type='projection',
                               non_implemented=True)

    def test_result_field_selections_4(self):
        # TODO: this is example of field that is named like an aggregator
        # number of smf... sum, count()...
        self.assertQueryResult('number of lumis in run 176304',
                               'run run=176304 | grep run.nlumis',
                               query_type='projection')

    def test_result_field_selections_stem(self):
        self.assertQueryResult('Zmmg event number',
                               ['file dataset=*Zmmg* | grep lumi.number, file.name',
                                'dataset dataset=*Zmmg* | grep dataset.nevents, dataset.name',
                                'block dataset=*Zmmg* | grep block.nevents, block.name'],
                               query_type='projection+ambigous+stem',
                               non_implemented=True)

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
            'where is Zmm',
            [ ],
            exclude_for_all_results=[
                 'site primary_dataset=ZMM',
            ],
            query_type='non-supported-by-services'
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


    def test_from_free_text_parser(self):
        pairs = [("Zee CMSSW_4_*", "dataset dataset=*Zee* release=CMSSW_4_*"),
                 ("Zee mc", "dataset dataset=*Zee* datatype=mc"),
                 # TODO: ("160915 CMSSW_4_*", "run run=160915 release=CMSSW_4_*"),
                 ("/store/mc/Summer11/ZMM/GEN-SIM/DESIGN42_V11_428_SLHC1-v1/0003/AAF5DDA5-0733-E111-A8D4-0002C90A3426.root",
                    "file file=/store/mc/Summer11/ZMM/GEN-SIM/DESIGN42_V11_428_SLHC1-v1/0003/AAF5DDA5-0733-E111-A8D4-0002C90A3426.root"),
                 # TODO: could add unique prefix CMSSW_4_1* automatically...
                 ("4_1 Zee", "dataset dataset=*Zee* release=*4_1*"),
                 ("MC CMSSW_4_* /Zee",
                    "dataset dataset=/Zee* datatype=mc release=CMSSW_4_*"),
                 # TODO: no wildcards allowed in tier so far.. is that needed?
                 #("gen-sim-reco", "tier tier=*gen-sim-reco*"),
                 #("SIM-DIGI", "tier tier=*SIM-DIGI*")
                 ]

        for kwq, result in pairs:
            self.assertQueryResult(kwq, result)

    # TODO: this is not valid anymore due to changes to service schema or constraints?
    def _test_complex_no_impossible_results(self):

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
    import cProfile
    if PROFILING_ENABLED:
        cProfile.run("unittest.main()", filename="das_kwdsearch_t.cprofile")
    else:
        unittest.main()
