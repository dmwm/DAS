#!/usr/bin/env python
# -*- coding: utf-8 -*-
#pylint: disable-msg=C0111,C0301,C0103,E1101,R0904,W0201, E0611,F0401
# pylint disabled msgs: C0111 missing docstring, C0301 line-too-long,
#                       C0103(invalid-name),E1101(no-member),
#                       R0904(too-many-public-methods),
#                       W0201(attribute-defined-outside-init),
#                       E0611,F0401

"""
tests for keyword search
"""
from __future__ import print_function
# system modules

import unittest
from collections import defaultdict

# DAS modules
from DAS.keywordsearch.search import KeywordSearch
from DAS.utils.das_config import das_readconfig
from DAS.core.das_core import DASCore

#globals
from DAS.web.dbs_daemon import initialize_global_dbs_mngr
from DAS.web.dbs_daemon import get_global_dbs_inst


N_QUERIES = 0
N_PASSED_AT_1 = 0
N_PASSED_AT = defaultdict(int)
PASSED_AT = [[] for i in range(0, 50)]
NOT_PASSED_AT = [[] for i in range(0, 50)]
TIMES = []
STATUSES = {}

# default requirement for the result to appear before k-th item
REQUIRE_TO_PASS_BEFORE_DEFAULT = 4

# shall tests fail on non-implemented stuff? in integration surely not!
DO_NOT_FAIL_ON_NON_IMPLEMENTED = True

SYNONYMS_NOT_IMPLEMENTED = True
PRINT_QUERY_RES_DICT = False
PROFILING_ENABLED = False

from pprint import pformat

import time


class Timer(object):
    """
    helper to easily measure time
    """

    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start


class KwsTesterMetaClass(type):
    """
    a metaclass to avoid expensive setUp for each test.
    """

    def __init__(cls, name, bases, d):
        type.__init__(cls, name, bases, d)

        # set up only once
        if hasattr(cls, 'global_dbs_inst') and cls.global_dbs_inst:
            return

        print('setUp in metaclass: getting dbs manager ' \
              '(and fetching datasets if needed)')
        cls.global_dbs_mngr = initialize_global_dbs_mngr(update_required=False)
        cls.global_dbs_inst = get_global_dbs_inst()
        cls.kws = KeywordSearch(dascore=DASCore(multitask=False))
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

    @classmethod
    def is_correct(cls, result, expected):
        if isinstance(expected, str) or isinstance(expected, unicode):
            return result == expected
        return result in expected

    def print_details(self, expected, first_result, qstatus, query):
        if PRINT_QUERY_RES_DICT:
            print('Query: ', query)
            print('Result: ', self.is_correct(first_result, expected))
            print('Queries so far:', N_QUERIES, \
                'Passed at #1: ', N_PASSED_AT_1, \
                'Passed at i-th:', pformat(N_PASSED_AT))
            print('Running times:', TIMES)
            print('Query statuses:', qstatus)
            print('Queries passed up to i=4', pformat(PASSED_AT[:4]))
            print('Queries NOT passed up to i=4', \
                pformat(NOT_PASSED_AT[:4]))

    def assert_query_result(self, query, expected=None, bad=None,
                            not_implemented=False):
        """
        run a test query, and gather statistics
        """
        global N_QUERIES, N_PASSED_AT_1
        # global but mutable: N_PASSED_AT, TIMES, STATUSES, PASSED_AT, NOT_PASSED_AT
        expected = expected or []
        pass_at_k_th = REQUIRE_TO_PASS_BEFORE_DEFAULT
        N_QUERIES += 1

        with Timer() as timer:
            _, results = self.kws.search(query,
                                         dbs_inst=self.global_dbs_inst,
                                         timeout=self.timeout)
        TIMES.append((timer.interval, query))
        suggestions = [item['result'] for item in results]
        first_result = suggestions[0]

        STATUSES[query] = 0
        if self.is_correct(first_result, expected):
            N_PASSED_AT_1 += 1
            STATUSES[query] = 1

        # count queries that contain the expected answer
        # not lower than at i-th position
        test_passed = False
        passed_at_i = False
        for pos in xrange(0, 30):
            if pos < len(suggestions) and self.is_correct(suggestions[pos],
                                                          expected):
                passed_at_i = True
                if pos + 1 <= pass_at_k_th:
                    test_passed = True
            if passed_at_i:
                N_PASSED_AT[pos] += 1
                PASSED_AT[pos].append({'query': query, 'res': expected})
            else:
                NOT_PASSED_AT[pos].append({'query': query, 'res': expected})

        self.print_details(expected, first_result, STATUSES, query)

        msg = '''
        Query: %s
        Got First: %s
        Expected: %s
        ''' % (query, first_result, str(expected))

        # require pass up to K-th result
        if not test_passed and \
                not (DO_NOT_FAIL_ON_NON_IMPLEMENTED and not_implemented):
            if not expected:
                pass
            elif isinstance(expected, list):
                self.assertTrue(first_result in expected, msg=msg)
            else:
                self.assertEquals(first_result, expected, msg=msg)
        if bad:
            for bad_ in bad:
                self.assertTrue(bad_ not in suggestions)


class TestDASKeywordSearch(KeywordSearchAbstractTester):
    def _test_numeric_params_1(self):
        # TODO: lumis can not be queried by run anymore...
        # values closer to the field name shall be preferred
        self.assert_query_result('lumis in run 176304', 'lumi run=176304')

    def _test_numeric_params_2(self):
        # TODO: lumi is not valid input param anymore?
        self.assert_query_result(
            'files in /HT/Run2011B-v1/RAW where run is 176304 lumi is 80',
            'file dataset=/HT/Run2011B-v1/RAW run=176304 lumi=80')

    def _test_operators(self):
        # operators are not implemented yet

        if False:
            self.assert_query_result('total number of files in Zmm',
                                     'file dataset=/a/b/c | count(file.name)')
            self.assert_query_result(
                'count of files in /DoubleMuParked25ns/*/*',
                'file dataset=/DoubleMuParked25ns/*/* | count(file.name)')
            self.assert_query_result(
                'count of conflicting files in /DoubleMuParked25ns/*/*',
                'file dataset=/DoubleMuParked25ns/*/* | count(das.conflict)')

    def test_dasql(self):
        self.assert_query_result('files of dataset=DoubleMuParked25ns',
                                 'file dataset=*DoubleMuParked25ns*')

        #assert search('files in /DoubleMuParked25ns/*/* | count(das.conflict')[0] == 'file dataset=/DoubleMuParked25ns/*/* | count(das.conflict)'

    # this result is not supported
    #def test_das_key_synonyms(self):
    #    self.assertQueryResult('location of *Run2012*PromptReco*/AOD',
    #                           'site dataset=*Run2012*PromptReco*/AOD*',
    #                           query_type='nl',
    #                           non_implemented=SYNONYMS_NOT_IMPLEMENTED)

    def test_dataset_synonyms(self):
        self.assert_query_result(
            'where is /ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM',
            'site dataset=/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM')

    def test_value_based(self):
        self.assert_query_result('datasets at T1_CH_CERN',
                                 'dataset site=T1_CH_CERN')

    def test_value_based_1(self):
        self.assert_query_result('datasets at T1_CH_*', 'dataset site=T1_CH_*')

    def test_interesting_queries(self):
        self.assert_query_result('magnetic field of run=20853',
                                 'run run=20853 | grep run.bfield')

        self.assert_query_result('when  was run=20853 taken?',
                                 ['run=20853 | grep run.start_time',
                                  'run=20853 | grep run.creation_time'],
                                 not_implemented=True)

        self.assert_query_result('administrator email of all T1 sites',
                                 'site site=*T1* | grep site.admin.email, site.name')

    def test_prefer_filtering_input(self):
        # summary run=150619 | grep summary.dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO
        self.assert_query_result(
            'summary dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO  run 150619',
            'summary dataset=/HICorePhysics/HIRun2010-ZMM-v2/RAW-RECO run=150619')

    def test_inputs_vs_postfilters(self):
        # Result #5 currently
        self.assert_query_result(
            'files of /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO  located at site T1_*',
            'file dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO site=T1_*')

    def test_inputs_vs_postfilters_1(self):

        # currently #4
        self.assert_query_result(
            'files of /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO  at site T1_*',
            'file dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO site=T1_*')

    def _test_inputs_vs_postfilters_2(self):
        # TODO: lumis can not be queried by run!
        # the query is quite ambigous...
        self.assert_query_result('lumis in run 176304', 'lumi run=176304')

    def test_inputs_non_existing_dataset(self):
        self.assert_query_result(
            '/DoubleMu/Run2012A-Zmmg-13Jul2012-v1xx/RAW-RECO',
            'dataset dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1xx/RAW-RECO')

    def test_postfilters(self):
        # 1
        self.assert_query_result(
            '/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO magnetic field>3.5',
            'run dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO | grep run.bfield>3.5')

    def test_result_field_selections(self):
        self.assert_query_result(
            '/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO magnetic field',
            'run dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO | grep run.bfield')

    def test_result_field_selections_pk(self):
        if not DO_NOT_FAIL_ON_NON_IMPLEMENTED:
            self.assert_query_result(
                '/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO magnetic field and run number',
                'run dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO | grep run.bfield, run.run_number')

    def _test_result_field_selections_2(self):
        self.assert_query_result('Zmmg complete file replicas',
                                 'file dataset=*Zmmg* | grep file.replica.complete, file.name',
                                 not_implemented=True)

    def test_result_field_selections_3(self):
        self.assert_query_result('Zmmg complete block replicas',
                                 'block dataset=*Zmmg* | grep block.replica.complete, block.name',
                                 not_implemented=True)

    def test_result_field_selections_4(self):
        self.assert_query_result('number of lumis in run 176304',
#                                 'lumi run=176304 | grep lumi.number')
                                 'run run=176304 | grep run.nlumis')

#    def test_result_field_selections_stem(self):
#        self.assert_query_result('Zmmg event number', [
#            'file dataset=*Zmmg* | grep lumi.number, file.name',
#            'dataset dataset=*Zmmg* | grep dataset.nevents, dataset.name',
#            'block dataset=*Zmmg* | grep block.nevents, block.name'],
#                                 not_implemented=True)

    def test_result_field_selections_stem_phrase(self):
        self.assert_query_result('Zmmg "event number"', [
            'file dataset=*Zmmg* | grep lumi.number, file.name',
            'dataset dataset=*Zmmg* | grep dataset.nevents, dataset.name',
            'block dataset=*Zmmg* | grep block.nevents, block.name'])

    def test_result_field_selections_harder(self):
        #2nd
        self.assert_query_result('delivered lumimosity in run 176304',
                                 'run run=176304 | grep run.delivered_lumi')

    def test_imperative(self):
        # 5
        self.assert_query_result(
            'tell me where is dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO located',
            'site dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO')

    def test_wh_words(self):
        self.assert_query_result(
            'where is dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO located',
            'site dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO')

    def test_wh_words_2(self):
        self.assert_query_result(query='where is Zmm',
                                 bad=['site primary_dataset=ZMM', ])

    def test_basic_queries(self):
        self.assert_query_result(
            'configuration /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO',
            'config dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO',
            not_implemented=SYNONYMS_NOT_IMPLEMENTED)

    def test_basic_queries_3(self):
        self.assert_query_result('/*Zmm*/*/*', 'dataset dataset=/*Zmm*/*/*')

    def test_basic_queries_1(self):
        # it is actually fine, because name is very common term,
        # so we don't want (and we dont get a filter)
        self.assert_query_result('name of vidmasze@cern.ch',
                                 ['user user=vidmasze@cern.ch | grep user.name',
                                  'user user=vidmasze@cern.ch | grep user.surname'])

    def test_basic_queries_4(self):
        self.assert_query_result('last name of vidmasze@cern.ch',
                                 'user user=vidmasze@cern.ch | grep user.surname')

    def test_from_free_text_parser(self):
        pairs = [("Zee CMSSW_4_*", "dataset dataset=*Zee* release=CMSSW_4_*"),
                 ("Zee mc", "dataset dataset=*Zee* datatype=mc"),
                 (
                     "/store/mc/Summer11/ZMM/GEN-SIM/DESIGN42_V11_428_SLHC1-v1/0003/AAF5DDA5-0733-E111-A8D4-0002C90A3426.root",
                     "file file=/store/mc/Summer11/ZMM/GEN-SIM/DESIGN42_V11_428_SLHC1-v1/0003/AAF5DDA5-0733-E111-A8D4-0002C90A3426.root"),
                 ("4_1 Zee", "dataset dataset=*Zee* release=*4_1*"),
                 ("MC CMSSW_4_* /Zee",
                  "dataset dataset=/Zee* datatype=mc release=CMSSW_4_*"),
                 #("gen-sim-reco", "tier tier=*gen-sim-reco*"),
                 #("SIM-DIGI", "tier tier=*SIM-DIGI*")
        ]
        for kwq, result in pairs:
            self.assert_query_result(kwq, result)

    # TODO: this is not valid due to changes to service schema or constraints?
    def _test_complex_no_impossible_results(self):
        self.assert_query_result(
            'file block=/MinimumBias/Run2011A-ValSkim-08Nov2011-v1/RAW-RECO#f424e3d4-0f05-11e1-a8b1-00221959e72f run=175648  site=T1_US_FNAL_MSS &&',
            'file block=/MinimumBias/Run2011A-ValSkim-08Nov2011-v1/RAW-RECO#f424e3d4-0f05-11e1-a8b1-00221959e72f run=175648 | grep file.replica.site=T1_US_FNAL_MSS',
            bad=[
                # these are completetly non-valid
                'file run=175648 | grep file.replica.site=T1_US_FNAL_MSS, file.block_name=/MinimumBias/Run2011A-ValSkim-08Nov2011-v1/RAW-RECO#f424e3d4-0f05-11e1-a8b1-00221959e72f',
                'file run=175648'
            ])

    def test_schema_terms_vs_field_names_1(self):
        # fairly easy (token)
        self.assert_query_result(
            '"number of files" dataset=*DoubleMuParked25ns*', [
                'dataset dataset=*DoubleMuParked25ns* | grep dataset.nfiles, dataset.name',
                'block dataset=*DoubleMuParked25ns* | grep block.nfiles, block.name'])

    def test_schema_terms_vs_field_names_2(self):
        # more ambiguous
        self.assert_query_result('number of files dataset=*DoubleMuParked25ns*',
                                 [
                                     'dataset dataset=*DoubleMuParked25ns* | grep dataset.nfiles, dataset.name',
                                     'block dataset=*DoubleMuParked25ns* | grep block.nfiles, block.name'])

    def test_schema_terms_vs_field_names_3(self):
        # here entity shall be preferred
        self.assert_query_result('files in dataset=*DoubleMuParked25ns*',
                                 'file dataset=*DoubleMuParked25ns*')


if __name__ == '__main__':
    import cProfile

    if PROFILING_ENABLED:
        cProfile.run("unittest.main()", filename="das_kwdsearch_t.cprofile")
    else:
        unittest.main()
