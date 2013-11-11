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

from DAS.core.das_query import DASQuery
import DAS.core.das_process_dataset_wildcards as dataset_wildcards
from DAS.web.dbs_daemon import initialize_global_dbs_mngr
from DAS.web.dbs_daemon import get_global_dbs_inst


class TestDASDatasetWildcards(unittest.TestCase):
    global_dbs_mngr = False

    def setUp(self):
        """
        sets up dbs manager instance
        """

        print '\nsetUp: getting dbs manager to access current datasets '\
              '(and fetching them if needed)'
        # set up only once
        if not self.global_dbs_mngr:
            self.global_dbs_mngr = initialize_global_dbs_mngr()
        self.dbs_inst = get_global_dbs_inst()

    def test_doctests(self):
        """
        runs doctests defined in DAS.core.das_process_dataset_wildcards
        """

        # run the tests
        (n_fails, n_tests) = doctest.testmod(verbose=True, m=dataset_wildcards)

        self.assertEquals(n_fails, 0)

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
            DASQuery('dataset dataset=*Zmm*', dbs_inst=self.dbs_inst)
        except Exception, exc:
            msg = str(exc)

        self.assertTrue('query is matching more than one' in msg)

        # none (no such dataset)
        msg = ''
        try:
            DASQuery('dataset dataset=*Zmmdsjfdsjguuds*',
                     dbs_inst=self.dbs_inst)
        except Exception, exc:
            msg = str(exc)

        self.assertTrue('pattern you specified did not match '
                        'any datasets in DAS cache' in msg)

        # one
        results = False
        try:
            results = DASQuery('dataset dataset=/*Zmm*/*/*',
                               dbs_inst=self.dbs_inst)
        except Exception as exc:
            results = False

        self.assertTrue([results])

    #def test_web(self):
    # TODO: write unit tests for web integration
    # no dataset matching
    # more than more interpretation available
    # only one interpretation (standard query execution)


if __name__ == '__main__':
    unittest.main()
