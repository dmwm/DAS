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
import DAS.web.cms_adjust_input


class TestCMSAdjustInput(unittest.TestCase):

    def test_doctests(self):
        """
        runs doctests defined in DAS.web.cms_adjust_input
        """
        (n_fails, n_tests) = doctest.testmod(verbose=True,
                                             m=DAS.web.cms_adjust_input)
        self.assertEquals(n_fails, 0)


if __name__ == '__main__':
    unittest.main()
