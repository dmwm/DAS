#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS core module
"""

import unittest
from DAS.utils.regex import ip_address_pattern, last_time_pattern, \
date_yyyymmdd_pattern, key_attrib_pattern, cms_tier_pattern, \
float_number_pattern, int_number_pattern, \
se_pattern, site_pattern, web_arg_pattern, number_pattern,\
last_key_pattern, unix_time_pattern

from DAS.utils.regex_daskeys import  phedex_tier_pattern

class testDASCore(unittest.TestCase):
    """
    A test class for regular expression patterns
    """
    def true(self, pat, param):
        """true result"""
        result = pat.match(param) and True
        self.assertEqual(True, result)

    def false(self, pat, param):
        """false result"""
        result = pat.match(param) and True
        self.assertEqual(None, result)

    def test_ip_address(self):
        """test ip_address pattern"""
        pat = ip_address_pattern
        arg = '123.123.123.123'
        self.true(pat, arg)
        arg = '123.123'
        self.false(pat, arg)
        arg = '123.123.123.123.123'
        self.false(pat, arg)

    def test_last_time(self):
        """test last_time pattern"""
        pat = last_time_pattern
        arg = '12h'
        self.true(pat, arg)
        arg = '24m'
        self.true(pat, arg)
        arg = '224m'
        self.false(pat, arg)

    def test_date_yyyymmdd(self):
        """test date_yyyymmdd pattern"""
        pat = date_yyyymmdd_pattern
        arg = '20100101'
        self.true(pat, arg)
        arg = '123'
        self.false(pat, arg)

    def test_key_attrib(self):
        """test key.attribute pattern"""
        pat = key_attrib_pattern
        arg = 'a.b.'
        self.true(pat, arg)
        arg = 'a.b.c.d'
        self.true(pat, arg)
        arg = 'sdfsdf'
        self.true(pat, arg)
        arg = '123'
        self.false(pat, arg)
        arg = 'abs1'
        self.false(pat, arg)

    def test_cms_tier(self):
        """test cms_tier pattern"""
        pat = cms_tier_pattern
        arg = 'T1_CH_CERN'
        self.true(pat, arg)
        arg = 'a.bv.c'
        self.false(pat, arg)

    def test_float_number(self):
        """test float_number pattern"""
        pat = float_number_pattern
        arg = '1.2'
        self.true(pat, arg)
        arg = '1'
        self.false(pat, arg)
        arg = '-1.1'
        self.true(pat, arg)

    def test_int_number(self):
        """test int_number pattern"""
        pat = int_number_pattern
        arg = '123'
        self.true(pat, arg)
        arg = 'sdflkh'
        self.false(pat, arg)

    def test_phedex_tier(self):
        """test phedex_tier pattern"""
        pat = phedex_tier_pattern
        arg = 'T1_CH_CERN'
        self.true(pat, arg)
        arg = 'T1'
        self.false(pat, arg)

    def test_se(self):
        """test se pattern"""
        pat = se_pattern
        arg = 'a.b.c'
        self.true(pat, arg)
        arg = 'a.b'
        self.false(pat, arg)

    def test_site(self):
        """test site pattern"""
        pat = site_pattern
        arg = 'CERN'
        self.true(pat, arg)
        arg = 'cern'
        self.false(pat, arg)

    def test_web_arg(self):
        """test web_arg pattern"""
        pat = web_arg_pattern
        arg = '0'
        self.true(pat, arg)
        arg = 'abc'
        self.false(pat, arg)

    def test_number(self):
        """test number pattern"""
        pat = number_pattern
        arg = '123'
        self.true(pat, arg)
        arg = '-123.123'
        self.true(pat, arg)
        arg = 'abc'
        self.false(pat, arg)

    def test_last_key(self):
        """test last key pattern"""
        pat = last_key_pattern
        arg = 'date last'
        self.true(pat, arg)
        arg = 'date jobsummary last'
        self.false(pat, arg)
        arg = 'date   last'
        self.true(pat, arg)

    def test_unix_time(self):
        """test unix time pattern"""
        pat = unix_time_pattern
        arg = '1234567890'
        self.true(pat, arg)
        arg = '0123456789'
        self.true(pat, arg)
        arg = '123456789'
        self.false(pat, arg)
#
# main
#
if __name__ == '__main__':
    unittest.main()


