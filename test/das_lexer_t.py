#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS config module
"""

import os
import types
import unittest
from DAS.core.das_lexer import DASLexer

class testDASConfig(unittest.TestCase):
    """
    A test class for the DAS config module
    """

    def setUp(self):
        """Initialization of unit test parameters"""
        daskeys   = ['file', 'site', 'lat', 'lon', 'date', 'ip']
        self.daslexer = DASLexer(daskeys)
        self.daslexer.build()

    def testDASLexer_positive(self):
        """test DAS lexer, positive test"""
        query  = "site=test"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

        query  = "file=/test* site=T1_CH_CERN"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

        query  = "ip=137.138.141.145"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

        query  = "lat=2 lon=2"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

        query  = "lat=-2.2 lon=2.1"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

        query  = "lat=2,lon=2"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

        query  = "lat"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

        query  = "lat = 2 lon = 2"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

        query  = "lat = [1,2]"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

        query  = "date=20100101"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

        query  = "file=/abc*"
        result = self.daslexer.test(query)
        expect = None
        self.assertEqual(result, expect)

    def testDASLexer_negative(self):
        """test DAS lexer, negative test"""
        query  = "bla"
        self.assertRaises(Exception, self.daslexer.test, (query,))

        query  = "LAT"
        self.assertRaises(Exception, self.daslexer.test, (query,))

        query  = "lat=2 lon>2"
        self.assertRaises(Exception, self.daslexer.test, (query,))

        query  = "file like *test*"
        self.assertRaises(Exception, self.daslexer.test, (query,))

        query  = "file = *test%"
        self.assertRaises(Exception, self.daslexer.test, (query,))
#
# main
#
if __name__ == '__main__':
    unittest.main()


