#!/usr/bn/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

# system modules
import re
import sys
import json
import time
import unittest
import tempfile
# python 3
if  sys.version.startswith('3.'):
    import urllib.parse as urllib
    import urllib.request as urllib2
else:
    import urllib
    import urllib2

# das modules
from DAS.utils.url_utils import url_args

class testUtils(unittest.TestCase):
    """
    A test class for the DAS utils module
    """
    def test_convert2ranges(self):
        "Test sort_rows function"
        url = 'http://a.b.com/api?arg1=1&arg2=foo'
        expect = {'arg1':'1', 'arg2': 'foo'}
        result = url_args(url)
        self.assertEqual(result, expect)
        expect = {'arg1':1, 'arg2': 'foo'}
        result = url_args(url, convert_types=True)
        self.assertEqual(result, expect)

#
# main
#
if __name__ == '__main__':
    unittest.main()
