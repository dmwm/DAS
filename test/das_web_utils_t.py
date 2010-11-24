#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS wew utils module
"""

import json
import plistlib
import unittest

from pymongo.connection import Connection

from DAS.web.utils import wrap2dasxml, wrap2dasjson, json2html

class testDASWebUtils(unittest.TestCase):
    """
    A test class for the DAS web utils module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug = 0

    def testDASXML(self):
        """test wrap2dasxml function"""
        data = {'test': {'foo':1}}
        expect = wrap2dasxml(data)
        result = plistlib.writePlistToString(data)
        self.assertEqual(expect, result)

    def testDASJSON(self):
        """test wrap2dasjson function"""
        data = {'test': {'foo': 1}}
        expect = wrap2dasjson(data)
        result = json.dumps(data)
        self.assertEqual(expect, result)

    def testJSON2HTML(self):
        """test json2html function"""
        data = {'test': {'foo': 1}}
        expect = json2html(data)
        result = '{\n <code class="key">"test"</code>: {\n    <code class="key">"foo"</code>: <code class="number">1</code>\n   }\n}'
        self.assertEqual(expect, result)
#
# main
#
if __name__ == '__main__':
    unittest.main()


