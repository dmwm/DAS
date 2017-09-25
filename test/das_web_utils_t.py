#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS wew utils module
"""

import json
import unittest

from DAS.web.utils import wrap2dasjson, json2html, quote
from DAS.web.utils import free_text_parser, choose_select_key

class testDASWebUtils(unittest.TestCase):
    """
    A test class for the DAS web utils module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug = 0

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

    def test_free_text_parser(self):
        """test free_text_parser function"""
        pairs = [("Zee CMSSW_4_*", "dataset dataset=*Zee* release=CMSSW_4_*"),
                 ("Zee mc", "dataset dataset=*Zee* datatype=mc"),
                 ("160915 CMSSW_4_*", "run run=160915 release=CMSSW_4_*"),
                 ("/abc.root CMSSW_2_0*", "file file=/abc.root release=CMSSW_2_0*"),
                 ("4_1 Zee", "dataset release=CMSSW_4_1* dataset=*Zee*"),
                 ("Zee /a/b/c#123", "dataset dataset=*Zee* block=/a/b/c#123"),
                 ("MC CMSSW_4_* /Zee", "dataset datatype=MC release=CMSSW_4_* dataset=/Zee*"),
                 ("gen-sim-reco", "tier tier=*gen-sim-reco*"),
                 ("raw-digi", "tier tier=*raw-digi*")]
        daskeys=['dataset', 'datatype', 'run', 'release', 'block', 'file']
        for uinput, dasquery in pairs:
            result = free_text_parser(uinput, daskeys)
            self.assertEqual(dasquery, result)

    def test_choose_select_key(self):
        """test choose_select_key function"""
        keys   = ['dataset', 'release', 'file']
        query  = 'dataset=abc release=CMS'
        expect = 'dataset' 
        result = choose_select_key(query, keys)
        self.assertEqual(expect, result)

        query  = 'release=CMS dataset=abc'
        expect = 'dataset'
        result = choose_select_key(query, keys)
        self.assertEqual(expect, result)

        query  = 'release release=CMS dataset=abc'
        expect = 'release'
        result = choose_select_key(query, keys)
        self.assertEqual(expect, result)

        query  = 'file=/a.root dataset=abc'
        expect = 'dataset'
        result = choose_select_key(query, keys)
        self.assertEqual(expect, result)

        query  = 'file=/a.root release=CMS'
        expect = 'file'
        result = choose_select_key(query, keys)
        self.assertEqual(expect, result)

        query  = 'release=CMS file=/a.root'
        expect = 'release'
        result = choose_select_key(query, keys)
        self.assertEqual(expect, result)

        query  = 'file dataset=abc'
        expect = 'file'
        result = choose_select_key(query, keys)
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()


