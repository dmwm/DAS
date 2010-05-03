#!/usr/bin/env python
#pylint: disable-msg=c0301,c0103

"""
unit test for DAS mapping set of tools
"""

import unittest
from DAS.core.das_mapping import translate, jsonparser

class testDASMapping(unittest.TestCase):
    """
    A test class for the DAS mapping
    """
    def test_mapconfig(self): 
        """test DAS translate routine"""
        result = translate('phedex', 'site')
        expect = ['se']
        self.assertEqual(expect, result)

    def test_jsonparser(self): 
        """test DAS jsonparser routine"""
        rep1 = {'bytes':1, 'files':1, 'se':'T2'}
        rep2 = {'bytes':1, 'files':1, 'se':'T3'}
        rec1 = {'bytes':1, 'files':1, 'name':'ABC', 'replica':rep1}
        rec2 = {'bytes':2, 'files':2, 'name':'CDE', 'replica':[rep1, rep2]}
        jsondict = {'service': {'block': [rec1, rec2]},'timestamp':123}
        result = jsonparser(jsondict, 'block')
        expect = [rec1, rec2]
        self.assertEqual(expect, result)

        result = jsonparser(jsondict, 'block.bytes')
        expect = [1, 2]
        self.assertEqual(expect, result)

#        result = jsonparser(jsondict, 'block.replica')
#        expect = [1, 2]
#        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()
