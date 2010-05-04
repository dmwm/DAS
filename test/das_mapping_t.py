#!/usr/bin/env python
#pylint: disable-msg=c0301,c0103

"""
unit test for DAS mapping set of tools
"""

import unittest
from DAS.core.das_mapping import translate

class testDASMapping(unittest.TestCase):
    """
    A test class for the DAS mapping
    """
    def test_mapconfig(self):                          
        """test DAS translate routine"""
        result = translate('phedex', 'site')
        expect = ['se']
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()
