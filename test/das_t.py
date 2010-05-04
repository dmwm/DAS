#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS core module
"""

import unittest
from DAS.core.das_core import DASCore

class testDAS(unittest.TestCase):
    """
    A test class for the DAS core module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.das = DASCore(debug=0)

    def test_call(self):                          
        """test call routine"""
        query = "find site where site = srm-cms.cern.ch"
        query = "find run.number where run.number=84193"
        result = self.das.call(query)
        resultlist = [res for res in result]
        expectlist = [{'run.number': '84193', 'system': 'dbs'}]
        self.assertEqual(expectlist, resultlist)

#
# main
#
if __name__ == '__main__':
    unittest.main()

