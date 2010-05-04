#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS DBS module
"""

import unittest
from services.dbs.dbs_parser import parser

class testDBS(unittest.TestCase):
    """
    A test class for the DAS DBS module
    """

    def testParser(self):                          
        """test call routine"""
        data = """<?xml version='1.0' standalone='yes'?>
<result STORAGEELEMENT_SENAME='se' FILES_LOGICALFILENAME='test'/>
"""
        resultlist = parser(data)
        expectlist = [{'site': 'se', 
                       'file': 'test'}]
        self.assertEqual(expectlist, resultlist)

#
# main
#
if __name__ == '__main__':
    unittest.main()


