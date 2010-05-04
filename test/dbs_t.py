#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS DBS module
"""

import unittest
from DAS.services.dbs.dbs_parser import parser
#from DAS.services.dbs.dbs_parser import parser, parser_old

class testDBS(unittest.TestCase):
    """
    A test class for the DAS DBS module
    """

#    def testParser_old(self): 
#        """test call routine"""
#        data = """<?xml version='1.0' standalone='yes'?>
#<result STORAGEELEMENT_SENAME='se' FILES_LOGICALFILENAME='test'/>
#"""
#        resultlist = parser_old(data)
#        expectlist = [{'site': 'se', 
#                       'file': 'test'}]
#        self.assertEqual(expectlist, resultlist)

    def testParser(self): 
        """test call routine"""
        data = """<?xml version='1.0' standalone='yes'?>
<!-- DBS Version 1 -->
<dbs>
<results>
  <row>
    <site>se</site>
    <file>test</file>
  </row>
</results>
<SUCCESS/>
</dbs>
"""
        resultlist = [i for i in parser(data)]
        expectlist = [{'site': 'se', 
                       'file': 'test'}]
        self.assertEqual(expectlist, resultlist)

#
# main
#
if __name__ == '__main__':
    unittest.main()


