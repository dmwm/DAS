#!/usr/bin/env python
#pylint: disable-msg=c0301,c0103

"""
unit test for base manager module
"""

import unittest
from DAS.utils.das_config import das_readconfig
from DAS.core.basemanager import BaseManager

class testBaseManager(unittest.TestCase):
    """
    a test class for the das base manager module
    """
    def setUp(self):
        """
        set up das core module
        """
        dasconfig = das_readconfig()
        self.mgr = BaseManager(dasconfig)

    def test_result(self):                          
        """test base manager result method"""
        query  = "find site where site=T2_UK"
        result = self.mgr.result(query)
        expect = None
        self.assertEqual(expect, result)
#
# main
#
if __name__ == '__main__':
    unittest.main()

