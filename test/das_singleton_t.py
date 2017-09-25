#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS singleton
"""

import time
import unittest
import traceback
from   DAS.utils.das_singleton import das_singleton

class testDAS_Singleton(unittest.TestCase):
    """
    A test class for the DAS db methods
    """

    def test_das_singleton(self):
        """Test DAS singleton"""
        mgr1 = das_singleton(multitask=None)
        mgr2 = das_singleton()
        mgr3 = das_singleton()
        self.assertNotEqual(mgr1, mgr2)
        self.assertEqual(mgr2, mgr3)
#
# main
#
if __name__ == '__main__':
    unittest.main()
