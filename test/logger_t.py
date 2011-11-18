#!/usr/bin/env python
#pylint: disable-msg=c0301,c0103

"""
unit test for logger module
"""

import os
import unittest
from DAS.utils.logger import PrintManager

class testDASLogger(unittest.TestCase):
    """
    A test class for the DAS logger module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.verbose = 0
        self.logger = PrintManager('TestDASLogger', verbose=self.verbose)

#
# main
#
if __name__ == '__main__':
    unittest.main()
