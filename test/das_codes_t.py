#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS web codes
"""

import time
import unittest
import traceback
from   DAS.web.das_codes import web_code, decode_code, DAS_WEB_CODES

class testDASWEBCODES(unittest.TestCase):
    """
    A test class for the DAS web codes
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.debug = 0

    def test_web_code(self):
        """Test DAS web codes"""
        for code, msg in DAS_WEB_CODES:
            result = web_code(msg)
            self.assertEqual(code, result)
            result = decode_code(code)
            self.assertEqual(msg, result)

        result = web_code('lkjlkj')
        self.assertEqual(-1, result)

        result = decode_code(-1)
        self.assertEqual('N/A', result)

#
# main
#
if __name__ == '__main__':
    unittest.main()
