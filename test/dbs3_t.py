#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS DBS module
"""

import unittest
from DAS.services.dbs3.dbs3_service import runrange

class testDBS(unittest.TestCase):
    """
    A test class for the DAS DBS3 module
    """

    def test_runrange(self): 
        """test runrange function"""
        result = runrange(123456, 234567)
        expect = '[123456,234567]'
        self.assertEqual(expect, result)

        result = runrange(234567, 123456)
        expect = '[123456,234567]'
        self.assertEqual(expect, result)

        result = runrange(123456, 123456)
        expect = "123456"
        self.assertEqual(expect, result)

        result = runrange(123456, 234567, True)
        expect = "['123456-234567']"
        self.assertEqual(expect, result)

        result = runrange(123456, 123456, True)
        expect = "123456"
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()


