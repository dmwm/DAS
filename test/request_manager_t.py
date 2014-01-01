#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS db methods
"""

import time
import unittest
from   DAS.web.request_manager import RequestManager

class testDAS_RegMgr(unittest.TestCase):
    """
    A test class for the DAS RequestManager
    """
    def setUp(self):
        """
        set up stuff
        """
        self.reqmgr = RequestManager(lifetime=0)

    def test_reqmgr(self):
        """Test reqmgr methods"""
        pid  = 1
        kwds = {'uinput':'bla'}
        self.reqmgr.add(pid, kwds)
        result = self.reqmgr.get(pid)
        self.assertEqual(kwds, result)

        self.reqmgr.remove(pid)
        result = self.reqmgr.get(pid)
        self.assertEqual(None, result)

    def test_reqmgr_tstamp(self):
        """Test reqmgr methods"""
        pid  = 1
        kwds = {'input':'bla'}
        self.reqmgr.add(pid, kwds)
        result = self.reqmgr.get(pid)
        self.assertEqual(kwds, result)
        time.sleep(1)
        self.reqmgr.clean()
        result = [r for r in self.reqmgr.items()]
        self.assertEqual([], result)
#
# main
#
if __name__ == '__main__':
    unittest.main()
