#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS db methods
"""

import time
import unittest
import traceback
from   DAS.utils.das_db import db_connection
from   DAS.utils.das_config import das_readconfig
from   DAS.web.request_manager import RequestManager
from   DAS.utils.utils import deepcopy

class testDAS_RegMgr(unittest.TestCase):
    """
    A test class for the DAS RequestManager
    """
    def setUp(self):
        """
        set up stuff
        """
        self.debug  = 0
        dasconfig   = deepcopy(das_readconfig())
        self.dburi  = dasconfig['mongodb']['dburi']
        self.reqmgr = RequestManager(self.dburi)

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
#
# main
#
if __name__ == '__main__':
    unittest.main()
