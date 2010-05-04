#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import time
import unittest
from random import randrange
from DAS.utils.threadpool import ThreadPool

GLOBAL_ARRAY = []

def worker(delay):
    """Simple worker"""
    GLOBAL_ARRAY.append(delay)

class testUtils(unittest.TestCase):
    """
    A test class for the DAS utils module
    """
    def test_pool(self):
        """Test thread pool"""
        nproc  = 3
        mypool = ThreadPool(nproc)
        delays = [randrange(0, 5) for idx in range(nproc)]
        expect = list(delays)
        expect.sort()
        rdict  = {}
        for delay in delays:
            mypool.add_task(worker, delay)
        time.sleep(1)
        result = list(GLOBAL_ARRAY)
        result.sort()
        self.assertEqual(result, expect)

#
# main
#
if __name__ == '__main__':
    unittest.main()
