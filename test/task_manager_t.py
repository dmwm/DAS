#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import time
import unittest
from random import randrange
from DAS.utils.task_manager import TaskManager
from multiprocessing import Array

def worker(idx, arr):
    """Simple worker which fills given array with provided idx value"""
    arr[idx] = idx

class testUtils(unittest.TestCase):
    """
    A test class for the DAS task manager class
    """
    def setUp(self):
        """
        set up shared array of type int with given size
        """
        self.size = 10
        self.data = Array('i', range(self.size))

    def test_pool(self):
        """Test task manager"""
        expect = [idx for idx in range(self.size)]
        mypool = TaskManager()
        for idx in expect:
            mypool.spawn(worker, idx, self.data)
        mypool.joinall()
        result = [idx for idx in self.data]
        self.assertEqual(result, expect)

#
# main
#
if __name__ == '__main__':
    unittest.main()
