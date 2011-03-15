#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS QL parser
"""

import time
import unittest
from random import randrange
from cherrypy import engine, tree
from multiprocessing import Array

from DAS.utils.task_manager import TaskManager, PluginTaskManager
from DAS.web.das_test_datasvc import Root

def daemon():
    """Simple daemon which doing nothing"""
    while True:
        print "daemon at %s" % time.time()
        time.sleep(1)

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

        # start TestDataService
        self.server = Root()

    def test_task_manager(self):
        """Test task manager"""
        expect = [idx for idx in range(self.size)]
        mypool = TaskManager()
        tasks  = []
        for idx in expect:
            tasks.append(mypool.spawn(worker, idx, self.data))
        mypool.joinall(tasks)
        result = [idx for idx in self.data]
        self.assertEqual(result, expect)

    def test_plugin_task_manager(self):
        """Test plugin task manager"""
        mgr  = PluginTaskManager(bus=self.server.engine, debug=1)
        mgr.subscribe()
        self.server.start()
        jobs = []
        jobs.append(mgr.spawn(daemon))
        mgr.clear(jobs)
        print "\njoin the task at %s\n" % time.time()
        time.sleep(2)
        print "\nstop server at %s\n" % time.time()
        self.server.stop()
#
# main
#
if __name__ == '__main__':
    unittest.main()
