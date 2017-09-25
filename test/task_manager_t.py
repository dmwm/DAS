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

from DAS.utils.task_manager import TaskManager, PluginTaskManager, UidSet
from DAS.web.das_test_datasvc import Root

class TestQueue():
    "Test queue class which implements empty method. Ctor accepts queue status"
    def __init__(self, empty):
        self.status = empty
    def empty(self):
        "Empty method implementation"
        return self.status

def daemon():
    """Simple daemon which doing nothing"""
    while True:
        time.sleep(1)

def worker(idx, arr, **kwds):
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
        time.sleep(2) # let jobs finish
        self.server.stop()

    def test_uidset(self):
        """Test UidSet class"""
        tasks = UidSet()
        tasks.add(1)
        self.assertEqual(1 in tasks, True)
        self.assertEqual(2 in tasks, False)
        tasks.add(1)
        tasks.add(2)
        self.assertEqual(1 in tasks, True)
        self.assertEqual(2 in tasks, True)
        self.assertEqual(tasks.get(1), 2) # we should have 2 values of 1
        self.assertEqual(tasks.get(2), 1) # we should have 1 value of 2
        tasks.discard(2)
        self.assertEqual(2 in tasks, False)
        tasks.discard(1)
        self.assertEqual(tasks.get(1), 1) # now we should have 1 value of 1
        tasks.discard(1)
        self.assertEqual(1 in tasks, False)

    def test_assign_priority(self):
        """Test priority assignment"""
        tasks  = TaskManager(qtype='PriorityQueue', qfreq=10)
        uid1   = '1.1.1.1'
        tasks._uids.add(uid1)
        uid2   = '2.2.2.2'
        tasks._uids.add(uid1)
        result = tasks.assign_priority(uid1) # no tasks in a queue
        self.assertEqual(int(result), 0)
        tasks._tasks = TestQueue(empty=False)
        res1   = [tasks._uids.add(uid1) for r in range(20)]
        self.assertEqual(int(tasks.assign_priority(uid1)), 2)
        res2   = [tasks._uids.add(uid2) for r in range(50)]
        self.assertEqual(int(tasks.assign_priority(uid2)), 5)

    def test_priority_task_manager(self):
        """Test priority task manager"""
        data   = [idx for idx in range(0, 30)]
        shared_data = Array('i', len(data))
        mypool = TaskManager(qtype='PriorityQueue', qfreq=10)
        tasks  = []
        for idx in data:
            if  idx%2:
                tasks.append(mypool.spawn(worker, idx, shared_data, uid=1))
            else:
                tasks.append(mypool.spawn(worker, idx, shared_data, uid=2))
        mypool.joinall(tasks)
        result = [idx for idx in shared_data]
        self.assertEqual(result, data)

#
# main
#
if __name__ == '__main__':
    unittest.main()
