#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS spawn manager
"""
from __future__ import print_function

import os
import time
import unittest
import multiprocessing

from DAS.utils.spawn_manager import Process, spawn, spawn_queue, SpawnManager
from DAS.utils.spawn_manager import spawn
from DAS.core.das_ql_parser import DASQueryParser

@Process
def test_int(val):
    return val

@Process
def test_list(val):
    return [val, val]

@Process
def test_dict(val):
    return {'val': val, 'array': [val, val]}

def test_wait(val):
    print("test_wait, time=%s, PID=%s, PPID=%s" \
            % (time.time(), os.getpid(), os.getppid()))
    time.sleep(val)
    return val

class MyTest(object):
    def __init__(self, val):
        self.val = val
    def method(self, val):
        return {'self.val': self.val, 'val': val}

class testUtils(unittest.TestCase):
    """
    A test class for the DAS task manager class
    """
    def setUp(self):
        """
        set up shared array of type int with given size
        """
        dassystems = ['dbs', 'sitedb', 'phedex']
        daskeys = ['dataset', 'file', 'block', 'run', 'site', 
                   'date', 'system']
        parserdir = '/tmp'

        self.dasqlparser = DASQueryParser(daskeys, dassystems)

    def test_spawn_functions(self):
        """Test spawn functions"""
        val = 1
        result = test_int(val)
        expect = val
        self.assertEqual(result, expect)

        result = test_list(val)
        expect = [val, val]
        self.assertEqual(result, expect)

        result = test_dict(val)
        expect = {'val': val, 'array': [val, val]}
        self.assertEqual(result, expect)

    def test_spawn_class(self):
        """Test spawn class methods"""
        val = 1
        obj = MyTest(val)
        result = spawn(obj.method, val+val)
        expect = {'self.val': val, 'val': val+val}
        self.assertEqual(result, expect)

    def test_spawn_queue(self):
        """Test spawn_queue function"""
        print("\n### test_spawn_queue")
        queue  = multiprocessing.Queue()
        sleep  = 2
        # spawn 3 processes which will sleep for given time
        time1  = time.time()
        spawn_queue(queue, test_wait, sleep)
        time1  = time.time()-time1
        time2  = time.time()
        spawn_queue(queue, test_wait, sleep)
        time2  = time.time()-time2
        time3  = time.time()
        spawn_queue(queue, test_wait, sleep)
        time3  = time.time()-time3
        # get results from result queue
        res1   = queue.get()
        res2   = queue.get()
        res3   = queue.get()
        # compare max diff time between spawned processes
#        print "times", time1, time2, time3
        max_t  = max(abs(time1-time2), abs(time1-time3), abs(time2-time3))
        result = True if max_t < sleep else False
        expect = True
        self.assertEqual(result, expect)
        # check that returned results are consistent
        self.assertEqual(res1, sleep)
        self.assertEqual(res2, sleep)
        self.assertEqual(res3, sleep)

    def test_spawn_manager(self):
        """Test spawn_queue function"""
        print("\n### test_spawn_manager")
        mgr = SpawnManager()
        sleep  = 2
        # spawn 3 processes which will sleep for given time
        time1  = time.time()
        mgr.spawn(test_wait, sleep)
        time1  = time.time()-time1
        time2  = time.time()
        mgr.spawn(test_wait, sleep)
        time2  = time.time()-time2
        time3  = time.time()
        mgr.spawn(test_wait, sleep)
        time3  = time.time()-time3
        # get results from result queue
        result = mgr.join()
        # check that returned results are consistent
        self.assertEqual(result, [sleep, sleep, sleep])
        # compare max diff time between spawned processes
        max_t  = max(abs(time1-time2), abs(time1-time3), abs(time2-time3))
        result = True if max_t < sleep else False
        expect = True
        self.assertEqual(result, expect)

    def test_spawn_manager(self):
        """Test spawn_queue function"""
        dasservices = ['dbs', 'dbs3']
        daskeys = ['dataset']
        parserdir = os.getcwd()
        query="dataset=/ZMM*/*/*"
        q1 = self.dasqlparser.parse(query)
        q2 = spawn(self.dasqlparser.parse, query)
        self.assertEqual(q1, q2)
#
# main
#
if __name__ == '__main__':
    unittest.main()
