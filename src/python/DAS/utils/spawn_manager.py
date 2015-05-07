#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=R0903
"""
File       : spawn_manager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: implementation of spawn function via multiprocessing module
             it is based on Process/Queue pairing.
"""
from __future__ import print_function

# system modules
import os
import multiprocessing
try:
    import cPickle as pickle
except ImportError:
    import pickle

def proc_worker(output, func, *args, **kwargs):
    "Worker which runs given function and put results back to output queue"
    pickled = kwargs.get('pickled', False)
    if  'pickled' in kwargs:
        del kwargs['pickled']
    res = func(*args, **kwargs)
    if  pickled:
        output.put(pickle.dumps(res))
    else:
        output.put(res)

def spawn(func, *args, **kwargs):
    """
    Implementation of spawn function via multiprocessing.Process module.
    We initialize output queue, run given function via separate process,
    and collect results from our output queue. The results should be
    pickleable, e.g. simple types, lists, dicts.
    """
    kwargs['pickled'] = True
    queue = multiprocessing.Queue()
    name  = 'process_pid%s_ppid%s' % (os.getpid(), os.getppid())
    proc  = multiprocessing.Process(target=proc_worker, name=name,\
                    args=tuple([queue, func] + list(args)),\
                    kwargs=kwargs)
    proc.start()
    proc.join()
    return pickle.loads(queue.get())

def spawn_queue(queue, func, *args, **kwargs):
    """
    Implementation of spawn function via multiprocessing.Process module.
    We initialize output queue, run given function via separate process,
    and collect results from our output queue. The results should be
    pickleable, e.g. simple types, lists, dicts.
    """
    name  = 'process_pid%s_ppid%s' % (os.getpid(), os.getppid())
    proc  = multiprocessing.Process(target=proc_worker, name=name,\
                    args=tuple([queue, func] + list(args)),\
                    kwargs=kwargs)
    proc.start()
    return proc

class Process(object):
    """Decorator to run a function through multiprocessing module"""
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        """
        Run function through multiprocessing.Process, results are stored in
        internal queue
        """
        return spawn(self.func, *args, **kwargs)

class SpawnManager(object):
    """
    Class which utilizes spawn_queue function and provides API to run and
    manage multiple processes
    """
    def __init__(self):
        self.queue = multiprocessing.Queue()
        self.procs = []

    def spawn(self, func, *args, **kwargs):
        "Spawn given function"
        proc = spawn_queue(self.queue, func, *args, **kwargs)
        self.procs.append(proc)

    def join(self, debug=False):
        "Join all processes and collect the results"
        results = []
        for proc in self.procs:
            if  debug:
                print(proc.name)
            proc.join()
            results.append(self.queue.get())
        return results
