#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0613,W0622,W0702,R0903

"""
DAS timer
"""

__author__ = "Valentin Kuznetsov"

import time
from DAS.utils.utils import genkey

class _DASTimerSingleton(object):
    """
    DAS timer class keeps track of execution time.
    """
    def __init__(self):
        self.timer = {}
        self.counter = 0
    def record(self, tag):
        """Record time for given tag"""
        thash = genkey(tag)
        if  thash in self.timer:
            time0 = self.timer[thash]['time']
            self.timer[thash]['time'] = time.time() - time0
        else:
            self.timer[thash] = \
                {'tag': tag, 'time': time.time(), 'counter': self.counter}
            self.counter += 1

DAS_TIMER_SINGLETON = _DASTimerSingleton()

def das_timer(tag, record):
    """Record timestamp for given tag if record parameter is set"""
    if  record:
        DAS_TIMER_SINGLETON.record(tag)

def get_das_timer():
    """Return timer dict to the caller"""
    return DAS_TIMER_SINGLETON.timer
