#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0613,W0622,W0702

"""
DAS timer
"""

__revision__ = "$Id: das_db.py,v 1.9 2010/05/04 21:12:19 valya Exp $"
__version__ = "$Revision: 1.9 $"
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
        hash = genkey(tag)
        if  self.timer.has_key(hash):
            time0 = self.timer[hash]['time']
            self.timer[hash]['time'] = time.time() - time0
        else:
            self.timer[hash] = {'tag': tag, 'time': time.time(), 'counter': self.counter}
            self.counter += 1

DAS_TIMER_SINGLETON = _DASTimerSingleton()

def das_timer(tag, record):
    """Record timestamp for given tag if record parameter is set"""
    if  record:
        DAS_TIMER_SINGLETON.record(tag)

def get_das_timer():
    """Return timer dict to the caller"""
    return DAS_TIMER_SINGLETON.timer
