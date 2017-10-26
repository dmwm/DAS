#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : thread.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: Wrapper around python threading/thread modules
"""
from __future__ import print_function

# system modules
import sys
import signal
import traceback
import threading

def set_thread_name(ident, name):
    "Set thread name for given identified"
    for thr in threading.enumerate():
        if  thr.ident == ident:
            thr.name = name
            break

def start_new_thread(name, func, args, unique=False):
    "Wrapper wroung standard thread.strart_new_thread call"
    if  unique:
        threads = threading.enumerate()
        threads.sort()
        for thr in threads:
            if  name == thr.name:
                return thr
    thr = threading.Thread(target=func, name=name, args=args)
    thr.daemon = True
    thr.start()
    return thr

def dumpstacks(isignal, iframe):
    """
    Dump context of all threads upon given signal
    http://stackoverflow.com/questions/132058/showing-the-stack-trace-from-a-running-python-application
    """
    print("DAS stack, signal=%s, frame=%s" % (isignal, iframe))
    id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
    adict = dict([(th.ident, th.isAlive()) for th in threading.enumerate()])
    ddict = dict([(th.ident, th.isDaemon()) for th in threading.enumerate()])
    code = []
    alive = 0
    for tid, stack in sys._current_frames().items():
        thname = id2name.get(tid,"")
        if 'das' in thname.lower():
            code.append("# Thread: %s(%d) alive=%s daemon=%s" \
                    % (thname, tid, adict[tid], ddict[tid]))
            if adict[tid]:
                alive += 1
    print("\n".join(sorted(code)))
    print("Total number of alive threads: %s" % alive)

signal.signal(signal.SIGUSR1, dumpstacks)
