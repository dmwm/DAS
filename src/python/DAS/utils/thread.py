#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable-msg=
"""
File       : thread.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: Wrapper around python threading/thread modules
"""

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

def start_new_thread(name, func, args):
    "Wrapper wroung standard thread.strart_new_thread call"
    thr = threading.Thread(target=func, name=name, args=args)
    thr.daemon = True
    thr.start()
    return thr

def dumpstacks(signal, frame):
    """
    Dump context of all threads upon given signal
    http://stackoverflow.com/questions/132058/showing-the-stack-trace-from-a-running-python-application
    """
    id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
    code = []
    for threadId, stack in sys._current_frames().items():
        code.append("\n# Thread: %s(%d)" % (id2name.get(threadId,""), threadId))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
    print "\n".join(code)

signal.signal(signal.SIGQUIT, dumpstacks)
