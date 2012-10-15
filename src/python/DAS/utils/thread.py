#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable-msg=
"""
File       : thread.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: Wrapper around python threading/thread modules
"""

# system modules
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
