#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : pymongostat.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

from __future__ import print_function
__author__ = "Valentin Kuznetsov"

# system modules
import os
import sys
import pprint
from   optparse import OptionParser

# pymongo modules
from pymongo import MongoClient

class MyOptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = OptionParser()
        self.parser.add_option("--port", action="store",
           dest="port", default=8230, help="Input file")
        self.parser.add_option("--json", action="store_true",
           default=False, dest="json", help="Return json output")
    def get_opt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def main():
    "Main function"
    optmgr  = MyOptionParser()
    opts, _ = optmgr.get_opt()
    conn = MongoClient('mongodb://localhost:8230')
    mdb = conn['admin']
    data = mdb.command("serverStatus")
    if opts.json:
        print(pprint.pformat(data))
        return
    process = data['process']
    pid = data['pid']
    version = data['version']
    conns = data['connections']['available']
    host = data['host']
    status = 'not ready'
    if  pid > 0 and conns > 100:
        status = 'ok'
    print("Process:%s, PID:%s, Version:%s, Host:%s, Connections: %s, Status: %s" \
            % (process, pid, version, host, conns, status))

if __name__ == '__main__':
    main()
