#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das_cleanup.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Clean up DAS caches
"""

# system modules
import json
import time
import argparse

# DAS modules
from DAS.utils.das_db import db_connection
from DAS.utils.das_config import das_readconfig

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--config", action="store",
            dest="config", default="", help="DAS configuration file")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose mode")

def cleanup(dasconfig, verbose=False):
    """DAS cache cleanup worker"""
    config = dasconfig['mongodb']
    dburi = config['dburi']
    dbname = config['dbname']
    cols = config['collections']
    del_ttl = dasconfig['dasdb']['cleanup_delta_ttl']
    conn = db_connection(dburi)
    spec = {'das.expire': { '$lt':time.time()-del_ttl}}
    msgs = []
    for col in cols:
        if  verbose:
            ndocs = conn[dbname][col].find(spec).count()
            msgs.append('%s.%s %s docs' % (dbname, col, ndocs))
        conn[dbname][col].remove(spec)
    if  verbose:
        tstamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))
        print('%s %s %s' % (tstamp, json.dumps(spec), ' '.join(msgs)))

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    dasconfig = das_readconfig(opts.config)
    cleanup(dasconfig, opts.verbose)

if __name__ == '__main__':
    main()
