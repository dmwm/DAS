#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das_dbs_update.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Script to update DBS datasets in DAS cache
"""

# system modules
import os
import sys
import argparse

# DAS modules
from DAS.web.dbs_daemon import DBSDaemon
from DAS.utils.das_config import das_readconfig

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--config", action="store",
            dest="config", default="", help="DAS configuration file")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose mode")

def dbs_update(dasconfig, verbose=False):
    """Start DBS daemon if it is requested via DAS configuration"""
    dburi = dasconfig['mongodb']['dburi']
    dbs_url = dasconfig['dbs']['url']
    dbs_global = dasconfig['dbs']['global']
    dbs_instances = dasconfig['dbs']['instances']
    config = dasconfig['web_server']
    interval  = config.get('dbs_daemon_interval', 3600)
    dbsexpire = config.get('dbs_daemon_expire', 3600)
    preserve_dbs_col = config.get('preserve_on_restart', False)

    dbs_urls = []
    for inst in dbs_instances:
        dbs_urls.append(\
                (dbs_url.replace(dbs_global, inst), inst))
    dbs_config  = {'expire': dbsexpire,
                   'preserve_on_restart': preserve_dbs_col}
    for dbs_url, inst in dbs_urls:
        dbsmgr = DBSDaemon(dbs_url, dburi, dbs_config)
        if  verbose:
            print("### update %s, %s" % (dbs_url, dbs_config))
        dbsmgr.update()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    dasconfig = das_readconfig(opts.config)
    dbs_update(dasconfig, opts.verbose)

if __name__ == '__main__':
    main()
