#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: das_populator.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: DAS populator responsibles for cache population
with given pre-defined set of DAS queries.
"""

# system modules
import time

# DAS modules
from DAS.utils.utils import print_exc, expire_timestamp

def das_populator(dasmgr, query, expire, interval=None):
    """DAS populator daemon"""
    while True:
        das_populator_helper(dasmgr, query, expire)
        if  not interval:
            interval = expire/2.0
        time.sleep(interval)

def das_populator_helper(dasmgr, query, expire):
    """Process DAS query through DAS Core and sets new expire tstamp for it"""
    try:
        # To allow re-use of queries feeded by DAS populator
        # we need to ensure that instance is present in DAS query,
        # since web interface does it by default.
        dasquery = dasmgr.adjust_query(query)
        if  'instance' not in dasquery:
            raise Exception('Supplied query does not have DBS instance')
        newts = expire_timestamp(expire)
        # process DAS query
        dasmgr.call(dasquery)
        # update DAS expire timestamp
        dasmgr.rawcache.update_das_expire(dasquery, newts)
        print "\n### DAS populator", query, dasquery, expire, newts
    except Exception as exc:
        print_exc(exc)
