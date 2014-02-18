#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=R0921,R0903
"""
A class that picks some random data and watches for changes,
the establish actual TTL instead of advertised.

The logic was intended to be:
    Pick one or more recent queries
    Store the result of the query
    Spawn a querymaintainer to keep the data in the cache for the next ~24h
    Compare the data regularly and see whether it has changed
    Store the lifetime as a function of the query structure
    (the rest is then done by LifetimeReport)
"""

class LifetimeMonitor(object):
    "Lifetime monitor skeleton"
    def __init__(self, **kwargs):
        raise NotImplementedError
