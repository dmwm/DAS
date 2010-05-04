#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS aggregated functions
"""

__revision__ = "$Id: das_functions.py,v 1.1 2009/07/10 21:03:54 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

def das_sum(args, results):
    """
    Provide DAS sum aggregation function.
    """
    print "Call das_sum", args, results
    pass

def das_count(args, results):
    """
    Provide DAS count aggregation function.
    """
    print "Call das_count", args, results
    reslist = [i for i in results]
    yield {'das_count(%s)' % args: len(reslist)}

def das_min(args, results):
    """
    Provide DAS min aggregation function.
    """
    print "Call das_min", args, results
    pass

def das_max(args, results):
    """
    Provide DAS max aggregation function.
    """
    print "Call das_max", args, results
    pass

def das_avg(args, results):
    """
    Provide DAS avg aggregation function.
    """
    print "Call das_avg", args, results
    pass

def das_stddev(args, results):
    """
    Provide DAS stddev aggregation function.
    """
    print "Call das_stddev", args, results
    pass
