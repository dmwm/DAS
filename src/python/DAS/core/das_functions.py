#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS aggregated functions. All functions should be represented in a form
of generators. See das_count for example.
"""

__revision__ = "$Id: das_functions.py,v 1.2 2009/07/12 19:16:20 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

def das_sum(ikey, results):
    """
    Provide DAS sum aggregation function.
    """
    results = [i for i in results]
    sum = 0
    for item in results:
        if  not item.has_key(ikey):
            raise Exception('Results does not contain key="%s"' % ikey)
        sum += float(item[ikey])
    yield {'das_sum(%s)' % ikey: sum}

def das_count(ikey, results):
    """
    Provide DAS count aggregation function.
    """
    results = [i for i in results]
    reslist = [item[ikey] for item in results]
    yield {'das_count(%s)' % ikey: len(reslist)}

def das_min(ikey, results):
    """
    Provide DAS min aggregation function.
    """
    results = [i for i in results]
    min = 0
    first = 0
    for item in results:
        if  not item.has_key(ikey):
            raise Exception('Results does not contain key="%s"' % ikey)
        if  not first:
            min = item[ikey]
            first = 1
        if  item[ikey] < min:
            min = item[ikey]
    yield {'das_min(%s)' % ikey: min}

def das_max(ikey, results):
    """
    Provide DAS max aggregation function.
    """
    results = [i for i in results]
    max = 0
    first = 0
    for item in results:
        if  not item.has_key(ikey):
            raise Exception('Results does not contain key="%s"' % ikey)
        if  not first:
            max = item[ikey]
            first = 1
        if  item[ikey] > max:
            max = item[ikey]
    yield {'das_max(%s)' % ikey: max}

def das_avg(ikey, results):
    """
    Provide DAS avg aggregation function.
    """
    results = [i for i in results]
    sum = 0
    for item in results:
        if  not item.has_key(ikey):
            raise Exception('Results does not contain key="%s"' % ikey)
        sum += float(item[ikey])
    yield {'das_avg(%s)' % ikey: sum/len(results)}

def das_median(ikey, results):
    """
    Provide DAS median aggregation function.
    """
    results = [i for i in results]
    reslist = [item[ikey] for item in results]
    reslist.sort()
    if  len(reslist) % 2: # odd number
        median = float(reslist[ (len(reslist)-1)/2 ])
    else:
        idx = len(reslist)/2
        median = (float(reslist[idx-1]) + float(reslist[idx]))/2
    yield {'das_median(%s)' % ikey: median}

def das_stddev(ikey, results):
    """
    Provide DAS stddev aggregation function.
    """
    print "Call das_stddev", ikey, results
    pass
