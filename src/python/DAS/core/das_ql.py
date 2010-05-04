#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS QL module include definition of DAS operators,
filters, aggregators, etc.
"""

__revision__ = "$Id: das_ql.py,v 1.2 2010/03/05 18:10:52 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

import inspect
from DAS.core.das_aggregators import ResultObject

DAS_FILTERS = ['grep']
DAS_OPERATORS = ['!=', '<=', '<', '>=', '>', '=', 
                 'between', 'nin', 'in', 'last']
MONGO_MAP = {
    '>':'$gt',
    '<':'$lt',
    '>=':'$gte',
    '<=':'$lte',
    'in':'$in',
    '!=':'$ne',
    'nin':'$nin',
}
def mongo_operator(das_operator):
    """
    Convert DAS operator into MongoDB equivalent
    """
    if  MONGO_MAP.has_key(das_operator):
        return MONGO_MAP[das_operator]
    return None

def das_filters():
    """
    Return list of DAS filters
    """
    return DAS_FILTERS

def das_operators():
    """
    Return list of DAS operators
    """
    return DAS_OPERATORS

def das_aggregators():
    """
    Inspect ResultObject class and return its member function,
    which represents DAS aggregator functions
    """
    alist = []
    for name, ftype in inspect.getmembers(ResultObject):
        if  name.find("__") != -1:
            continue
        alist.append(name)
    return alist
