#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS QL module include definition of DAS operators,
filters, aggregators, etc.
"""

__revision__ = "$Id: das_ql.py,v 1.4 2010/04/30 16:35:03 valya Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "Valentin Kuznetsov"

import inspect
from DAS.core.das_aggregators import ResultObject

DAS_FILTERS   = ['grep', 'unique']
DAS_OPERATORS = ['=', 'between', 'in', 'last']
#DAS_OPERATORS = ['!=', '<=', '<', '>=', '>', '=', 
#                 'between', 'nin', 'in', 'last']
DAS_SPECIALS  = ['date', 'system']
DAS_RESERVED  = DAS_FILTERS + DAS_OPERATORS + DAS_SPECIALS
URL_MAP       = {
    '!=' : '%21%3D',
    '<=' : '%3C%3D',
    '>=' : '%3E%3D',
    '<'  : '%3C',
    '>'  : '%3E',
    '='  : '%3D',
    ' '  : '%20'
}
MONGO_MAP     = {
    '>'  : '$gt',
    '<'  : '$lt',
    '>=' : '$gte',
    '<=' : '$lte',
    'in' : '$in',
    '!=' : '$ne',
    'nin': '$nin',
#    'between':'$in',
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

def das_special_keys():
    """
    Return list of DAS special keywords
    """
    return DAS_SPECIALS

def das_reserved():
    """
    Return list of DAS reserved keywords
    """
    return DAS_RESERVED

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

