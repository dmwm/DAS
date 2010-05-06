#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS aggregators provides basic aggregation functions such as
sum, count, avg. We use coroutines to get all results into
das_func sink, which by itself hold ResultObject as a result
holder.
"""

__revision__ = "$Id: das_aggregators.py,v 1.3 2010/03/05 18:08:23 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

import types
from DAS.utils.utils import dict_type

class ResultObject(object):
    """
    ResultObject holds the result value. This value can
    be set, updated and retrieved from this class. We also
    provide basic aggregation functions such as sum, count, etc.
    This class is used by coroutine to keep the result visible
    outside of the coroutine sink.
    """
    def __init__(self):
        self.result = 'Not available'
    def sum(self, value):
        """Sum function for this object"""
        if  self.result == 'Not available':
            self.result = 0
        self.result += value
    def count(self, value):
        """Count function for this object"""
        if  self.result == 'Not available':
            self.result = 0
        self.result += 1
    def max(self, value):
        """Max function for this object"""
        if  self.result == 'Not available':
            self.result = 0
        if  value > self.result:
            self.result = value
    def min(self, value):
        """Min function for this object"""
        if  self.result == 'Not available':
            self.result = 99999999999
        if  value < self.result:
            self.result = value
    def avg(self, value):
        """Average function for this object"""
        pass
#    def median(self, value):
#        """Median function for this object"""
#        pass
    
def coroutine(func):
    """Coroutine helper, to be used as decorator"""
    def start(*args, **kwargs):
        """Function call inside of coroutine helper"""
        cor = func(*args, **kwargs)
        cor.next()
        return cor
    return start

def decomposer(idict, target):
    """
    Receive input dict and pass its key,value pairs further to the target.
    """
    try:
#        for key, val in idict.items():
#            target.send({key:val})
        target.send(idict)
    except StopIteration:
        pass

@coroutine
def selector(key, target):
    """A coroutine that selector the recived dict for given key"""
    row = (yield)
    if  dict_type(row):
        if  row.has_key(key):
            try:
                target.send(row[key])
            except StopIteration:
                pass
    elif type(row) is types.ListType:
        for item in row:
            if  dict_type(item) and item.has_key(key):
                try:
                    target.send(item[key])
                except:
                    pass

@coroutine
def printer():
    """A coroutine that recieves data, it is a sink"""
    while True:
        value = (yield)
        print value


@coroutine
def das_action(obj, action):
    """A coroutine that recieves and perform the given action over the value"""
    while True:
        value = (yield)
        getattr(obj, action)(value)

def cochain(ckey, data_name, sink_name):
    """
    Construct coroutine chain for given compound key, data object name
    and sink name. Here is an example of such chain:

    .. doctest:

        decomposer(data,
            filter('block', 
                filter('replica', 
                    filter('size', sink
                    )
                )
            )
        )

    """
    code  = "decomposer(%s," % data_name 
    count = 1 # open brackets already
    for key in ckey.split('.'):
        code  += "selector('%s'," % key
        count += 1
    code += sink_name
    for idx in range(0, count):
        code += ")"
    return code

def das_func(func, ckey, genrows):
    """DAS aggregator function"""
    robj = ResultObject()
    sink = das_action(robj, func)
    for row in genrows:
        code = cochain(ckey, 'row', 'sink')
        obj  = compile(code, '<string>', 'exec')
        eval(obj)
    if  robj.result != 'Not available':
        return robj.result
    return None

