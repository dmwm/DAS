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
        self._id = None
        self.rec_count = 0
    def sum(self, obj):
        """Sum function for this object"""
        value, _ = obj
        if  self.result == 'Not available':
            self.result = 0
        self.result += value
        self.rec_count += 1
    def count(self, obj):
        """Count function for this object"""
        value, _ = obj
        if  self.result == 'Not available':
            self.result = 0
        self.result += 1
        self.rec_count += 1
    def max(self, obj):
        """Max function for this object"""
        value, _id = obj
        if  self.result == 'Not available':
            self.result = 0
        if  value > self.result:
            self.result = value
            if  _id:
                self._id = _id
        self.rec_count += 1
    def min(self, obj):
        """Min function for this object"""
        value, _id = obj
        if  self.result == 'Not available':
            self.result = 99999999999
        if  value < self.result:
            self.result = value
            if  _id:
                self._id = _id
        self.rec_count += 1
    def avg(self, obj):
        """
        Average function for this object, we store both sum and rec_count, such
        that avg = sum/rec_count
        """
        value, _ = obj
        if  self.result == 'Not available':
            self.result = 0
        self.result += float(value)
        self.rec_count += 1
    def median(self, obj):
        """
        Median function for this object, we store all values as array, such
        that median can be found as middle value of array.
        """
        value, _ = obj
        if  self.result == 'Not available':
            self.result = []
        self.result.append(value)
        self.rec_count += 1
    
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
    _id = idict.get('_id', None)
    try:
        target.send((idict, _id))
    except StopIteration:
        pass

@coroutine
def selector(key, target):
    """A coroutine that selector the recived dict for given key"""
    row, _id = (yield)
    if  dict_type(row):
        if  row.has_key(key):
            try:
                target.send((row[key], _id))
            except StopIteration:
                pass
    elif isinstance(row, list):
        for item in row:
            if  dict_type(item) and item.has_key(key):
                try:
                    target.send((item[key], _id))
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
        return robj
    return None

def count(key, rows):
    """DAS count aggregator function"""
    robj = das_func('count', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    return {'value': robj.result}

def das_avg(key, rows):
    """DAS avg aggregator function"""
    robj = das_func('avg', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    if  robj.rec_count:
        data = {'value': float(robj.result)/robj.rec_count}
    else:
        data = {'value': 'N/A'}
    return data

def das_median(key, rows):
    """DAS median aggregator function"""
    robj = das_func('median', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    if  len(robj.result) == 1:
        data = {'value': robj.result[0]}
    if  len(robj.result) % 2:
        data = {'value': robj.result[len(robj.result)/2]}
    else:
        val  = (robj.result[len(robj.result)/2-1] + \
                robj.result[len(robj.result)/2] )/2
        data = {'value': val}
    return data

def das_min(key, rows):
    """DAS min aggregator function"""
    robj = das_func('min', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    return {'value': robj.result, '_id': robj._id}

def das_max(key, rows):
    """DAS max aggregator function"""
    robj = das_func('max', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    return {'value': robj.result, '_id': robj._id}

def das_sum(key, rows):
    """DAS sum aggregator function"""
    robj = das_func('sum', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    return {'value': robj.result}

def das_count(key, rows):
    """DAS count aggregator function"""
    robj = das_func('count', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    return {'value': robj.result}

