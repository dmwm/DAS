#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=C0326

"""
DAS aggregators provides basic aggregation functions such as
sum, count, avg. We use coroutines to get all results into
das_func sink, which by itself hold ResultObject as a result
holder.
"""
from __future__ import print_function

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
        self.obj_id = None
        self.rec_count = 0
    def sum(self, obj):
        """Sum function for this object"""
        if  self.result == 'Not available':
            self.result = 0
        value, _ = obj
        if  value:
            self.result += value
        self.rec_count += 1
    def count(self, obj):
        """Count function for this object"""
        _, _ = obj
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
                self.obj_id = _id
        self.rec_count += 1
    def min(self, obj):
        """Min function for this object"""
        value, _id = obj
        if  self.result == 'Not available':
            self.result = 99999999999
        if  value < self.result:
            self.result = value
            if  _id:
                self.obj_id = _id
        self.rec_count += 1
    def avg(self, obj):
        """
        Average function for this object, we store both sum and rec_count, such
        that avg = sum/rec_count
        """
        value, _ = obj
        if  self.result == 'Not available':
            self.result = 0
        if  value != None:
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
        if  value != None:
            self.result.append(value)
            self.rec_count += 1

def coroutine(func):
    """Coroutine helper, to be used as decorator"""
    def start(*args, **kwargs):
        """Function call inside of coroutine helper"""
        cor = func(*args, **kwargs)
        next(cor)
        return cor
    return start

def decomposer(idict, target):
    """
    Receive input dict and pass its key,value pairs further to the target.
    """
    if  idict and isinstance(idict, dict):
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
        if  key in row:
            try:
                target.send((row[key], _id))
            except StopIteration:
                pass
    elif isinstance(row, list):
        for item in row:
            if  dict_type(item) and key in item:
                try:
                    target.send((item[key], _id))
                except:
                    pass

@coroutine
def printer():
    """A coroutine that recieves data, it is a sink"""
    while True:
        value = (yield)
        print(value)


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
    count_bracket = 1 # bracket is open
    for key in ckey.split('.'):
        code  += "selector('%s'," % key
        count_bracket += 1
    code += sink_name
    for _ in range(0, count_bracket):
        code += ")"
    return code

def das_func(func, ckey, genrows):
    """DAS aggregator function"""
    robj = ResultObject()
    # keep sink and row variables visible in namespace since cochain will
    # compile them into object code
    sink = das_action(robj, func) # coroutine sink
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
        data = {'value': robj.result[len(robj.result)//2]} # int division
    else:
        # int division for indexes
        val  = (robj.result[len(robj.result)//2-1] + \
                robj.result[len(robj.result)//2])/2
        data = {'value': val}
    return data

def das_min(key, rows):
    """DAS min aggregator function"""
    robj = das_func('min', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    return {'value': robj.result, '_id': robj.obj_id}

def das_max(key, rows):
    """DAS max aggregator function"""
    robj = das_func('max', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    return {'value': robj.result, '_id': robj.obj_id}

def das_sum(key, rows):
    """DAS sum aggregator function"""
    robj = das_func('sum', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    return {'value': robj.result}

def das_count(key, rows):
    """DAS count aggregator function"""
    if  key == 'lumi.number' or key == 'lumi':
        rows = expand_lumis(rows)
    robj = das_func('count', key, rows)
    if  not robj:
        return {'value': 'N/A'}
    return {'value': robj.result}

def expand_lumis(rows):
    "Expand lumis in given set of rows"
    for row in rows:
        lumi_val = row['lumi']
        if  isinstance(lumi_val, list):
            for item in lumi_val:
                lumi = item['number']
                if  isinstance(lumi, list):
                    for lumis in lumi:
                        lrange = [l for l in range(lumis[0], lumis[-1]+1)]
                        for slumi in lrange:
                            rec = dict(row)
                            rec['lumi'] = {'number': slumi}
                            yield rec
                else:
                    rec = dict(row)
                    rec['lumi'] = item
                    yield rec
        else:
            yield row
