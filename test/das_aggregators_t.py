#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS aggregators
"""

import unittest
from DAS.core.das_aggregators import das_func, das_sum, das_avg, das_min, das_max
from DAS.core.das_aggregators import das_count, das_median, expand_lumis
from DAS.utils.ddict import DotDict

class testDASAggregators(unittest.TestCase):
    """
    A test class for the DAS aggregators
    """
    def test_expand_lumis(self):
        "Test expand_lumis function"
        rows   = [{'lumi':[{'number': [[1,2], [5,7]]}]}]
        expect = [{'lumi':{'number': 1}},
                  {'lumi':{'number': 2}},
                  {'lumi':{'number': 5}},
                  {'lumi':{'number': 6}},
                  {'lumi':{'number': 7}}]
        result = [r for r in expand_lumis(rows)]
        self.assertEqual(expect, result)

        rows   = [{'lumi':[{'number': [[1,2]]}]}]
        expect = [{'lumi':{'number': 1}},
                  {'lumi':{'number': 2}}]
        result = [r for r in expand_lumis(rows)]
        self.assertEqual(expect, result)

        rows   = [{'lumi':[{'number': [[1,2]]}], 'run':{'run_number':1}},
                  {'lumi':[{'number': [[5,7]]}], 'run':{'run_number':2}}]
        expect = [{'lumi':{'number': 1}, 'run':{'run_number':1}},
                  {'lumi':{'number': 2}, 'run':{'run_number':1}},
                  {'lumi':{'number': 5}, 'run':{'run_number':2}},
                  {'lumi':{'number': 6}, 'run':{'run_number':2}},
                  {'lumi':{'number': 7}, 'run':{'run_number':2}}]
        result = [r for r in expand_lumis(rows)]
        self.assertEqual(expect, result)

        expect = {'value': 5}
        result = das_count('lumi.number', rows)
        self.assertEqual(expect, result)

    def test_aggregators(self):
        """test aggregators dict records"""
        rows = []
        data = {'block': {'name':'AAA', 'replica': [{'name':'a', 'size':1}, {'name':'b', 'size':10}] }}
        rows.append(data)
        data = {'block': {'name':'AAA', 'replica': [{'name':'a', 'size':2}, {'name':'b', 'size':20}] }}
        rows.append(data)

        expect = 33
        robj = das_func('sum', 'block.replica.size', rows)
        self.assertEqual(expect, robj.result)

        expect = 4
        robj = das_func('count', 'block.replica.size', rows)
        self.assertEqual(expect, robj.result)

        expect = 1
        robj = das_func('min', 'block.replica.size', rows)
        self.assertEqual(expect, robj.result)

        expect = 20
        robj = das_func('max', 'block.replica.size', rows)
        self.assertEqual(expect, robj.result)

        expect = (1+10+2+20)/4.
        robj = das_func('avg', 'block.replica.size', rows)
        self.assertEqual(expect, float(robj.result)/robj.rec_count)

        expect = (10+2)//2
        robj = das_func('median', 'block.replica.size', rows)
        val = (robj.result[len(robj.result)//2-1] + \
                                robj.result[len(robj.result)//2] )//2
        self.assertEqual(expect, val)

        expect = 20
        drows  = [DotDict(row) for row in rows]
        robj = das_func('max', 'block.replica.size', drows)
        self.assertEqual(expect, robj.result)

    def test_aggregators_with_das(self):
        """test aggregators with das record"""
        rows = []
        data = {'block': {'name':'AAA', 'replica': [{'name':'a', 'size':1}, {'name':'b', 'size':10}] }}
        data.update({'_id' : 1})
        rows.append(data)
        data = {'block': {'name':'AAA', 'replica': [{'name':'a', 'size':2}, {'name':'b', 'size':20}] }}
        data.update({'_id' : 2})
        rows.append(data)

        expect = {'result': 20, '_id': 2}
        robj = das_func('max', 'block.replica.size', rows)
        self.assertEqual(expect, dict(result=robj.result, _id=robj.obj_id))

    def test_das_aggregators(self):
        """test das aggregator functions"""
        rows = []
        data = {'block': {'name':'AAA', 'replica': [{'name':'a', 'size':1}, {'name':'b', 'size':10}] }}
        data.update({'_id' : 1})
        rows.append(data)
        data = {'block': {'name':'AAA', 'replica': [{'name':'a', 'size':2}, {'name':'b', 'size':20}] }}
        data.update({'_id' : 2})
        rows.append(data)

        expect = {'value': 33}
        result = das_sum('block.replica.size', rows)
        self.assertEqual(expect, result)

        expect = {'value': 4}
        result = das_count('block.replica.size', rows)
        self.assertEqual(expect, result)

        expect = {'value': 1, '_id': 1}
        result = das_min('block.replica.size', rows)
        self.assertEqual(expect, result)

        expect = {'value': 20, '_id': 2}
        result = das_max('block.replica.size', rows)
        self.assertEqual(expect, result)

        expect = {'value': (1+10+2+20)/4.}
        result = das_avg('block.replica.size', rows)
        self.assertEqual(expect, result)

        expect = {'value': (10+2)/2}
        result = das_median('block.replica.size', rows)
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()
