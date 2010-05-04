#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS aggregators
"""

import unittest
from DAS.core.das_aggregators import das_func
from DAS.utils.utils import dotdict

class testQLParser(unittest.TestCase):
    """
    A test class for the DAS qlparser
    """
    def testBracketObj(self):                          
        """test search for bracket objects"""
        rows = []
        data = {'block': {'name':'AAA', 'replica': [{'name':'a', 'size':1}, {'name':'b', 'size':10}] }}
        rows.append(data)
        data = {'block': {'name':'AAA', 'replica': [{'name':'a', 'size':2}, {'name':'b', 'size':20}] }}
        rows.append(data)

        expect = 33
        result = das_func('sum', 'block.replica.size', rows)
        self.assertEqual(expect, result)

        expect = 4
        result = das_func('count', 'block.replica.size', rows)
        self.assertEqual(expect, result)

        expect = 1
        result = das_func('min', 'block.replica.size', rows)
        self.assertEqual(expect, result)

        expect = 20
        result = das_func('max', 'block.replica.size', rows)
        self.assertEqual(expect, result)

        expect = 20
        drows  = [dotdict(row) for row in rows]
        result = das_func('max', 'block.replica.size', drows)
        self.assertEqual(expect, result)
#
# main
#
if __name__ == '__main__':
    unittest.main()
