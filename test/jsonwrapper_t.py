#!/usr/bin/env python
#pylint: disable-msg=R0904,F0401
# pylint disabled: R0904: applies to inherited module; F0401 - can not import

"""
Unit test for jsonwrapper
"""
from __future__ import print_function

import sys
import unittest
import traceback
from DAS.utils import jsonwrapper as myjson


class TestUtils(unittest.TestCase):
    """
    A test class for the jsonwrapper module
    """

    def setUp(self):
        """
        set up DAS core module
        """
        self.data = {'a': 1, 'b': [{'c': 1, 'd': ['1', '2']}]}
        self.module = myjson.MODULE

    @classmethod
    def run_allowing_importerror(cls, test_func):
        """
        return True if there was no exceptions,
        allows import error to happen
        """
        try:
            test_func()
        except ImportError:
            print('\n' + '-' * 70)
            print('A test failed, but this might be recoverable:')
            traceback.print_exc(file=sys.stdout)
        else:
            return True

    def test_main(self):
        """ require tests to pass at least with one json module
         i.e. allow to pass if yajl or cjson raise ImportError """
        try:
            outcomes = [self.run_allowing_importerror(self._test_json),
                        self.run_allowing_importerror(self._test_cjson),
                        self.run_allowing_importerror(self._test_yajl)]
            if not any(outcomes):
                self.fail('The tests failed for all json modules...')
        finally:
            # revert any monkey-patching changes to jsonwrapper module
            myjson.MODULE = self.module

    def _test_json(self):
        """ Test json wrapper """
        myjson.MODULE = 'json'
        expect = self.data
        result = myjson.loads(myjson.dumps(self.data))
        self.assertEqual(expect, result)

        expect = self.data
        result = myjson.JSONDecoder().decode(
            myjson.JSONEncoder().encode(self.data))
        self.assertEqual(expect, result)

        data = {'a': 1, 'b': 2}
        kwds = {'sort_keys': True}
        result1 = myjson.JSONEncoder(**kwds).encode(data)
        data = {'b': 2, 'a': 1}
        result2 = myjson.JSONEncoder(**kwds).encode(data)
        self.assertEqual(result1, result2)

        myjson.MODULE = self.module

    def _test_cjson(self):
        """ Test cjson wrapper """
        import cjson

        myjson.MODULE = 'cjson'
        myjson.cjson = cjson
        expect = self.data
        result = myjson.loads(myjson.dumps(self.data))
        self.assertEqual(expect, result)

        expect = self.data
        result = myjson.JSONDecoder().decode(
            myjson.JSONEncoder().encode(self.data))
        self.assertEqual(expect, result)

        myjson.MODULE = self.module

    def _test_yajl(self):
        """Test yajl wrapper"""
        import yajl
        myjson.MODULE = 'yajl'
        myjson.yajl = yajl  # so not to depend on import sequence
        expect = self.data
        result = myjson.loads(myjson.dumps(self.data))
        self.assertEqual(expect, result)

        expect = self.data
        result = myjson.JSONDecoder().decode(
            myjson.JSONEncoder().encode(self.data))
        self.assertEqual(expect, result)

        data = {'a': 1, 'b': 2}
        kwds = {'sort_keys': True}
        result1 = myjson.JSONEncoder(**kwds).encode(data)
        data = {'b': 2, 'a': 1}
        result2 = myjson.JSONEncoder(**kwds).encode(data)
        self.assertEqual(result1, result2)

        data = 123
        result1 = myjson.JSONEncoder(**kwds).encode(data)
        kwds = {'sort_keys': True}
        result2 = myjson.JSONEncoder(**kwds).encode(data)
        self.assertEqual(result1, result2)

        data = {'a': 123, 'b': [1, 2, 3], 'c': {'d': [1, '2', 3]}}
        result1 = myjson.JSONEncoder().encode(data)
        result2 = myjson.JSONDecoder().decode(result1)
        self.assertEqual(data, result2)

        data = {'a': 123, 'b': [1, 2, 3], 'c': {'d': [1, '2', 3]}}
        result1 = myjson.dumps(data)
        result2 = myjson.loads(result1)
        self.assertEqual(data, result2)

        myjson.MODULE = self.module

#
# main
#
if __name__ == '__main__':
    unittest.main()
