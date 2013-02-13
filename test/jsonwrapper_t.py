#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for jsonwrapper
"""



import unittest
import urllib2, urllib
import DAS.utils.jsonwrapper as myjson

class testUtils(unittest.TestCase):
    """
    A test class for the jsonwrapper module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.data = {'a':1, 'b':[{'c':1, 'd':['1','2']}]}
        self.module = myjson.MODULE

    def test_json(self):
        """Test json wrapper"""
        myjson.MODULE = 'json'
        expect  = self.data
        result  = myjson.loads(myjson.dumps(self.data))
        self.assertEqual(expect, result)

        expect  = self.data
        result  = myjson.JSONDecoder().decode(myjson.JSONEncoder().encode(self.data))
        self.assertEqual(expect, result)

        data = {'a':1, 'b':2}
        kwds = {'sort_keys':True}
        result1 = myjson.JSONEncoder(**kwds).encode(data)
        data = {'b':2, 'a':1}
        result2 = myjson.JSONEncoder(**kwds).encode(data)
        self.assertEqual(result1, result2)

        myjson.MODULE = self.module

    def test_cjson(self):
        """Test cjson wrapper"""
        import cjson
        myjson.MODULE = 'cjson'
        myjson.cjson = cjson
        expect  = self.data
        result  = myjson.loads(myjson.dumps(self.data))
        self.assertEqual(expect, result)

        expect  = self.data
        result  = myjson.JSONDecoder().decode(myjson.JSONEncoder().encode(self.data))
        self.assertEqual(expect, result)

        myjson.MODULE = self.module

    def test_yajl(self):
        """Test yajl wrapper"""
        import yajl
        myjson.MODULE = 'yajl'
        expect  = self.data
        result  = myjson.loads(myjson.dumps(self.data))
        self.assertEqual(expect, result)

        expect  = self.data
        result  = myjson.JSONDecoder().decode(myjson.JSONEncoder().encode(self.data))
        self.assertEqual(expect, result)

        data = {'a':1, 'b':2}
        kwds = {'sort_keys':True}
        result1 = myjson.JSONEncoder(**kwds).encode(data)
        data = {'b':2, 'a':1}
        result2 = myjson.JSONEncoder(**kwds).encode(data)
        self.assertEqual(result1, result2)

        data = 123
        result1 = myjson.JSONEncoder(**kwds).encode(data)
        kwds = {'sort_keys':True}
        result2 = myjson.JSONEncoder(**kwds).encode(data)
        self.assertEqual(result1, result2)

        data = {'a':123, 'b':[1, 2, 3], 'c':{'d':[1, '2', 3]}}
        result1 = myjson.JSONEncoder().encode(data)
        result2 = myjson.JSONDecoder().decode(result1)
        self.assertEqual(data, result2)

        data = {'a':123, 'b':[1, 2, 3], 'c':{'d':[1, '2', 3]}}
        result1 = myjson.dumps(data)
        result2 = myjson.loads(result1)
        self.assertEqual(data, result2)

        myjson.MODULE = self.module

#
# main
#
if __name__ == '__main__':
    unittest.main()
