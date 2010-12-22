#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS checkargs decorators
"""

import time
import unittest
from   cherrypy import HTTPError
from   DAS.web.utils import checkargs
from   DAS.web.das_web import DAS_WEB_INPUTS
from   DAS.web.das_expert import DAS_EXPERT_INPUTS
from   DAS.web.das_cache import DAS_CACHE_INPUTS

@checkargs(DAS_WEB_INPUTS)
def func_web(*args, **kwds):
    """Test function"""
    pass

@checkargs(DAS_EXPERT_INPUTS)
def func_expert(*args, **kwds):
    """Test function"""
    pass

@checkargs(DAS_CACHE_INPUTS)
def func_cache(*args, **kwds):
    """Test function"""
    pass

class testCheckArgs(unittest.TestCase):
    """
    A test class for the DAS checkargs decorators
    """
    def test_web(self):
        """
        Test checkargs of DAS web server, here is supported list
        supported = ['input', 'idx', 'limit', 'show', 'collection', 
                     'format', 'sort', 'dir', 'view', 'method']
        """

        arg    = [0]
        kwds   = dict(idx='1', limit='1', show='json',
                input='site=T1', collection='merge',
                format='xml', sort='true', dir='asc', view='list')
        result = func_web(arg, **kwds)
        expect = None
        self.assertEqual(result, expect)

        wrong  = {'mykey':'sdf'} # wrong key
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'idx':'sdf'} # wrong value
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'limit':'sdf'} # wrong value
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'collection':'sdf'} # wrong collection
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'ajax':'5'} # wrong value
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'method':'FOO'} # wrong method
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'show':'FOO'} # wrong value
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'format':'1'} # wrong value
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'sort':'1'} # wrong value
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'dir':'ascending'} # wrong value
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'ajax':'select'} # wrong value
        self.assertRaises(HTTPError, func_web, *arg, **wrong)
        wrong  = {'view':'select'} # wrong value
        self.assertRaises(HTTPError, func_web, *arg, **wrong)

    def test_expert(self):
        """
        Test checkargs of DAS expert server
        supported = ['idx', 'limit', 'collection', 'database', 'query',
                     'dasquery', 'dbcoll', 'msg']
        """
        arg    = [0]
        kwds   = dict(idx='1', limit='1', query='bla',
                        collection='merge', database='das',
                        dasquery='bla', dbcoll='das.cache', msg='bla')
        result = func_expert(arg, **kwds)
        expect = None
        self.assertEqual(result, expect)

        wrong  = {'idx':'sdf'}
        self.assertRaises(HTTPError, func_expert, *arg, **wrong)

    def test_cache(self):
        """
        Test checkargs of DAS cache server
        supported = ['query', 'idx', 'limit', 'expire', 'method',
                     'skey', 'order', 'collection']
        """
        arg    = [0]
        expire = str(long(time.time()))
        kwds   = dict(idx='1', limit='1', query='bla', expire=expire,
                method='GET', skey='site', order='asc', collection='cache')
        result = func_cache(arg, **kwds)
        expect = None
        self.assertEqual(result, expect)

        wrong  = {'idx':'sdf'}
        self.assertRaises(HTTPError, func_cache, *arg, **wrong)

#
# main
#
if __name__ == '__main__':
    unittest.main()
