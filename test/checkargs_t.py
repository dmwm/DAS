#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS checkargs decorators
"""

import time
import unittest
from   cherrypy import HTTPError
from   DAS.web.utils import checkargs

DAS_WEB_INPUTS = ['input', 'idx', 'limit', 'collection', 'name', 'system',
    'qcache', 'reason', 'instance', 'view', 'query', 'fid', 'pid', 'next',
    'kwquery']

@checkargs(DAS_WEB_INPUTS)
def func_web(*args, **kwds):
    """Test function"""
    pass

class testCheckArgs(unittest.TestCase):
    """
    A test class for the DAS checkargs decorators
    """
    def test_web(self):
        """
        Test checkargs of DAS web server, here is supported list
        supported = ['input', 'idx', 'limit', 'collection', 
                     'format', 'sort', 'dir', 'view', 'method']
        """

        arg    = [0]
        kwds   = dict(idx='1', limit='1', 
                input='site=T1', collection='merge', view='list')
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

#
# main
#
if __name__ == '__main__':
    unittest.main()
