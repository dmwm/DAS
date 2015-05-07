#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS cache module
"""

import json
import time
import unittest
from urllib2 import URLError
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import PrintManager
from DAS.utils.utils import deepcopy
from DAS.utils.url_utils import urllib2_request

class testDASCache(unittest.TestCase):
    """
    A test class for the DAS cache module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug    = 0
        config   = deepcopy(das_readconfig())
        logger   = PrintManager('TestDASCache', verbose=debug)
        config['logger']  = logger
        config['verbose'] = debug

    def test_cachemgr(self):
        """test cacher server functionality"""
        headers = {"Accept": "application/json"}
        query   = "ip=137.138.141.145 | grep ip.City"
        idx     = 0
        limit   = 10
        expire  = 60
        params  = {'query':query, 'idx':idx, 'limit':limit, 'expire':expire}
        host    = 'http://localhost:8211'

        # delete query
        request = 'DELETE'
        path    = '/rest/delete'
        try:
            data    = urllib2_request(request, host+path, params)
            expect  = {"status":"success"}
            result  = json.loads(data)
            self.assertEqual(expect["status"], result["status"])
        except URLError as exp:
            pass
        
        # post request
        headers = {"Accept": "application/json", "Content-type": "application/json"}
        request = 'POST'
        path    = '/rest/create'
        try:
            data    = urllib2_request(request, host+path, params, headers)
            expect  = {"status": "requested", "query": query, "expire":expire}
            result  = json.loads(data)
            self.assertEqual(expect["status"], result["status"])
        except URLError as exp:
            pass

        # get data
        request = 'GET'
        path    = '/rest/request'
        try:
            data    = urllib2_request(request, host+path, params)
            expect  = {"status": "success"}
            result  = json.loads(data)
            self.assertEqual(expect["status"], result["status"])
        except URLError as exp:
            pass

#
# main
#
if __name__ == '__main__':
    unittest.main()
