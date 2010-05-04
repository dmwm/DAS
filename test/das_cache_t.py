#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS cache module
"""

import time
import unittest
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
from DAS.core.das_cache import DASCache
from DAS.web.utils import urllib2_request

class testDASCache(unittest.TestCase):
    """
    A test class for the DAS cache module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug    = 0
        config   = das_readconfig()
        logger   = DASLogger(verbose=debug, stdout=debug)
        config['logger']  = logger
        config['verbose'] = debug
        self.dascache = DASCache(config)

    def test_cachemgr(self):
        """test cacher server functionality"""
        query   = 'find site where site=T2_UK'
        idx     = 0
        limit   = 10
        expire  = 60
        params  = {'query':query, 'idx':idx, 'limit':limit, 'expire':expire}
        host    = 'http://localhost:8011'

        # delete query
        request = 'DELETE'
        path    = '/rest/json/%s' % request
        data    = urllib2_request(host+path, params)
        expect  = {"status":"success"}
        self.assertEqual(expect, eval(data))
        print "pass", path
        
        # get data
        request = 'GET'
        path    = '/rest/json/%s' % request
        data    = urllib2_request(host+path, params)
        expect  = {"status": "not found", "query": query, "limit": limit, "idx": idx}
        self.assertEqual(expect, eval(data))
        print "pass", path

        # put request
        request = 'PUT'
        path    = '/rest/json/%s' % request
        data    = urllib2_request(host+path, params)
        expect  = {"status": "requested", "query": query, "expire":expire}
        self.assertEqual(expect, eval(data))
        print "pass", path

        time.sleep(3)

        # get data
        request = 'GET'
        path    = '/rest/json/%s' % request
        data    = urllib2_request(host+path, params)
        result  = [{"system": "sitedb", "site": "gw-3.ccc.ucl.ac.uk"}, 
                   {"system": "sitedb", "site": "gw-3.ccc.ucl.ac.uk"}, 
                   {"system": "sitedb", "site": "srm.glite.ecdf.ed.ac.uk  "}, 
                   {"system": "sitedb", "site": "srm.glite.ecdf.ed.ac.uk  "}, 
                   {"system": "sitedb", "site": "heplnx204.pp.rl.ac.uk"}, 
                   {"system": "sitedb", "site": "lcgse01.phy.bris.ac.uk"}, 
                   {"system": "sitedb", "site": "gfe02.hep.ph.ic.ac.uk"}, 
                   {"system": "sitedb", "site": "gfe02.hep.ph.ic.ac.uk"}, 
                   {"system": "sitedb", "site": "dgc-grid-50.brunel.ac.uk"}, 
                   {"system": "sitedb", "site": "dgc-grid-50.brunel.ac.uk"}]
        expect  = {"status": "success", "result":result,
                   "query": query, "limit": limit, "idx": idx}
        self.assertEqual(expect, eval(data))
        print "pass", path

#
# main
#
if __name__ == '__main__':
    unittest.main()
