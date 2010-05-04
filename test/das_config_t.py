#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS config module
"""

import os
import unittest
from utils.das_config import das_configfile, das_readconfig, das_writeconfig

class testDASConfig(unittest.TestCase):
    """
    A test class for the DAS config module
    """

    def testEnvConfig(self):
        """test das configuration file"""
        if  os.environ.has_key('DASHOME'):
            del os.environ['DASHOME']
        self.assertRaises(EnvironmentError, das_configfile)
        
    def testConfig(self):                          
        """test read/write of configuration file"""
        das_writeconfig()
        readdict = das_readconfig()
        result   = readdict['systems']
        result.sort()
        expect   = ['dbs', 'phedex', 'sitedb', 'runsum']
        expect.sort()
        self.assertEqual(expect, result)
#
# main
#
if __name__ == '__main__':
    unittest.main()


