#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS config module
"""

import os
import types
import unittest
from tempfile import NamedTemporaryFile
from DAS.utils.das_config import das_configfile, das_readconfig, write_configparser
from DAS.utils.utils import deepcopy

class testDASConfig(unittest.TestCase):
    """
    A test class for the DAS config module
    """

    def testEnvConfig(self):
        """test das configuration file"""
        if  'DAS_CONFIG' in os.environ:
            del os.environ['DAS_CONFIG']
        self.assertRaises(EnvironmentError, das_configfile)
        
    def testConfig(self):                          
        """test read/write of configuration file"""
        if  'DAS_CONFIG' in os.environ:
            del os.environ['DAS_CONFIG']
        fds = NamedTemporaryFile()
        os.environ['DAS_CONFIG'] = fds.name
        dasconfig = das_configfile()
        write_configparser(dasconfig, True)
        readdict = deepcopy(das_readconfig())
        self.assertEqual(dict, type(readdict))
#
# main
#
if __name__ == '__main__':
    unittest.main()


