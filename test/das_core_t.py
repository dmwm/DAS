#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS core module
"""

import os
import socket
import unittest

from pymongo import MongoClient

from DAS.utils.das_config import das_readconfig
from DAS.core.das_core import DASCore
from DAS.core.das_query import DASQuery
from DAS.utils.ddict import DotDict
from DAS.utils.utils import deepcopy

class testDASCore(unittest.TestCase):
    """
    A test class for the DAS core module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug = 0
        self.das = DASCore(debug=debug, multitask=False)
        config = deepcopy(das_readconfig())
        dburi = config['mongodb']['dburi']
        connection = MongoClient(dburi)
        connection.drop_database('das') 

    def testAggregators(self):
        """test DASCore aggregators via zip service"""
        # test DAS workflow
        query = "file dataset=/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM | grep file.size | sum(file.size)"
        dquery = DASQuery(query)
        result = self.das.call(dquery)
        result = self.das.get_from_cache(dquery)
        result = [r for r in result][0]
        if  'das' in result:
            del result['das'] # strip off DAS info
        expect = {"function": "sum", "result": {"value": 5658838455}, 
                  "key": "file.size", "_id":0}
        # the result may have value == 'N/A' when test is run w/o certificates (travis)
        # in this cas we just skip it
        if result['result']['value'] != 'N/A':
            self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()


