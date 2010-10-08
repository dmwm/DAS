#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for CERN SSO toolkit
"""

import os
import unittest
import traceback

from   DAS.utils.utils import get_key_cert
from   DAS.utils.cern_sso_auth import get_data

def runsummary(run, debug):
    """Test RunSummary for given run number"""
    pat = '<runNumber>%s</runNumber>' % run
    key, cert = get_key_cert()
    url  = 'https://cmswbm.web.cern.ch/cmswbm/cmsdb/servlet/RunSummary?'
    url += 'RUN=%s&DB=cms_omds_lb&FORMAT=XML' % run
    data = get_data(url, key, cert, debug)
    for line in data.read().split('\n'):
        if  line == pat:
            return pat

class testCERNSSO(unittest.TestCase):
    """
    A test class for CERN SSO toolkit
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.debug = 0

    def test_get_data(self):
        """Test get_data function"""
        run = 97029
        result = runsummary(run, self.debug)
        expect = '<runNumber>%s</runNumber>' % run
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()
