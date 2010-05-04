#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS map reader
"""

import yaml
import unittest
import tempfile
from DAS.services.map_reader import read_service_map

class testDBS(unittest.TestCase):
    """
    A test class for the DAS map reader
    """

    def testReader(self): 
        """test read_service_map function"""
        apimap  = {
            "url": "https://a.b.com", "system": "sitedb", 
            "api": "CMSNametoSE",
            "format": "XML",
            "params": {"name": ""}, 
            "expire": 3600, 
            "record" : {
                "daskeys": [{"map": "site.name", "key": "site", "pattern": ""}],
                "api2das": [{"pattern": "", "das_key": "site", "api_param": "name"}], 
            }
        }
        expect  = {'api2das': [{'pattern': '', 'api_param': 'name', 'das_key': 'site'}], 
                'api': {'params': {'name': ''}, 'name': 'CMSNametoSE'}, 
                'format': 'XML',
                'expire': 3600, 'url': 'https://a.b.com', 'system': 'sitedb', 
                'daskeys': [{'map': 'site.name', 'key': 'site', 'pattern': ''}]}
        fdescr  = tempfile.NamedTemporaryFile()
        mapfile = fdescr.name
        stream  = file(mapfile, 'w')
        yaml.dump(apimap, stream)
        result  = [r for r in read_service_map(mapfile, field="api")][0]
        result.pop('created')
        self.assertEqual(expect, result)

#
# main
#
if __name__ == '__main__':
    unittest.main()


