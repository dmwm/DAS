#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS map reader
"""

import yaml
import unittest
import tempfile
from DAS.services.map_reader import read_service_map, validator

class testDBS(unittest.TestCase):
    """
    A test class for the DAS map reader
    """

    def setUp(self):
        """set up unit test data records"""
        self.apimap  = {
            "url": "https://a.b.com", "system": "sitedb", 
            "urn": "CMSNametoSE",
            "format": "XML", "wild_card": "*",
            "params": {"name": ""}, 
            "services": "",
            "expire": 3600, 
            "apitag": None,
            "daskeys": [{"map": "site.name", "key": "site", "pattern": ""}],
            "das2api": [{"pattern": "", "das_key": "site", "api_param": "name"}], 
            "created": 123
        }
        self.presentation = {"presentation": 
                {"city": [{"ui": "City", "das": "city.name"}, 
                          {"ui": "Address", "das": "city.Placemark.address"}]},
                "created":123}
        self.notations = {"notations": 
                [{"map": "name", "api": "", "notation": "cmsname"}, 
                 {"map": "name", "api": "", "notation": "cms_name"}], 
                "system": "sitedb", "created": 123}

    def testReader(self): 
        """test read_service_map function"""
        apimap  = dict(self.apimap)
        del apimap['created']
        fdescr  = tempfile.NamedTemporaryFile()
        mapfile = fdescr.name
        stream  = file(mapfile, 'w')
        yaml.dump(apimap, stream)
        result  = [r for r in read_service_map(mapfile)][0]
        result.pop('created')
        self.assertEqual(apimap, result)

    def testValidator(self):
        """test validator function"""
        record = dict(self.apimap)
        result = validator(record)
        self.assertEqual(True, result)
        del record['created']
        result = validator(record)
        self.assertEqual(False, result)

        record = dict(self.presentation)
        result = validator(record)
        self.assertEqual(True, result)
        del record['created']
        result = validator(record)
        self.assertEqual(False, result)

        record = dict(self.notations)
        result = validator(record)
        self.assertEqual(True, result)
        del record['created']
        result = validator(record)
        self.assertEqual(False, result)


#
# main
#
if __name__ == '__main__':
    unittest.main()


