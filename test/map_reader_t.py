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
            "format": "XML",
            "params": {"name": ""}, 
            "services": "",
            "expire": 3600, 
            "wild_card": "*",
            "lookup": "site",
            "das_map": [{"rec_key": "site.name", "das_key": "site", "api_arg": "name"}],
            "ts": 123,
            "type":"service"
        }
        self.presentation = {"presentation": 
                {"city": [{"ui": "City", "das": "city.name"}, 
                          {"ui": "Address", "das": "city.Placemark.address"}]},
                "ts":123, "type": "presentation"}
        self.notations = {"notations": 
                [{"rec_key": "name", "api": "", "api_output": "cmsname"}, 
                 {"rec_key": "name", "api": "", "api_output": "cms_name"}], 
                "system": "sitedb", "ts": 123, "type": "notation"}

    def testReader(self): 
        """test read_service_map function"""
        apimap  = dict(self.apimap)
        del apimap['ts']
        fdescr  = tempfile.NamedTemporaryFile()
        mapfile = fdescr.name
        stream  = open(mapfile, 'w')
        yaml.dump(apimap, stream)
        result  = [r for r in read_service_map(mapfile)][0]
        result.pop('ts')
        result.pop('hash')
        self.assertEqual(apimap, result)
        stream.close()

    def testValidator(self):
        """test validator function"""
        record = dict(self.apimap)
        result = validator(record)
        self.assertEqual(True, result)
        del record['ts']
        result = validator(record)
        self.assertEqual(False, result)

        record = dict(self.presentation)
        result = validator(record)
        self.assertEqual(True, result)
        del record['ts']
        result = validator(record)
        self.assertEqual(False, result)

        record = dict(self.notations)
        result = validator(record)
        self.assertEqual(True, result)
        del record['ts']
        result = validator(record)
        self.assertEqual(False, result)


#
# main
#
if __name__ == '__main__':
    unittest.main()


