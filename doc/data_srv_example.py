#!/usr/bin/env python

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator

class MyService(DASAbstractService):
    def __init__(self, config):
        DASAbstractService.__init__(self, 'mysrv', config)
        self.map = {
            'myapi' : {
                'keys': ['service_key_to_be_used_in_DAS'],
                'params' : {'name':''}
            },
        }
        map_validator(self.map)

    def api(self, query, cond_dict=None):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        Return: [{'key':'value', 'key2':'value2'}, ...]
        """
        # construct params out of provided query and cond_dict
        data  = self.getdata(self.url, params)
        # parse output data here
        return data

