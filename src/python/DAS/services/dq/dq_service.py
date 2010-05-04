#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DQ service
"""
__revision__ = "$Id: dq_service.py,v 1.1 2009/06/25 18:05:27 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

try:
    import json # python 2.6 and later
except:
    import simplejson as json
import types
import urllib2
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import genkey, map_validator

class DQService(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dq', config)
        self._keys = None
        self.map = self.dq_map()
        map_validator(self.map)

    def dq_map(self):
        """
        Fetch DQ map
        """
        if  self._keys:
            return self._keys
        url    = self.url
        params = {"api":"doc"}
        data = urllib2.urlopen(url, json.dumps(params))
        res  = data.read()
        return json.loads(res)
