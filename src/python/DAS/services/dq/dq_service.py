#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DQ service
"""
__revision__ = "$Id: dq_service.py,v 1.2 2009/06/26 19:07:53 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

try:
    import json # python 2.6 and later
except:
    import simplejson as json
import re
import types
import urllib2
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import genkey, map_validator

def param_parser(param_str):
    """
    Parse input parameter string for DQ flags. They are represented in a form
    Tracker_Global=GOOD&Tracker_Local1=1 and should be converted into
    [{"Oper": "=", "Name": "Tracker_Global",  "Value": "GOOD"},...]
    """
    pat = re.compile("=|&|>|<")
    olist = []
    if  pat.search(param_str):
        for cond in param_str.split('&'):
            for op in ['<=', '>=', '!=', '<', '>', '=']:
                if  cond.find(op) != -1:
                    key, val = cond.split(op)
                    olist.append(dict(Name=key, Value=val, Oper=op))
    if  olist:
        return olist
    return param_str

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

    def adjust_params(self, args):
        """
        Data-service specific parser to adjust parameters according to
        specifications.
        """
        for key, val in args.items():
            args[key] = param_parser(val)

