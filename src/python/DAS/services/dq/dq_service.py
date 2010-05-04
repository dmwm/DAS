#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DQ data-service plugin.
"""
__revision__ = "$Id: dq_service.py,v 1.7 2009/11/20 01:00:55 valya Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "Valentin Kuznetsov"

import re
import types
import urllib2
#import DAS.utils.jsonwrapper as json
import json
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
                    break
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
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def adjust_params(self, args):
        """
        Data-service specific parser to adjust parameters according to
        specifications.
        """
        for key, val in args.items():
            args[key] = param_parser(val)

    def parser(self, source, api, params=None):
        """
        Dashboard data-service parser.
        """
        jsondict = json.load(source)
        for key in jsondict.keys():
            newkey = self.dasmapping.notation2das(self.name, key)
            if  newkey != key:
                jsondict[newkey] = jsondict[key]
                del jsondict[key]
        yield jsondict
        source.close()

