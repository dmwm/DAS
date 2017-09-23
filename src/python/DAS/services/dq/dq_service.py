#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DQ data-service plugin.
"""
__revision__ = "$Id: dq_service.py,v 1.8 2010/02/25 14:53:49 valya Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "Valentin Kuznetsov"

import re
import json
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator

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
            for oper in ['<=', '>=', '!=', '<', '>', '=']:
                if  cond.find(oper) != -1:
                    key, val = cond.split(oper)
                    olist.append(dict(Name=key, Value=val, Oper=oper))
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

    def adjust_params(self, api, kwds, inst=None):
        """
        Data-service specific parser to adjust parameters according to
        specifications.
        """
        for key, val in kwds.items():
            kwds[key] = param_parser(val)

    def parser(self, dformat, source, api):
        """
        DQ data-service parser.
        """
        jsondict = json.load(source)
        source.close()
        for key in jsondict.keys():
            newkey = self.dasmapping.notation2das(self.name, key)
            if  newkey != key:
                jsondict[newkey] = jsondict[key]
                del jsondict[key]
        yield jsondict

