#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DQ service
"""
__revision__ = "$Id: dq_service.py,v 1.3 2009/07/22 20:40:10 valya Exp $"
__version__ = "$Revision: 1.3 $"
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
        jsondict = json.loads(res)
        # TEMP fix, until Anzar will fix its map, it should not contain dataset
        # and dqflaglist should be replaced with dqflags.
        # {u'listRuns4DQ': {u'keys': [u'dataset', u'run', u'dqflaglist'], 
        #  u'params': {u'api': u'listRuns4DQ', u'DQFlagList': u'list', u'dataset': u'string'}}, 
        #  u'listSubSystems': {u'keys': [u'subsystems'], u'params': {u'api': u'listSubSystems'}}}
        for key, val in jsondict.items():
            if  val.has_key('keys'):
                keys = val['keys']
                if  'dataset' in keys:
                    keys.remove('dataset')
                if  'dqflaglist' in keys:
                    keys.remove('dqflaglist')
                    keys.append('dqflags')
                val['keys'] = keys
                jsondict[key] = val
        return jsondict

    def adjust_params(self, args):
        """
        Data-service specific parser to adjust parameters according to
        specifications.
        """
        for key, val in args.items():
            args[key] = param_parser(val)

