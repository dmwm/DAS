#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mapping
"""

__revision__ = "$Id: das_mapping.py,v 1.1 2009/04/13 19:03:40 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import os
import types
import ConfigParser

def dasmap_config():
    """
    Return DAS map file found in $DAS_ROOT/etc/dasmap.cfg
    """
    dasconfig = ''
    if  os.environ.has_key('DAS_ROOT'):
        dasconfig = os.path.join(os.environ['DAS_ROOT'], 'etc/dasmap.cfg')
        if  not os.path.isfile(dasconfig):
            raise EnvironmentError('No DAS config file %s found' % dasconfig)
    else:
        raise EnvironmentError('DAS_ROOT environment is not set up')
    config = ConfigParser.ConfigParser()
    config.read(dasconfig)
    mapdict = {}
    for sect in config.sections():
        sectdict = {}
        for opt in config.options(sect):
            sectdict[opt] = config.get(sect, opt).split(',')
        mapdict[sect] = sectdict
    return mapdict

def translate(system, name):
    """
    Translate given QL key into data-service notation.
    """
    dasmap = dasmap_config()
    if  not dasmap.has_key(name):
        return
    for srv, val in dasmap[name].items():
        if  srv == system:
            return val

def finddata(jsondict):
    """
    Find desired values in provided json dict.
{"phedex":{"request_timestamp":1239641469.37755,"instance":"prod","request_url":"http://cmsweb.cern.ch:7001/phedex/datasvc/json/prod/blockReplicas","request_version":"1.3.1","block":[{"bytes":"291825111","files":"4","name":"/Cosmics/CRUZET4_v1_CRZT210_V1_SuperPointing_v1/RECO#96c1b23f-1d88-4aa5-96ed-966a73a38c2d","is_open":"n","id":"426474","replica":[{"bytes":"291825111","files":"4","node":"T1_US_FNAL_Buffer","time_create":"1219412876.0661","time_update":"1229179156.51997","group":null,"se":"cmssrm.fnal.gov","node_id":"9","custodial":"n","complete":"y"},{"bytes":"291825111","files":"4","node":"T1_US_FNAL_MSS","time_create":"1219413379.70068","time_update":"1229179156.51997","group":null,"se":"cmssrm.fnal.gov","node_id":"10","custodial":"y","complete":"y"},{"bytes":"291825111","files":"4","node":"T2_CH_CAF","time_create":"1219413630.40003","time_update":"1229179156.51997","group":null,"se":"caf.cern.ch","node_id":"501","custodial":"n","complete":"y"}]}],"request_call":"blockReplicas","call_time":"0.00833","request_date":"2009-04-13 16:51:09 UTC"}}
    """
    return
