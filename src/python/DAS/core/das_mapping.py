#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mapping
"""

__revision__ = "$Id: das_mapping.py,v 1.2 2009/04/13 20:39:18 valya Exp $"
__version__ = "$Revision: 1.2 $"
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
    return name

def jsonparser(jsondict, ikey):
    """
    Find desired values in provided json dict.
    """
    if  type(jsondict) is not types.DictType:
        return
    if  ikey.count('.'): # composed key
        key, attr = ikey.split('.')
    else:
        key  = ikey
        attr = ''
#    print "### ikey='%s' key='%s', attr='%s'" % (ikey, key, attr)
#    print "### jsondict", jsondict
    if  jsondict.has_key(key):
        for k, v in jsondict.items():
            if  k != key:
                continue
            if  not attr:
                return v
            if  type(v) is types.ListType:
#                olist = []
#                for i in v:
#                    res = jsonparser(i, attr)
#                    if  res:
#                        olist.append(res)
#                return olist
                return [jsonparser(i, attr) for i in v]
            elif  type(v) is types.DictType:
                if  v.has_key(attr):
                    return v[attr]
    else:
        for k in jsondict.keys():
            val = jsonparser(jsondict[k], ikey)
            if  val:
                return val
    return
