#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mapping
"""

__revision__ = "$Id: das_mapping.py,v 1.6 2009/05/13 15:19:32 valya Exp $"
__version__ = "$Revision: 1.6 $"
__author__ = "Valentin Kuznetsov"

import os
import types
import ConfigParser
from DAS.utils.utils import transform_dict2list
from DAS.utils.utils import izip_longest

def read_config(cname):
    """
    Return DAS map file found in $DAS_ROOT/etc/cname
    """
    dasconfig = ''
    if  os.environ.has_key('DAS_ROOT'):
        dasconfig = os.path.join(os.environ['DAS_ROOT'], 'etc/%s' % cname)
        if  not os.path.isfile(dasconfig):
            raise EnvironmentError('No DAS config file %s found' % dasconfig)
    else:
        raise EnvironmentError('DAS_ROOT environment is not set up')
    config = ConfigParser.ConfigParser()
    config.read(dasconfig)
    mapdict = {}
    for system in config.sections():
        sectdict = {}
        for opt in config.options(system):
            sectdict[opt] = config.get(system, opt).split(',')
        mapdict[system] = sectdict
    return mapdict

def parselist(result):
    """
    parse input list and look-up if it's elements are lists
    if so, parse them and make final list. Used by translate.
    """
    if  type(result) is not types.ListType:
        return [result]
    olist = []
    for item in result:
        if  type(item) is types.ListType:
            for elem in item:
                olist.append(elem)
        else:
            olist.append(item)
    return olist

def das2api(system, name):
    """
    Translate DAS QL key, name, into data-service API input parameter
    """
    result = translate('das2api.cfg', system, name)
    return parselist(result)

def das2result(system, name):
    """
    Translate DAS QL key, name, into data-service result dict, e.g.
    block.complete => block.replica.complete
    """
    result = translate('dasmap.cfg', system, name)
    return parselist(result)

def result2das(system, name):
    """
    Translate data-service output into DAS QL keys, e.g.
    block.replica.complete => block.complete
    """
    dasmap = read_config('dasmap.cfg')
    olist  = []
    if  dasmap.has_key(system):
        mapdict = dasmap[system]
        for k, v in mapdict.items():
            if  v.count(name):
                olist.append(k)
        if  olist:
            return olist
    return [name]

def result2das_v1(system, name):
    """
    Translate data-service output into DAS QL keys, e.g.
    block.replica.complete => block.complete
    """
    dasmap = read_config('dasmap.cfg')
    olist  = []
    if  dasmap.has_key(system):
        mapdict = dasmap[system]
        for k, v in mapdict.items():
            if  name.find('_') != -1: # we got composite key
                composed_key = name.split('_')
                composed_key.sort()
                v.sort()
                if  v == composed_key:
                    olist.append(k)
            if  v.count(name):
#                return k
                olist.append(k)
        if  olist:
            return olist
    return name

def translate(cname, system, name):
    """
    Translate given QL key into data-service notation.
    """
    dasmap = read_config(cname)
    if  not dasmap.has_key(system):
        return name
    for key, val in dasmap[system].items():
        if  key == name:
#            if  type(val) is types.ListType:
#                return val[0]
            return val
    return name

def jsonparser(system, jsondict, keylist):
    """
    Find desired values in provided json dict.
    It is generalization of jsonparser4key function which
    deals with a list of input keys. The output should be
    a list of rows. Each row is a dict whose values are simple
    types (including dict), but not lists.
    """
    for key in jsondict.keys():
        if  key.lower() == 'exception' or key.lower() == 'error':
            msg = jsondict[key]
            raise Exception(msg)

    def yieldrows(system, jsondict, key):
        res = jsonparser4key(system, jsondict, key)
        if  not res:
            yield {key:''}
        if  type(res) is types.ListType:
            for value in res:
                yield {key:value}
        else:
            yield {key:res}

    def helper(system, jsondict, key):
        for item in yieldrows(system, jsondict, key):
            yield item
    tdict = {}
    for key in keylist:
        tdict[key] = helper(system, jsondict, key)
        
    newrow  = {}
    keydict = {}
    for key in keylist:
        res = result2das(system, key)
        keydict[key] = res
        for newkey in result2das(system, key):
            newrow[newkey] = ''
    def add_to_list(row, newkey, res):
        if  type(res) is types.ListType:
            for irow in res:
                if  irow == res[0]:
                    newrow = row
                else:
                    newrow = dict(row)
                newrow[newkey] = irow
                yield newrow
        else:
            row[newkey] = res
            yield row
    olist   = []
    for key, val in tdict.items():
        if  not olist:
            for item in val:
                row = dict(newrow)
                for newkey in keydict[key]:
                    res = item[key]
                    olist += [k for k in add_to_list(row, newkey, res)]
        else:
            _olist = []
            for row, item in zip(olist, val):
                for newkey in keydict[key]:
                    res = item[key]
                    _olist = [k for k in add_to_list(row, newkey, res)]
            for item in _olist:
                if  item not in olist:
                    olist.append(item)
    return olist

def jsonparser_v1(system, jsondict, keylist):
    """
    Find desired values in provided json dict.
    It is generalization of jsonparser4key function which
    deals with a list of input keys. The output should be
    a list of rows. Each row is a dict whose values are single, 
    no lists.
    """
    for key in jsondict.keys():
        if  key.lower() == 'exception' or key.lower() == 'error':
            msg = jsondict[key]
            raise Exception(msg)
    def yieldrows(system, jsondict, key):
        res = jsonparser4key(system, jsondict, key)
        if  not res:
            yield {key:''}
        if  type(res) is types.ListType:
            for value in res:
                yield {key:value}
        else:
            yield {key:res}

    tlist = ((item for item in yieldrows(system, jsondict, key)) \
                for key in keylist)
    olist = []
    for tup in zip(*tlist):
        newrow = {}
        for item in tup:
            for k, v in item.items():
                newkey = result2das(system, k)
                if  type(newkey) is types.ListType:
                    for nkey in newkey:
                        newrow[nkey] = v
                else:
                    newrow[newkey] = v
        foundlist = 0
        for val in newrow.values():
            if  type(val) is types.ListType:
                foundlist = 1
        if  foundlist:
            for irow in transform_dict2list(newrow):
                olist.append(irow)
        else:
            olist.append(newrow)
    return olist

def jsonparser4key(system, jsondict, ikey):
    """
    Find desired values in provided json dict. The input key, ikey,
    can be in a form a.b.c so we split it in parts and look-up in 
    provided jsondict such dict structure. For example,
    jsondict = {'block' : {'files':4, replica:[{'se':T2}, ...],...}}
    In this case we should be able to find all values for 
    block.replica.se
    """
    if  type(jsondict) is not types.DictType:
        return
    if  ikey.count('.'): # composed key
        key, attr = ikey.split('.', 1)
    else:
        key  = ikey
        attr = ''
    # if we got composite key
#    if  key.find('_') != -1:
#        for k in key.split('_'):
#            if  jsondict.has_key(k.strip()):
#                return jsondict
    if  jsondict.has_key(key):
        for k, v in jsondict.items():
            if  k != key:
                continue
            if  not attr:
                return v
            if  type(v) is types.ListType:
                return [jsonparser4key(system, i, attr) for i in v]
            elif  type(v) is types.DictType:
                if  v.has_key(attr):
                    return v[attr]
    else:
        olist = []
        for k in jsondict.keys():
            val = jsonparser4key(system, jsondict[k], ikey)
            if  val:
#                return val
                if  type(val) is types.ListType:
                    for item in val:
                        olist.append(item)
                else:
                    olist.append(val)
        return olist
    return

def jsonparser4key_v1(jsondict, ikey):
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
#                    res = jsonparser4key(i, attr)
#                    if  res:
#                        olist.append(res)
#                return olist
                return [jsonparser4key(i, attr) for i in v]
            elif  type(v) is types.DictType:
                if  v.has_key(attr):
                    return v[attr]
    else:
        for k in jsondict.keys():
            val = jsonparser4key(jsondict[k], ikey)
            if  val:
                return val
    return
