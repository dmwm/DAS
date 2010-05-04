#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mapping
"""

__revision__ = "$Id: das_mapping.py,v 1.10 2009/07/10 19:26:10 valya Exp $"
__version__ = "$Revision: 1.10 $"
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

def api2das(system, name):
    """
    Translate data-service API input parameter into DAS QL key,
    LumiDB uses run_number, while DAS uses run
    """
    dasmap = read_config('dasmap.cfg')
    keys = []
    if  dasmap.has_key(system):
        for key, values in dasmap[system].items():
            if  name in values:
                keys.append(key)
    if  keys:
        return keys
    return [name]

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

def translate(cname, system, name):
    """
    Translate given QL key into data-service notation.
    """
    dasmap = read_config(cname)
    if  not dasmap.has_key(system):
        return name
    for key, val in dasmap[system].items():
        if  key == name:
            return val
    return name

# NEW json2das/jsonparser implementation
def json2das(system, input, keylist, selkeys):
    """
    Convert rows from provided input dict (input) into DAS rows
    using system name , e.g. phedex, dbs, etc., data-service
    output keys (keylist) and selection key (selkeys) from DAS QL.
    """
    # NOTE: the input dict should be in a form of
    # {'system':{result_dict}} as phedex or
    # {'1':{result dict}, } as sitedb
    for row in jsonparser(input, keylist):
        for key in keylist:
            found = 0
            for nkey in result2das(system, key):
                if  nkey != key and nkey in selkeys:
                    row[nkey] = row[key]
                    found = 1
            if  found:
                del row[key]
#        yield row
        count = 0
        for newrow in json2das_zip(row):
            count = 1
            yield newrow
        if  not count:
            yield row

def json2das_zip(row):
    """
    Analyze input row and if key value is a list, expand it in a list
    of rows with the same keys.
    """
    longest = 0
    for val in row.values():
        if  type(val) is types.ListType:
            if  len(val) > longest:
                longest = len(val)
    if  not longest:
        yield row
    else:
        for key, val in row.items():
            if  type(val) is not types.ListType:
                 row[key] = [val for i in range(0, longest)]
        for i in range(0, longest):
            newrow = {}
            for key, val in row.items():
                newrow[key] = val[i]
            yield newrow

def jsonparser(input, keylist):
    """
    Yields rows from provided input dict (input) and key list (keylist)
    """
    row = {}
    for key in keylist:
        gen = jsonparser4key(input, key)
#        if  gen:
        if  type(gen) is types.GeneratorType or type(gen) is types.ListType:
            item = [i for i in gen]
            if  not item:
                item = ''
            if  len(item) == 1:
                row[key] = item[0]
            else:
                row[key] = item
            if  len(row.keys()) == len(keylist):
                yield row
        elif gen:
            row[key] = gen
            yield row
        else:
            row[key] = ''
            yield row

def jsonparser4key(jsondict, ikey):
    """
    Yield value for provided key from given jsondict
    """
    if  type(jsondict) is not types.DictType:
        return
    if  ikey.count('.'): # composed key
        key, attr = ikey.split('.', 1)
    else:
        key  = ikey
        attr = ''
    if  jsondict.has_key(key):
        for k, v in jsondict.items():
            if  k != key:
                continue
            if  not attr:
                return v
            if  type(v) is types.ListType:
                return [jsonparser4key(i, attr) for i in v]
            elif  type(v) is types.DictType:
                if  v.has_key(attr):
                    return v[attr]
    else:
        def helper(helper, ikey):
            for k in jsondict.keys():
                val = jsonparser4key(jsondict[k], ikey)
                if  val:
                    if  type(val) is types.ListType or \
                        type(val) is types.GeneratorType:
                        for item in val:
                            yield item
                    else:
                        yield val
        return helper(jsondict, ikey)
# END
