#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
General set of useful utilities used by DAS
"""

__revision__ = "$Id: utils.py,v 1.3 2009/04/07 19:28:40 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

import re
import md5
import time
import types

def splitlist(ilist, nentries):
    """
    Split input list into a list of lists with nentries
    """
    for step in range(0, len(ilist), nentries):
        idx = step
        jdx = idx+nentries
        if  jdx > len(ilist):
            jdx = len(ilist)
        yield ilist[idx:jdx]

def genkey(query):
    """
    Generate a new key for a given query. We use md5 hash for the
    query and key is just hex representation of this hash.
    """
    keyhash = md5.new()
    keyhash.update(query)
    return keyhash.hexdigest()

def gen2list(results):
    """
    Convert generator to a list discarding duplicates
    """
    reslist = []
    for res in results:
        row = dict(res)
        if  not reslist.count(row):
            reslist.append(row)
    return reslist

def dump(reslist, limit=None):
    """
    Print items in provided generator
    """
    if  not reslist:
        print "No results found"
        return
    keys = reslist[0].keys()
    keys.sort()
    keys.remove('id')
    keys = ['id']+keys
    keysize = 0
    for key in keys:
        if  len(key) > keysize:
            keysize = len(key)

    idx  = 0
    for res in reslist:
        if  limit and idx >= limit:
            break
        for key in keys:
            padding = " "*(keysize-len(key))
            print "%s%s : %s" % (key, padding, res[key])
        print
        idx += 1

def cartesian_product(master_set, slave_set, rel_keys=None):
    """
    Create cartesian product between two provided sets w/ provided relation
    keys (rel_keys). Provided sets should be in a form of 
    [{'system':system_name, 'key':value'}, ...]
    """
    reslist = []
    # define non-null keys from result sets,
    notnullkeys = []
#    print "master", master_set
#    print "slave", slave_set
    for irow in master_set:
        row = dict(irow)
        for irow_match in slave_set:
            match = 0
            row_match = dict(irow_match)
            for key in rel_keys:
                if  row[key] == row_match[key]:
                    match += 1
            if  match != len(rel_keys): # not all keys are matched
                continue
#            print "match"
#            print row
#            print row_match
            newrow = dict(row)
            for k, val in row_match.items():
                if  val:
                    if  k == 'system':
                        if  newrow[k].find(val) == -1:
                            newrow[k] = newrow[k] + "+" + val
                    else:
                        newrow[k] = val
            if  not reslist.count(newrow):
                reslist.append(newrow)
    return reslist

def cartesian_product_v1(master_set, slave_set):
    """
    Create cartesian product between two provided sets.
    Set should be in a form of [{'system':system_name, 'key':value'}, ...]
    """
    # define non-null keys from result sets,
    notnullkeys = []
    for row in master_set:
        for row_match in slave_set:
            if  not notnullkeys:
                notnullkeys = \
                [key for key, val in row_match.items() if val]
            for key in notnullkeys:
                if  key == 'system':
                    continue
                if  row[key] == row_match[key]:
                    for k, val in row_match.items():
                        if  val:
                            if  k == 'system':
                                if  row[k].find(val) == -1:
                                    row[k] = row[k] + "+" + val
                            else:
                                row[k] = val
                    yield row

def timestamp():
    return int(str(time.time()).split('.')[0])
        
#def results2couch(query, results, processing, expire=600):
def results2couch(query, results, expire=600):
    """
    Modify results and add to each row dict the query and timestamp
    to be used by couch db.
    """
    resdict = {}
    resdict['query'] = query
    resdict['hash'] = genkey(query)
#    resdict['processing'] = processing
    tstamp = timestamp()
    resdict['timestamp'] = tstamp
    resdict['expire'] = tstamp + expire
    resdict['results'] = results
    return resdict
#    return [resdict]


def genresults(system, results, collect_list):
    """
    Generator of results for given system based on provided dict 
    of 'results' and final set 'collect_list'.
    The output rowdict in a form {'system':system_name, 'key':value}
    """
    rdict = {}
    rdict['system'] = system
    for key in collect_list:
        rdict[key] = ""

    olist = []
    for res in results:
        rowdict = dict(rdict)
        for idx in range(0, len(collect_list)):
            key = collect_list[idx]
            if  res.has_key(key):
                rowdict[key] = res[key]
        olist.append(rowdict)
    return olist

def query_params(query):
    """
    Divide input query in a set of select keys and set of parameters. All queries
    are in a form of
    find key1, key2, ... where param=val
    """
    parts = query.split(' where ')
    selkeys = parts[0].replace('find ','').split(',')
    params = {}
    if  len(parts) > 1:
        cond = parts[1]
        for oper in \
        ['!=', '=', 'not like', 'between', 'not in', ' in ', ' like ']:
            if  cond.find(oper) != -1:
                clist = cond.split(oper)
                params[clist[0].strip()]=(oper.strip(), clist[1].strip())
                break
    return selkeys, params

def transform_dict2list(indict):
    """
    transform input dictionary into list of dictionaries, e.g.
    d=['a':1, 'b':[1,2]}
    output list = [{'a':1,'b':1}, {'a':1,'b':2}]
    """
    if  str(indict).find('[') == -1: # no list found in input dict
        return [indict]
    row = {}
    for key in indict.keys():
        row[key] = ''
    data = []
    for key, val in indict.items():
        if  type(val) is types.ListType:
            for item in val:
                newrow = dict(row)
                newrow[key] = item
                data.append(newrow)
        else:
            newrow = dict(row)
            newrow[key] = val
            data.append(newrow)
    newdata = []
    for row in data:
        newrow = dict(row)
        for key, val in row.items():
            if  val:
                newrow[key] = val
            else:
                if  type(indict[key]) is types.ListType:
                    for value in indict[key]:
                        newrow = dict(row)
                        newrow[key] = value
                        values = [val for val in newrow.values() if val]
                        if  not newdata.count(newrow) \
                            and len(values) == len(newrow.keys()):
                            newdata.append(newrow)
                else:
                    newrow[key] = indict[key]
        values = [val for val in newrow.values() if val]
        if  not newdata.count(newrow) and len(values) == len(newrow.keys()):
            newdata.append(newrow)
    return newdata

def getarg(kwargs, key, default):
    """
    retrieve value from input dict for given key and default
    """
    arg = default
    if  kwargs.has_key(key):
        arg = kwargs[key]
        if  type(default) is types.IntType:
            arg = int(arg)
    return arg

def sitename(site):
    """
    Based on pattern determine what site name is provided, e.g. 
    CMS name or SAM name, etc.
    """
    patlist = [
               ('phedex', re.compile('^T[0-9]_[A-Z]+(_)[A-Z]+')),#T2_UK_NO
               ('cms', re.compile('^T[0-9]_')), # T2_UK
               ('se',  re.compile('[a-z]+(\.)[a-z]+(\.)')), # a.b.c
               ('site', re.compile('^[A-Z]+')),
              ]
    for name, pat in patlist:
        if  pat.match(site):
            return name

def add2dict(idict, key, value):
    """
    Add value as a list to the dictionary for given key.
    """
    if  idict.has_key(key):
        val = idict[key]
        if  type(val) is not types.ListType:
            val = [val]
        if  type(value) is types.ListType:
            idict[key] = val + value
        else:
            idict[key].append(value)
    else:
        idict[key] = value
#    idict.setdefault(key, []).append(value)

def map_validator(smap):
    """
    Validator for data-serivce maps. The data-service map should be
    provided in a form
    map = {
            'api' : {
                'keys' : ['key1', 'key2'],
                'params' : {'param':1, 'param2':2}
            }
    }
    """
    msg = 'Fail to validate data-service map %s' % smap
    if  type(smap.keys()) is not types.ListType:
        raise Exception(msg)
    for item in smap.values():
        if  type(item) is not types.DictType:
            raise Exception(msg)
        if  item.keys() != ['keys', 'params']:
            raise Exception(msg)
        if  type(item['keys']) is not types.ListType:
            raise Exception(msg)
        if  type(item['params']) is not types.DictType:
            raise Exception(msg)
