#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
General set of useful utilities used by DAS
"""

__revision__ = "$Id: utils.py,v 1.13 2009/05/13 14:54:08 valya Exp $"
__version__ = "$Revision: 1.13 $"
__author__ = "Valentin Kuznetsov"

import re
import md5
import time
import types
import traceback
from itertools import groupby

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
    reslist = [name for name, group in groupby(results)]
    return reslist

def dump(reslist, limit=None, selkeys=None):
    """
    Print items in provided generator
    """
    if  not reslist:
        print "No results found"
        return
    if  type(reslist) is not types.ListType:
        reslist = [reslist]
    try:
        if  selkeys:
            keys = [i for i in reslist[0].keys() if selkeys.count(i)]
        else:
            keys = reslist[0].keys()
    except:
        traceback.print_exc()
        print "dump results fail, reslist", reslist
        raise Exception('Fail to dump output result list')
    keys.sort()
    try:
        keys.remove('id')
        keys = ['id']+keys
    except:
        pass
    keysize = 0
    for key in keys:
        if  len(key) > keysize:
            keysize = len(key)

    idx  = 0
    for res in reslist:
        if  limit and idx >= limit:
            break
        padding = " "*(keysize-len('id'))
        print "id%s : %s" % (padding, idx)
        for key in keys:
            padding = " "*(keysize-len(key))
            print "%s%s : %s" % (key, padding, res[key])
        print
        idx += 1

def cartesian_product(ilist1, ilist2):
    """
    Create cartesian product between two provided lists/generators
    whose elements are dicts with identical keys, e.g.
    {'system':system_name, 'key':value'}
    """
    if  type(ilist1) is types.GeneratorType:
        list1 = [i for i in ilist1]
    else:
        list1 = ilist1
    if  type(ilist2) is types.GeneratorType:
        list2 = [i for i in ilist2]
    else:
        list2 = ilist2
    # find which list is largest
    if  len(list1) >= len(list2):
        master_list = list1
        slave_list  = list2
    else:
        master_list = list2
        slave_list  = list1

    if  not slave_list:
        for item in master_list:
            yield item
        return

    # find relation keys between two dicts (rows) in lists
    row1  = master_list[0]
    keys1 = [k for k, v in row1.items() if v and k != 'system']
    row2  = slave_list[0]
    keys2 = [k for k, v in row2.items() if v and k != 'system']
    rel_keys = list( set(keys1) & set(keys2) )
    ins_keys = set(keys2) - set(rel_keys)
    
    # loop over largest list and insert
    master_len = len(master_list)
    for idx in range(0, master_len):
        idict = master_list[idx]
        update = 0
        for jdx in range(0, len(slave_list)):
            jdict = slave_list[jdx]
            found = 0
            for key in rel_keys:
                if  idict[key] == jdict[key]:
                    found += 1
            if  found == len(rel_keys):
                if  not jdx:
                    update = 1
                    for k in ins_keys:
                        idict[k] = jdict[k]
                    if  idict.has_key('system') and \
                        idict['system'].find(jdict['system']) == -1:
                        idict['system'] = '%s+%s' % \
                            (idict['system'], jdict['system'])
                    yield dict(idict)
                else:
                    row = dict(idict)
                    for k in ins_keys:
                        row[k] = jdict[k]
                    if  row.has_key('system') and \
                        row['system'].find(jdict['system']) == -1:
                        row['system'] = '%s+%s' % \
                            (row['system'], jdict['system'])
                    yield row

def cartesian_product_via_list(master_set, slave_set, rel_keys=None):
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

def timestamp():
    return int(str(time.time()).split('.')[0])
        
def results2couch(query, results, expire=600):
    """
    Modify results and add to each row dict the query and timestamp
    to be used by couch db.
    """
    resdict = {}
    resdict['query'] = query
    resdict['hash'] = genkey(query)
    tstamp = timestamp()
    resdict['timestamp'] = tstamp
    resdict['expire'] = tstamp + expire
    resdict['results'] = results
    return resdict

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
    # TODO: should be replaced by ANTRL parser

    parts = query.split(' where ')
    selkeys = parts[0].replace('find ','').split(',')
    params = {}
    if  len(parts) > 1:
        cond_exp = parts[1]
        for cond in cond_exp.split(' and '):
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
    foundlist = 0
    row  = {}
    for k, v in indict.items():
        row[k] = None
        if  type(v) is types.ListType:
            if  foundlist and foundlist != len(v):
                raise Exception('Input dict contains multi-sized lists')
            foundlist = len(v)

    olist = []
    if  foundlist:
        for i in range(0, foundlist):
            newrow = dict(row)
            for k, v in indict.items():
                if  type(v) is types.ListType:
                    newrow[k] = v[i]
                else:
                    newrow[k] = v
            olist.append(newrow)
    return olist

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
        keys = item.keys()
        keys.sort()
        if  len(keys) == 3:
            if  keys != ['api', 'keys', 'params']:
                raise Exception(msg)
        elif len(keys) == 2:
            if  keys != ['keys', 'params']:
                raise Exception(msg)
        else:
            raise Exception(msg)
        if  type(item['keys']) is not types.ListType:
            raise Exception(msg)
        if  type(item['params']) is not types.DictType:
            raise Exception(msg)

def permutations(iterable, r=None):
    # permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    # permutations(range(3)) --> 012 021 102 120 201 210
    pool = tuple(iterable)
    n = len(pool)
    r = n if r is None else r
    if r > n:
        return
    indices = range(n)
    cycles = range(n, n-r, -1)
    yield tuple(pool[i] for i in indices[:r])
    while n:
        for i in reversed(range(r)):
            cycles[i] -= 1
            if cycles[i] == 0:
                indices[i:] = indices[i+1:] + indices[i:i+1]
                cycles[i] = n - i
            else:
                j = cycles[i]
                indices[i], indices[-j] = indices[-j], indices[i]
                yield tuple(pool[i] for i in indices[:r])
                break
        else:
            return

def oneway_permutations(ilist):
    """
    Uni-directional permutation function
    Example: ilist=[a,b,c] and this function returns
    (a,b), (a,c), (b,c)
    """
    for idx in range(0, len(ilist)):
        key = ilist[idx]
        try:
            tmp = list(ilist[idx+1:])
            for i in tmp:
                yield (key, i)
        except:
            pass
def unique_list(ilist):
    """
    Return sorted unique list out of provided one.
    """
    ilist.sort()
#    return [k for k, g in groupby(ilist)]
    tmplist = [k for k, g in groupby(ilist)]
    tmplist.sort()
    return tmplist

def izip_longest(*args, **kwds):
    """
    izip_longest('ABCD', 'xy', fillvalue='-') --> Ax By C- D-
    introduced in python 2.6
    """
    fillvalue = kwds.get('fillvalue')
    def sentinel(counter = ([fillvalue]*(len(args)-1)).pop):
        yield counter()         # yields the fillvalue, or raises IndexError
    from itertools import repeat, chain, izip
    fillers = repeat(fillvalue)
    iters = [chain(it, sentinel(), fillers) for it in args]
    try:
        for tup in izip(*iters):
            yield tup
    except IndexError:
        pass

