#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
General set of useful utilities used by DAS
"""

__revision__ = "$Id: utils.py,v 1.53 2010/01/04 19:01:51 valya Exp $"
__version__ = "$Revision: 1.53 $"
__author__ = "Valentin Kuznetsov"

import os
import re
import DAS.utils.jsonwrapper as json
try:
    # with python 2.5
    import hashlib
except:
    # prior python 2.5
    import md5
import time
import types
import traceback
from itertools import groupby
from pymongo.objectid import ObjectId
import xml.etree.cElementTree as ET

def adjust_value(value):
    """
    Change null value to None.
    """
    pat_float   = re.compile(r'(^\d+\.\d*$|^\d*\.{1,1}\d+$)')
    pat_integer = re.compile(r'(^[0-9]$|^[0-9][0-9]*$)')
    if  type(value) is types.StringType:
        if  value == 'null' or value == '(null)':
            return None
        elif pat_float.match(value):
            return float(value)
        elif pat_integer.match(value):
            return int(value)
        else:
            return value
    else:
        return value

class dict_of_none (dict):
    """Define new dict type whose missing keys always assigned to None"""
    def __missing__ (self, key):
        """Assign missing key to None"""
        return None

def dict_value(idict, prim_key):
    """
    Find value in given dictionary for given primary key. The primary
    key can be composed one, e.g. a.b. If found value is a list type
    take first element of the list and look-up over there the primary key.
    """
    try:
        if  prim_key.find('.') != -1:
            value = idict
            for key in prim_key.split('.'):
                if  type(value) is types.ListType:
                    value = value[0][key]
                    break
                else:
                    value = value[key]
        else:
            value = idict[prim_key]
        return value
    except:
        msg  = 'Unable to look-up key=%s\n' % prim_key
        msg += 'dict=%s' % str(idict)
        raise Exception(msg)

#    if  idict.has_key(prim_key):
#        return idict[prim_key]
#    try:
#        value = dict(idict)
#        for key in prim_key.split('.'):
#            if  type(value) is types.ListType:
#                value = value[0][key]
#                break
#            else:
#                value = value[key]
#    except:
#        msg  = 'Unable to look-up key=%s\n' % prim_key
#        msg += 'dict=%s' % str(dict)
#        raise Exception(msg)

def merge_dict(dict1, dict2):
    """
    Merge content of two dictionaries w/ default list value for keys.
    Original code was:
    merged_dict = {}
    for dictionary in [dict1, dict2]:
        for key, value in dictionary.items():
            merged_dict.setdefault(key,[]).append(value) 
    return merged_dict
    where I changed append(value) on merging lists or adding new
    value into the list based on value type.
    """
#    merged_dict = {}
#    for dictionary in (dict1, dict2):
#        for key, value in dictionary.items():
#            dict_value = merged_dict.setdefault(key, [])
#            if  type(value) is types.ListType:
#                dict_value += value
#            else:
#                dict_value += [value]
#            merged_dict[key] = dict_value
#    return merged_dict

#    merged_dict = dict(dict1)
#    for key, value in dict2.items():
#        if  merged_dict.has_key(key):
#            val = merged_dict[key]
#            if  type(val) is types.ListType:
#                if  type(value) is types.ListType:
#                    merged_dict[key] = val + value
#                else:
#                    val.append(value)
#                    merged_dict[key] = val
#            else:
#                if  type(value) is types.ListType:
#                    merged_dict[key] = [val] + value
#                else:
#                    merged_dict[key] = [val] + [value]
#        else:
#            merged_dict[key] = [value]
#    return merged_dict
    for key, value in dict2.items():
        if  dict1.has_key(key):
            val = dict1[key]
            if  type(val) is types.ListType:
                if  type(value) is types.ListType:
                    dict1[key] = val + value
                else:
                    val.append(value)
                    dict1[key] = val
            else:
                if  type(value) is types.ListType:
                    dict1[key] = [val] + value
                else:
                    dict1[key] = [val] + [value]
        else:
            dict1[key] = [value]

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

def dasheader(system, query, api, url, args, ctime, expire, ver):
    """
    Return DAS header (dict) wrt DAS specifications, see
    https://twiki.cern.ch/twiki/bin/view/CMS/DMWMDataAggregationService
    #DAS_data_service_compliance
    """
    timestamp = time.time()
    if  type(query) is types.DictType:
        query = json.dumps(query)
    dasdict = dict(system=[system], timestamp=timestamp,
#                url=url, ctime=ctime, query=[str(query)],
#                params={api:args}, version=ver,
                url=url, ctime=ctime, qhash=[genkey(query)], version=ver,
                expire=timestamp+expire, api=[api])
    return dict(das=dasdict)

def genkey(query):
    """
    Generate a new key-hash for a given query. We use md5 hash for the
    query and key is just hex representation of this hash.
    """
    try:
        keyhash = hashlib.md5()
    except:
        # prior python 2.5
        keyhash = md5.new()
    if  type(query) is types.DictType:
        if  query.has_key('spec') and query['spec'].has_key('_id'):
            val = query['spec']['_id']
            if  isinstance(val, ObjectId):
                val = str(val)
                query['spec']['_id'] = val
        query = json.dumps(query)
    keyhash.update(query)
    return keyhash.hexdigest()

def gen2list(results):
    """
    Convert generator to a list discarding duplicates
    """
    reslist = [name for name, group in groupby(results)]
    return reslist

# Does not work with list of two identical items
#def uniqify(ilist):
#    """
#    Make all entries in a list to be unique.
#    http://pyfaq.infogami.com/how-do-you-remove-duplicates-from-a-list
#    """
#    ilist.sort()
#    last = ilist[-1]
#    for i in range(len(ilist)-2, -1, -1):
#        if last==ilist[i]: del ilist[i]
#        else: last=ilist[i]

def dump(ilist, idx=0):
    """
    Print items in provided generator
    """
#    if  type(ilist) is types.GeneratorType:
#        reslist = [i for i in ilist]
#    elif type(ilist) is not types.ListType:
#        reslist = [ilist]
#    else:
#        reslist = ilist
#    if  not reslist:
#        print "No results found"
#        return
#    reslist.sort()
#    reslist = [k for k, g in groupby(reslist)]
    if  type(ilist) is types.GeneratorType:
        reslist = ilist
    elif type(ilist) is not types.ListType:
        reslist = [ilist]
    else:
        reslist = ilist
    if  not reslist:
        print "No results found"
        return
    idx = 0
    for row in reslist:
        print "id : %s" % idx
        if  type(row) is types.DictType:
            print json.dumps(row)
        else:
            print row
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
    if  type(results) is types.GeneratorType:
        resdict['results'] = [res for res in results]
    else:
        resdict['results'] = results
    return resdict

# TODO: I can use genresults generator implementation only if
# I'll solve the problem with das_core.py:find_cond_dict since it's used
# to read results from first data-service and pass found relative keys
# to other data-service. At this point iteration from genresults will
# cause empty generator at cartesian_product later on.
def genresults_gen(system, results, collect_list):
    """
    Generator of results for given system based on provided dict 
    of 'results' and final set 'collect_list'.
    The output rowdict in a form {'system':system_name, 'key':value}
    """
    rdict = {}
    rdict['system'] = system
    for key in collect_list:
        rdict[key] = ""

    for res in results:
        rowdict = dict(rdict)
        for idx in range(0, len(collect_list)):
            key = collect_list[idx]
            if  res.has_key(key):
                rowdict[key] = res[key]
        yield rowdict

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
            val = idict[key]
            if  type(val) is types.ListType:
                idict[key].append(value)
            else:
                idict[key] = [val, value]
#            idict[key].append(value)
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

def get_key_cert():
    """
    Get user key/certificate
    """
    key  = None
    cert = None
    globus_key  = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
    globus_cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
    if  os.path.isfile(globus_key):
        key  = globus_key
    if  os.path.isfile(globus_cert):
        cert  = globus_cert

    # First presendence to HOST Certificate, RARE
    if  os.environ.has_key('X509_HOST_CERT'):
        cert = os.environ['X509_HOST_CERT']
        key  = os.environ['X509_HOST_KEY']

    # Second preference to User Proxy, very common
    elif os.environ.has_key('X509_USER_PROXY'):
        cert = os.environ['X509_USER_PROXY']
        key  = cert

    # Third preference to User Cert/Proxy combinition
    elif os.environ.has_key('X509_USER_CERT'):
        cert = os.environ['X509_USER_CERT']
        key  = os.environ['X509_USER_KEY']

    # Worst case, look for cert at default location /tmp/x509up_u$uid
    elif not key or not cert:
        uid  = os.getuid()
        cert = '/tmp/x509up_u'+str(uid)
        key  = cert

    if  not os.path.exists(cert):
        raise Exception("Certificate PEM file %s not found" % key)
    if  not os.path.exists(key):
        raise Exception("Key PEM file %s not found" % key)

    return key, cert

def gen_key_tuples(data, key):
    """
    Generator function to yield (value, id) pairs for provided key
    """
    for idx in range(0, len(data)):
        row = data[idx]
        tup = (row[key], idx)
        yield tup

def sort_data(data, key, direction='asc'):
    """
    Generator function to yield rows sorted by provided key in a row.
    """
    tup_list = [i for i in gen_key_tuples(data, key)]
    tup_list.sort()
    if  direction == 'desc':
        tup_list.reverse()
    for pair in tup_list:
        yield data[pair[1]]

class dotdict(dict):
    """
    Access python dictionaries via dot notations, code taken from
    http://parand.com/say/index.php/2008/10/24/python-dot-notation-dictionary-access/
    """
    def __getattr__(self, attr):
        return self.get(attr, None)
    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__

def trace(source):
    """Trace a generator by printing items received"""
    for item in source:
        print item
        yield item

def access(data, elem):
    """
    Access elements of the dict (data). The element can be supplied in dotted form, e.g.
    site.admin.title and code will search for {'site':[{'admin':[{'title'...}]}]}
    """
    if  elem.find('.') == -1:
        key = elem
        if  type(data) is types.DictType:
            if  data.has_key(key):
                yield data[key]
        elif type(data) is types.ListType or type(data) is types.GeneratorType:
            for item in data:
                if  item.has_key(key):
                    yield item[key]
    else:
        keylist = elem.split('.')
        key  = keylist[0]
        rkey = '.'.join(keylist[1:])
        if  data.has_key(key):
            res = data[key]
            if  type(res) is types.DictType:
                result = access(res, rkey)
                if  type(result) is types.GeneratorType:
                    for item in result:
                        yield item
            elif type(res) is types.ListType or types(res) is types.GeneratorType:
                for row in res:
                    result = access(row, rkey)
                    if  type(result) is types.GeneratorType:
                        for item in result:
                            yield item

def dict_helper(idict, notations):
    """
    Create new dict for provided notations/dict. Perform implicit conversion
    of data types, e.g. if we got '123', convert it to integer. The conversion
    is done based on adjust_value function.
    """
    try:
        from DAS.extensions.das_speed_utils import _dict_handler
        return _dict_handler(idict, notations)
    except:
        child_dict = {}
        for kkk, vvv in idict.items():
            child_dict[notations.get(kkk, kkk)] = adjust_value(vvv)
        return child_dict

def xml_parser(notations, source, tag, add=None):
    """
    XML parser based on ElementTree module. To reduce memory footprint for
    large XML documents we use iterparse method to walk through provided
    source descriptor (a .read()/close()-supporting file-like object 
    containig XML source).
    """
    sup     = {}
    context = ET.iterparse(source, events=("start", "end"))
    root    = None
    for item in context:
        event, elem = item
        if  event == "start" and root is None:
            root = elem # the first element is root
        row = {}
        if  add and not sup:
            if  add.find("_") != -1:
                atag, attr = add.split("_")
                if  elem.tag == atag and elem.attrib.has_key(attr):
                    sup[add] = elem.attrib[attr]
            else:
                if  elem.tag == add:
                    sup[add] = elem.attrib
        if  elem.tag != tag or event == 'end':
            elem.clear()
            continue
        key = notations.get(elem.tag, elem.tag)
        row[key] = dict_helper(elem.attrib, notations)
        row.update(sup)
        for child in elem.getchildren():
            child_key  = notations.get(child.tag, child.tag)
            child_dict = dict_helper(child.attrib, notations)

            if  row[key].has_key(child_key):
                val = row[key][child_key]
                if  type(val) is types.ListType:
                    val.append(child_dict)
                    row[key][child_key] = val
                else:
                    row[key][child_key] = [val] + [child_dict]
            else:
                row[key][child_key] = child_dict
            child.clear()
        elem.clear()
        yield row
    root.clear()
    source.close()

def json_parser(source):
    """
    JSON parser based on json module. It accepts either source
    descriptor with .read()-supported file-like object or
    data as a string object.
    """
    if  type(source) is types.InstanceType: # got data descriptor
        try:
            jsondict = json.load(source)
        except:
            traceback.print_exc()
        source.close()
    else:
        data = source
        # to prevent unicode/ascii errors like
        # UnicodeDecodeError: 'utf8' codec can't decode byte 0xbf in position
        if  type(data) is types.StringType:
            data = unicode(data, errors='ignore')
        res = data.replace('null', '\"null\"')
        try:
            jsondict = json.loads(res)
        except:
            jsondict = eval(res)
            pass
    yield jsondict

def row2das(mapper, system, api, row):
    """Transform keys of row into DAS notations, e.g. bytes to size"""
    if  type(row) is not types.DictType:
        return
    for key, val in row.items():
        newkey = mapper(system, key, api)
        if  newkey != key:
            row[newkey] = row.pop(key)
        if  type(val) is types.DictType:
            row2das(mapper, system, api, val)
        elif type(val) is types.ListType:
            for item in val:
                if  type(item) is types.DictType:
                    row2das(mapper, system, api, item)

def aggregator(results):
    """
    DAS aggregator which iterate over all records in results set and
    perform aggregation of records on the primary_key of the record.
    """
    record = results.next()
    record['_id'] = str(record['_id']) # _id is ObjectId, convert it to string
    prim_key = record['primary_key']
    del record['primary_key']
    del record['das']
    update = 0
    for row in results:
        row['_id'] = str(row['_id'])
        del row['primary_key']
        del row['das']
        val1 = dict_value(record, prim_key)
        val2 = dict_value(row, prim_key)
        if  val1 == val2:
            merge_dict(record, row)
            update = 1
        else:
            yield record
            record = dict(row)
            update = 0
    if  update: # check if we did update for last row
        yield record
