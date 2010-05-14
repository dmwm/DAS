#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
General set of useful utilities used by DAS
"""

__revision__ = "$Id: utils.py,v 1.86 2010/04/30 16:33:40 valya Exp $"
__version__ = "$Revision: 1.86 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os
import re
import time
import types
import hashlib
import plistlib
import traceback
import xml.etree.cElementTree as ET
from   itertools import groupby
from   pymongo.objectid import ObjectId

# DAS modules
from   DAS.utils.regex import float_number_pattern, int_number_pattern
from   DAS.utils.regex import phedex_tier_pattern, cms_tier_pattern
from   DAS.utils.regex import se_pattern, site_pattern
import DAS.utils.jsonwrapper as json

def expire_timestamp(expire):
    """Return expire timestamp"""
    timestamp = time.time()
    # use Jan 1st, 2010 as a seed to check expire date
    # prior 2010 DAS was not released in production
    tup = (2010, 1, 1, 0, 0, 0, 0, 1, -1)
    if  type(expire) is types.IntType or expire < time.mktime(tup):
        expire = timestamp + expire
    return expire

def yield_rows(*args):
    """
    Yield rows from provided input.
    """
    for input in args:
        if  type(input) is types.GeneratorType:
            for row in input:
                yield row
        elif type(input) is types.ListType:
            for row in input:
                yield row
        else:
            yield input

def adjust_value(value):
    """
    Change null value to None.
    """
    pat_float   = float_number_pattern
    pat_integer = int_number_pattern
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

class dotdict(dict):
    """
    Access python dictionaries via dot notations, original code taken from
    http://parand.com/say/index.php/2008/10/24/python-dot-notation-dictionary-access/
    Class has been extended with helper method to use compound keys, e.g. a.b.c.
    All extended method follow standard python dictionary naming conventions,
    but has extra underscore in front of them to allow dict methods. A non
    overlapping methods do not have extra underscore.
    """
    def __getattr__(self, attr):
        obj = self.get(attr, {})
        if  type(obj) is types.DictType:
            return dotdict(obj)
        return obj
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def _delete(self, ckey):
        """
        Delete provided compound key from dot dictionary
        """
        obj  = self
        keys = ckey.split('.')
        for key in keys:
            if  key == keys[-1]:
                if  obj.has_key(key):
                    del obj[key]
            else:
                obj = obj[key]
        
    def _get(self, ckey):
        """
        Get value for provided compound key.
        """
        obj  = None
        keys = ckey.split('.')
        for key in keys:
            if  key == keys[0]:
                obj = self.get(key, None)
            else:
                obj = getattr(obj, key)
            if  not obj and obj != 0:
                return None
            if  type(obj) is types.DictType:
                obj = dotdict(obj)
            elif type(obj) is types.ListType:
                obj = obj[0]
                if  type(obj) is types.DictType:
                    obj = dotdict(obj)
        return obj
        
    def _set(self, ikey, value):
        """
        Set value for provided compound key.
        """
        obj  = self
        keys = ikey.split('.')
        for idx in range(0, len(keys)):
            key = keys[idx]
            if  not obj.has_key(key):
                ckey = '.'.join(keys[idx:])
                nkey, nval = convert_dot_notation(ckey, value)
                obj[nkey] = nval
                return
            if  key != keys[-1]:
                obj  = obj[key]
        obj[key] = value

def dict_type(obj):
    """
    Return if provided object is type of dict or instance of dotdict class
    """
    return type(obj) is types.DictType or isinstance(obj, dotdict)

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

    .. doctest::

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

def genkey(query):
    """
    Generate a new key-hash for a given query. We use md5 hash for the
    query and key is just hex representation of this hash.
    """
    keyhash = hashlib.md5()
    if  type(query) is types.DictType:
#        if  query.has_key('spec') and query['spec'].has_key('_id'):
#            val = query['spec']['_id']
#            if  isinstance(val, ObjectId):
#                val = str(val)
#                query['spec']['_id'] = val
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
    whose elements are dicts with identical keys, e.g.::

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
    keys (rel_keys). Provided sets should be in a form of ::

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
    The output rowdict in a form::

        {'system':system_name, 'key':value}
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
    The output rowdict in a form::

        {'system':system_name, 'key':value}
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
    transform input dictionary into list of dictionaries, e.g.::

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
               ('phedex', phedex_tier_pattern),#T2_UK_NO
               ('cms', cms_tier_pattern), # T2_UK
               ('se',  se_pattern), # a.b.c
               ('site', site_pattern),
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

    .. doctest::

        map = {
                'api' : {
                    'keys' : ['key1', 'key2'],
                    'params' : {'param':1, 'param2':2},
                    'url' : 'service_url',
                    'expire' : 'expiration timestamp',
                    'format' : 'data_format',
                }
        }
    """
    msg = 'Fail to validate data-service map %s' % smap
    if  type(smap.keys()) is not types.ListType:
        raise Exception(msg)
    possible_keys = ['api', 'keys', 'params', 'url', 'expire', 
                        'format', 'wild_card']
    possible_keys.sort()
    for item in smap.values():
        if  type(item) is not types.DictType:
            raise Exception(msg)
        keys = item.keys()
        keys.sort()
        if  len(keys) == len(possible_keys):
            if  keys != possible_keys:
                raise Exception(msg)
        elif len(keys) == len(possible_keys)-1:
            pkeys = list(possible_keys)
            pkeys.remove('api')
            if  keys != pkeys:
                raise Exception(msg)
        else:
            raise Exception(msg)
        if  type(item['keys']) is not types.ListType:
            raise Exception(msg)
        if  type(item['params']) is not types.DictType:
            raise Exception(msg)

def permutations(iterable, r=None):
    """
    permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    permutations(range(3)) --> 012 021 102 120 201 210
    """
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

def trace(source):
    """Trace a generator by printing items received"""
    for item in source:
        print item
        yield item

def splitter(record, key):
    """
    Split input record into a stream of records based on provided key.
    A key can be supplied in a dotted form, e.g. block.replica.name
    """
    yield record

def access(data, elem):
    """
    Access elements of the dict (data). The element can be supplied in dotted form, e.g.
    site.admin.title and code will search for::

        {'site':[{'admin':[{'title'...}]}]}
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

def delete_elem(data, elem):
    """
    Delete provided elem (can be in form of compound key, e.g. block.name)
    from data row.
    """
    keys = elem.split(".")
    val  = data
    try:
        for key in keys[:-1]:
            val = val[key]
        if  val.has_key(keys[-1]):
            del val[keys[-1]]
    except:
        pass

def translate(notations, api, rec):
    """
    Translate given row to DAS notations according to provided notations
    and api. Each entry in notations list is a form of

    .. doctest::

        {"notation":"site.resource.name", "map":"site.name", "api":""}
    """
    for row in notations:
        count    = 0
        notation = row['notation']
        dasmap   = row['map']
        api2use  = row['api']
        if  not api2use or api2use == api:
            record = dict(rec)
            rows = access(rec, notation)
            keys = dasmap.split(".")
            keys.reverse()
            for val in rows:
                item, newval = convert_dot_notation(dasmap, val)
                recval = record[item]
                if  type(recval) is types.DictType:
                    recval.update(newval)
                else: 
                    record[item] = newval
                count += 1
                delete_elem(record, notation)
            yield record
        if  not count:
            yield rec

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

def xml_parser(source, prim_key, tags=[]):
    """
    XML parser based on ElementTree module. To reduce memory footprint for
    large XML documents we use iterparse method to walk through provided
    source descriptor (a .read()/close()-supporting file-like object 
    containig XML source).

    The provided prim_key defines a tag to capture, while supplementary
    *tags* list defines additional tags which can be added to outgoing
    result. For instance, file object shipped from PhEDEx is enclosed
    into block one, so we want to capture block.name together with
    file object.
    """
    notations = {}
    sup       = {}
    context   = ET.iterparse(source, events=("start", "end"))
    root      = None
    for item in context:
        event, elem = item
        if  event == "start" and root is None:
            root = elem # the first element is root
        row = {}
        if  tags and not sup:
            for tag in tags:
                if  tag.find(".") != -1:
                    atag, attr = tag.split(".")
                    if  elem.tag == atag and elem.attrib.has_key(attr):
                        att_value = elem.attrib[attr]
                        if  type(att_value) is types.DictType:
                            att_value = dict_helper(elem.attrib[attr], notations)
                        if  type(att_value) is types.StringType:
                            att_value = adjust_value(att_value)
                        sup[atag] = {attr:att_value}
                else:
                    if  elem.tag == tag:
                        sup[tag] = elem.attrib
        key = elem.tag
        if  key != prim_key or event == 'end':
            elem.clear()
            continue
        row[key] = dict_helper(elem.attrib, notations)
        row[key].update(sup)
        get_children(elem, row, key, notations)
#        for child in elem.getchildren():
#            child_key  = child.tag
#            child_data = child.attrib
#            if  not child_data:
#                child_dict = adjust_value(child.text)
#            else:
#                child_dict = dict_helper(child_data, notations)

#            if  row[key].has_key(child_key):
#                val = row[key][child_key]
#                if  type(val) is types.ListType:
#                    val.append(child_dict)
#                    row[key][child_key] = val
#                else:
#                    row[key][child_key] = [val] + [child_dict]
#            else:
#                row[key][child_key] = child_dict
#            child.clear()
#        elem.clear()
        yield row
    root.clear()
    source.close()

def get_children(elem, row, key, notations):
    """
    xml_parser helper function. It gets recursively information about
    children for given element tag. Information is stored into provided
    row for given key. The change of notations can be applied during
    parsing step by using provided notations dictionary.
    """
    for child in elem.getchildren():
        child_key  = child.tag
        child_data = child.attrib
        if  not child_data:
            child_dict = adjust_value(child.text)
        else:
            child_dict = dict_helper(child_data, notations)

        if  type(row[key]) is types.DictType and row[key].has_key(child_key):
            val = row[key][child_key]
            if  type(val) is types.ListType:
                val.append(child_dict)
                row[key][child_key] = val
            else:
                row[key][child_key] = [val] + [child_dict]
        else:
            if  child.getchildren(): # we got grand-children
                if  child_dict:
                    row[key][child_key] = child_dict
                else:
                    row[key][child_key] = {}
                if  type(child_dict) is types.DictType:
                    newdict = {child_key: child_dict}
                else:
                    newdict = {child_key: {}}
                get_children(child, newdict, child_key, notations) 
                row[key][child_key] = newdict[child_key]
            else:
                if  type(row[key]) is not types.DictType:
                    row[key] = {}
                row[key][child_key] = child_dict
        child.clear()
    elem.clear()

def json_parser(source):
    """
    JSON parser based on json module. It accepts either source
    descriptor with .read()-supported file-like object or
    data as a string object.
    """
    if  type(source) is types.InstanceType or\
        type(source) is types.FileType: # got data descriptor
        try:
            jsondict = json.load(source)
        except:
            traceback.print_exc()
            source.close()
            raise
        source.close()
    else:
        data = source
        # to prevent unicode/ascii errors like
        # UnicodeDecodeError: 'utf8' codec can't decode byte 0xbf in position
        if  type(data) is types.StringType:
            data = unicode(data, errors='ignore')
            res  = data.replace('null', '\"null\"')
        else:
            res  = data
        try:
            jsondict = json.loads(res)
        except:
            jsondict = eval(res)
            pass
    yield jsondict

def plist_parser(source):
    """
    Apple plist parser based on plistlib. It accepts either source
    descriptor with .read()-supported file-like object or
    data as a string object.
    """
    if  type(source) is types.InstanceType or\
        type(source) is types.FileType: # got data descriptor
        try:
            data = source.read()
        except:
            traceback.print_exc()
            source.close()
            raise
        source.close()
    else:
        data = source
    yield plistlib.readPlistFromString(data)

def convert_dot_notation(key, val):
    """
    In DAS key can be presented in dot notation, e.g. block.name.
    Take provided key/value pair and convert it into dict if it
    is required.
    """
    split_list = key.split('.')
    if  len(split_list) == 1: # no dot notation found
        return key, val
    split_list.reverse()
    newval = val
    for item in split_list:
        if  item == split_list[-1]:
            return item, newval
        newval = {item:newval}
    return item, newval
    
def row2das(mapper, system, api, row):
    """
    Transform keys of row into DAS notations, e.g. bytes to size
    If compound key found, e.g. block.replica.name, it will
    be converted into appropriate dict, e.g. {'block':{'replica':{'name':val}}
    """
    if  type(row) is not types.DictType:
        return
    for key, val in row.items():
        newkey = mapper(system, key, api)
        if  newkey != key:
            row.pop(key)
            nkey, nval = convert_dot_notation(newkey, val)
            row.update({nkey:nval})
#            row[nkey] = nval
        if  type(val) is types.DictType:
            row2das(mapper, system, api, val)
        elif type(val) is types.ListType:
            for item in val:
                if  type(item) is types.DictType:
                    row2das(mapper, system, api, item)

def aggregator(results, expire):
    """
    High-level API, DAS aggregator function.
    """
    for rec in aggregator_helper(results, expire):
        das_id = rec.pop('das_id')
        if  type(das_id) is types.ListType:
            rec['das_id'] = list(set(das_id))
        else:
            rec['das_id'] = [das_id]
        _ids = rec.pop('_id')
        if  type(_ids) is not types.ListType:
            _ids = [_ids]
        rec['cache_id'] = list(set(_ids))
        yield rec

def aggregator_helper(results, expire):
    """
    DAS aggregator helper which iterates over all records in results set and
    perform aggregation of records on the primary_key of the record.
    """
    record = results.next()
    prim_key = record['das']['primary_key']
    record.pop('das')
    update = 1
    for row in results:
        row_prim_key = row['das']['primary_key']
        row.pop('das')
        if  row_prim_key != prim_key:
            record.update({'das':{'expire':expire, 'primary_key':prim_key}})
            yield record
            prim_key = row_prim_key
            record = row
            continue
        try:
            val1 = dict_value(record, prim_key)
        except:
            record.update({'das':{'expire':expire, 'primary_key':prim_key}})
            yield record
            record = dict(row)
            update = 0
            continue
        try:
            val2 = dict_value(row, prim_key)
        except:
            row.update({'das':{'expire':expire, 'primary_key':prim_key}})
            yield row
            record = dict(row)
            update = 0
            continue
        if  val1 == val2:
            merge_dict(record, row)
            update = 1
        else:
            record.update({'das':{'expire':expire, 'primary_key':prim_key}})
            yield record
            record = dict(row)
            update = 0
    if  update: # check if we did update for last row
        record.update({'das':{'expire':expire, 'primary_key':prim_key}})
        yield record
    else:
        row.update({'das':{'expire':expire, 'primary_key':row_prim_key}})
        yield row

def extract_http_error(err):
    """
    Upon urllib failure the data-service can send HTTPError message.
    It can be in a form of JSON, etc. This function attempts to extract
    such message. If it fails it just str(err) and return.
    """
    msg  = str(err)
    try:
        err = json.loads(err)
        if  err.has_key('message'):
            value = err['message']
            if  type(value) is types.DictType:
                msg = ''
                for key, val in value.items():
                    msg += '%s: %s. ' % (key, val)
            else:
                msg = str(value)
    except:
        pass
    return msg

def make_headers(data_format):
    """
    Create HTTP header based on input parameters
    """
    headers = {}
    if  data_format.lower() == 'json':
        headers.update({'Accept':'text/json;application/json'})
    elif data_format.lower() == 'xml':
        headers.update({'Accept':'text/xml;application/xml'})
    return headers

def filter(rows, filters):
    """
    Filter given rows with provided set of filters.
    """
    for row in rows:
        ddict = dotdict(row)
        flist = [(f,ddict._get(f)) for f in filters]
        for iter in flist:
            yield iter
