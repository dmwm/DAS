#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0703

"""
General set of useful utilities used by DAS
"""

__author__ = "Valentin Kuznetsov"

# system modules
import os
import re
import sys
import time
from   types import GeneratorType, InstanceType
import hashlib
import plistlib
import calendar
import datetime
import traceback
import xml.etree.cElementTree as ET
from   itertools import groupby
from   bson.objectid import ObjectId

# DAS modules
from   DAS.utils.regex import float_number_pattern, int_number_pattern
from   DAS.utils.regex import phedex_tier_pattern, cms_tier_pattern
from   DAS.utils.regex import se_pattern, site_pattern, unix_time_pattern
from   DAS.utils.regex import last_time_pattern, date_yyyymmdd_pattern
import DAS.utils.jsonwrapper as json

def dastimestamp(msg='DAS '):
    """
    Return timestamp in pre-defined format. For simplicity we match
    cherrypy date format.
    """
    tstamp = time.strftime('[%d/%b/%Y:%H:%M:%S]', time.localtime())
    if  msg:
        return msg + tstamp
    return tstamp

def print_exc(exc):
    """Standard way to print exceptions"""
    print dastimestamp('DAS ERROR '), type(exc), str(exc)
    _type, _value, exc_traceback = sys.exc_info()
    traceback.print_tb(exc_traceback, file=sys.stdout)

def parse_filters(query):
    """
    Parse filter list in DAS query and return MongoDB key/value dictionary.
    Be smart not to overwrite spec condition of DAS query.
    """
    spec  = query.get('spec', {})
    filters = query.get('filters', [])
    mdict = {}
    existance = {'$exists': True}
    for filter in filters:
        for key, val in parse_filter(spec, filter).items():
            if  mdict.has_key(key):
                value = mdict[key]
                if  isinstance(value, dict) and isinstance(val, dict):
                    mdict[key].update(val)
                elif isinstance(value, dict) and value == existance:
                    mdict[key] = val
                else:
                    msg  = 'Mis-match in filters (%s, %s)' \
                        % ({key:value}, filter)
                    raise Exception(msg)
            else:
                mdict.update({key:val})
    return mdict

def parse_filter(spec, filter):
    """
    Parse given filter and return MongoDB key/value dictionary.
    Be smart not to overwrite spec condition of DAS query.
    """
    if  filter.find('=') != -1 and \
       (filter.find('<') == -1 and filter.find('>') == -1):
        key, val = filter.split('=')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        if  float_number_pattern.match(str(val)):
            val = float(val)
        if  isinstance(val, str) or isinstance(val, unicode):
            if  val.find('*') != -1:
                val = re.compile('%s' % val.replace('*', '.*'))
        return {key:val}
    elif  filter.find('<=') != -1:
        key, val = filter.split('<=')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        if  float_number_pattern.match(str(val)):
            val = float(val)
        return {key: {'$lte': val}}
    elif  filter.find('<') != -1:
        key, val = filter.split('<')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        if  float_number_pattern.match(str(val)):
            val = float(val)
        return {key: {'$lt': val}}
    elif  filter.find('>=') != -1:
        key, val = filter.split('>=')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        if  float_number_pattern.match(str(val)):
            val = float(val)
        return {key: {'$gte': val}}
    elif  filter.find('>') != -1:
        key, val = filter.split('>')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        if  float_number_pattern.match(str(val)):
            val = float(val)
        return {key: {'$gt': val}}
    else:
        if  not spec.get(filter, None) and filter != 'unique':
            return {filter:{'$exists':True}}
    return {}

def size_format(uinput):
    """
    Format file size utility, it converts file size into KB, MB, GB, TB, PB units
    """
    try:
        num = float(uinput)
    except Exception as exc:
        print_exc(exc)
        return "N/A"
    base = 1000. # power of 10, or use 1024. for power of 2
    for xxx in ['','KB','MB','GB','TB','PB']:
        if  num < base: 
            return "%3.1f%s" % (num, xxx)
        num /= base

def convert2date(value):
    """
    Convert input value to date range format expected by DAS.
    """
    msg = "Unsupported syntax for value of last operator"
    pat = last_time_pattern
    if  not pat.match(value):
        raise Exception(msg)
    if  value.find('h') != -1:
        hour = int(value.split('h')[0])
        if  hour > 24:
            raise Exception('Wrong hour %s' % value)
        date1 = time.time() - hour*60*60
        date2 = time.time()
    elif value.find('m') != -1:
        minute = int(value.split('m')[0])
        if  minute > 60:
            raise Exception('Wrong minutes %s' % value)
        date1 = time.time() - minute*60
        date2 = time.time()
    else:
        raise Exception('Unsupported value for last operator')
    value = [long(date1), long(date2)]
    return value

def das_dateformat(value):
    """Check if provided value in expected DAS date format."""
    value = str(value)
    pat = date_yyyymmdd_pattern
    if  pat.match(value): # we accept YYYYMMDD
        ddd = datetime.date(int(value[0:4]), # YYYY
                            int(value[4:6]), # MM
                            int(value[6:8])) # DD
        return calendar.timegm((ddd.timetuple()))
    else:
        msg = 'Unacceptable date format'
        raise Exception(msg)

def convert_datetime(sec):
    """
    Convert seconds since epoch or YYYYMMDD to date YYYY-MM-DD
    """
    value = str(sec)
    pat = date_yyyymmdd_pattern
    pat2 = unix_time_pattern
    if pat.match(value): # we accept YYYYMMDD
        return "%s-%s-%s" % (value[:4], value[4:6], value[6:8])
    elif pat2.match(value):
        return time.strftime("%Y-%m-%d", time.gmtime(sec))
    else:
        msg = 'Unacceptable date format'
        raise Exception(msg)

def dbsql_opt_map(operator):
    """
    convert the das operator to normal ones
    I only need a map, it might be already exists.
    $gt $lt $gte $lte
    >   <   >=   <=
    """
    if operator == '$gt':
        return '>'
    elif operator == '$lt':
        return '<'
    elif operator == '$gte':
        return '>='
    elif operator == '$lte':
        return '<='
    elif operator == '$in':
        return 'in'
    

def get_http_expires(data):
    """
    Return HTTP Expires value in seconds since epoch.
    If it is not set, None value is returned.
    """
    expire = data.info().getheader('Expires')
    if  expire:
        expire = calendar.timegm( \
        time.strptime(expire, '%a, %d %b %Y %H:%M:%S %Z') )
    return expire

def datestamp(das_date):
    """Convert provided DAS date into datetime.date object"""
    tstamp = str(das_date)
    if  len(tstamp) != 8:
        raise Exception('Provided date does not confirm DAS date format')
    year  = tstamp[:4]
    month = tstamp[4:6]
    day   = tstamp[6:8]
    return datetime.date(int(year), int(month), int(day))

def next_day(das_date):
    """Return next date provided DAS date""" 
    next  = datestamp(das_date) + datetime.timedelta(days=1)
    return int(time.strftime("%Y%m%d", next.timetuple()))
    
def prev_day(das_date):
    """Return previous date provided DAS date""" 
    prev  = datestamp(das_date) - datetime.timedelta(days=1)
    return int(time.strftime("%Y%m%d", prev.timetuple()))

def expire_timestamp(expire):
    """
    Return expire timestamp. The input parameter expire can be in a form of
    integer or HTTP header string.
    """
    # check if we provided with HTTP header string
    if  isinstance(expire, str) and \
        expire.find(',') != -1 and expire.find(':') != -1:
        return calendar.timegm(time.strptime(expire, '%a, %d %b %Y %H:%M:%S %Z'))
    if  isinstance(expire, str):
        expire = long(expire)
    tstamp = time.time()
    # use Jan 1st, 2010 as a seed to check expire date
    # prior 2010 DAS was not released in production
    tup = (2010, 1, 1, 0, 0, 0, 0, 1, -1)
    if  isinstance(expire, int) and expire < calendar.timegm(tup):
        expire = tstamp + expire
    return expire

def yield_rows(*args):
    """
    Yield rows from provided input.
    """
    for iarg in args:
        if  isinstance(iarg, GeneratorType):
            for row in iarg:
                yield row
        elif isinstance(iarg, list):
            for row in iarg:
                yield row
        else:
            yield iarg

def adjust_value(value):
    """
    Change null value to None.
    """
    pat_float   = float_number_pattern
    pat_integer = int_number_pattern
    if  isinstance(value, str):
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

def adjust_mongo_keyvalue(value):
    """
    This function can be used to adjust values of MongoDB queries, which
    are represented via $in, $lte, $gte keys
    """
    newdict = value
    if  isinstance(value, dict):
        newdict = {}
        for key, val in value.items():
            newval = val
            if  isinstance(val, dict):
                for kkk, vvv in val.items():
                    if  kkk == '$in':
                        newval = vvv
                    elif  kkk == '$lte' or kkk == '$gte':
                        newval = [val['$gte'], val['$lte']]
                    else:
                        msg = 'Unsupported key-value %s' % val
                        raise Exception(msg)
                    newdict[key] = newval
    return newdict

class dict_of_none (dict):
    """Define new dict type whose missing keys always assigned to None"""
    def __missing__ (self, key):
        """Assign missing key to None"""
        return None

def yield_obj(rdict, ckey):
    """
    Helper function for DotDict class.
    For a given dict and compound key (a.b.c) extract and yield next key
    and object(s) for the first key of the provided compound key. 
    """
    keys = ckey.split('.')
    key  = keys[0]
    if  len(keys) > 1:
        next = '.'.join(keys[1:])
    else:
        next = None
    if  rdict.has_key(key):
        obj = rdict[key]
        if  isinstance(obj, list) or isinstance(obj, GeneratorType):
            for item in obj:
                yield next, item
        else:
            yield next, obj

class DotDict(dict):
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
        if  isinstance(obj, dict):
            return DotDict(obj)
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
#        for key in keys:
        for idx in range(0, len(keys)):
            key  = keys[idx]
            try:
                next = keys[idx+1]
            except:
                next = None
            if  key == keys[0]:
                obj = self.get(key, None)
            else:
                obj = getattr(obj, key)
            if  not obj and obj != 0:
                return None
            if  isinstance(obj, dict):
                obj = DotDict(obj)
            elif isinstance(obj, list):
                for item in obj:
                    obj = item
                    if  next and obj.has_key(next):
                        if  isinstance(obj, dict):
                            obj = DotDict(obj)
                            if  obj:
                                break
#                obj = obj[0]
#                if  isinstance(obj, dict):
#                    obj = DotDict(obj)
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

    def get_values(self, ckey):
        """
        Generator which yields values for any compound key. It works
        up to three level deep in DotDict structure.
        """
        for next_key, item in yield_obj(self, ckey):
            if  isinstance(item, dict):
                for final, elem in yield_obj(item, next_key):
                    if  isinstance(elem, dict) and elem.has_key(final):
                        yield elem[final]
                    else:
                        yield elem
            elif isinstance(item, list) or isinstance(item, GeneratorType):
                for final, elem in item:
                    for last, att in yield_obj(elem, final):
                        if  isinstance(att, dict) and att.has_key(last):
                            yield att[last]
                        else:
                            yield att

    def get_keys(self, ckey):
        """Generator which yields all keys for a starting ckey"""
        if  self.has_key(ckey):
            doc = self[ckey]
        else:
            doc = [o for o in self.get_values(ckey)]
        if  isinstance(doc, dict):
            for key in doc.keys():
                if  ckey.rfind('%s.' % key) == -1:
                    yield '%s.%s' % (ckey, key)
                    for kkk in self.get_keys('%s.%s' % (ckey, key)):
                        yield kkk
        elif isinstance(doc, list):
            for item in doc:
                if  isinstance(item, dict):
                    for key in item.keys():
                        if  ckey.rfind('%s.' % key) == -1:
                            yield '%s.%s' % (ckey, key)
                            for kkk in self.get_keys('%s.%s' % (ckey, key)):
                                yield kkk
                elif isinstance(item, list):
                    for elem in item:
                        if  isinstance(elem, dict):
                            yield '%s.%s' % (ckey, elem)
                        else:
                            yield ckey
                else: # basic type, so we reach the end
                    yield ckey
        else: # basic type, so we reach the end
            yield ckey

def dict_type(obj):
    """
    Return if provided object is type of dict or instance of DotDict class
    """
    return isinstance(obj, dict) or isinstance(obj, DotDict)

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
                if  isinstance(value, list):
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
    for key, value in dict2.items():
        if  dict1.has_key(key):
            val = dict1[key]
            if  isinstance(val, list):
                if  isinstance(value, list):
                    dict1[key] = val + value
                else:
                    val.append(value)
                    dict1[key] = val
            else:
                if  isinstance(value, list):
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
    if  isinstance(query, dict):
        query = json.JSONEncoder(sort_keys=True).encode(query)
    keyhash.update(query)
    return keyhash.hexdigest()

def gen2list(results):
    """
    Convert generator to a list discarding duplicates
    """
    reslist = [name for name, _ in groupby(results)]
    return reslist

def dump(ilist, idx=0):
    """
    Print items in provided generator
    """
    if  isinstance(ilist, GeneratorType):
        reslist = ilist
    elif not isinstance(ilist, list):
        reslist = [ilist]
    else:
        reslist = ilist
    if  not reslist:
        print "dump, no results found"
        return
    idx = 0
    for row in reslist:
        print "id : %s" % idx
        if  isinstance(row, dict):
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
    if  isinstance(ilist1, GeneratorType):
        list1 = [i for i in ilist1]
    else:
        list1 = ilist1
    if  isinstance(ilist2, GeneratorType):
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
        for jdx in range(0, len(slave_list)):
            jdict = slave_list[jdx]
            found = 0
            for key in rel_keys:
                if  idict[key] == jdict[key]:
                    found += 1
            if  found == len(rel_keys):
                if  not jdx:
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
    """Generate timestamp"""
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
    if  isinstance(results, GeneratorType):
        resdict['results'] = [res for res in results]
    else:
        resdict['results'] = results
    return resdict

# NOTE: I can use genresults generator implementation only if
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
    for kkk, vvv in indict.items():
        row[kkk] = None
        if  isinstance(vvv, list):
            if  foundlist and foundlist != len(vvv):
                raise Exception('Input dict contains multi-sized lists')
            foundlist = len(vvv)

    olist = []
    if  foundlist:
        for idx in range(0, foundlist):
            newrow = dict(row)
            for kkk, vvv in indict.items():
                if  isinstance(vvv, list):
                    newrow[kkk] = vvv[idx]
                else:
                    newrow[kkk] = vvv
            olist.append(newrow)
    return olist

def getarg(kwargs, key, default):
    """
    retrieve value from input dict for given key and default
    """
    arg = default
    if  kwargs.has_key(key):
        arg = kwargs[key]
        if  isinstance(default, int):
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
        if  not isinstance(val, list):
            val = [val]
        if  isinstance(value, list):
            idict[key] = val + value
        else:
            val = idict[key]
            if  isinstance(val, list):
                idict[key].append(value)
            else:
                idict[key] = [val, value]
    else:
        idict[key] = value

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
    if  not isinstance(smap.keys(), list):
        raise Exception(msg)
    possible_keys = ['api', 'keys', 'params', 'url', 'expire', 
                        'format', 'wild_card']
    possible_keys.sort()
    for item in smap.values():
        if  not isinstance(item, dict):
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
        if  not isinstance(item['keys'], list):
            raise Exception(msg)
        if  not isinstance(item['params'], dict):
            raise Exception(msg)

def permutations(iterable, rrr=None):
    """
    permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    permutations(range(3)) --> 012 021 102 120 201 210
    """
    pool = tuple(iterable)
    nnn = len(pool)
    rrr = nnn if rrr is None else rrr
    if  rrr > nnn:
        return
    indices = range(nnn)
    cycles = range(nnn, nnn-rrr, -1)
    yield tuple(pool[i] for i in indices[:rrr])
    while nnn:
        for idx in reversed(range(rrr)):
            cycles[idx] -= 1
            if cycles[idx] == 0:
                indices[idx:] = indices[idx+1:] + indices[idx:idx+1]
                cycles[idx] = nnn - idx
            else:
                jdx = cycles[idx]
                indices[idx], indices[-jdx] = indices[-jdx], indices[idx]
                yield tuple(pool[i] for i in indices[:rrr])
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
    tmplist = [k for k, _ in groupby(ilist)]
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

def access(data, elem):
    """
    Access elements of the dict (data). The element can be supplied in dotted form, e.g.
    site.admin.title and code will search for::

        {'site':[{'admin':[{'title'...}]}]}
    """
    if  elem.find('.') == -1:
        key = elem
        if  isinstance(data, dict):
            if  data.has_key(key):
                yield data[key]
        elif isinstance(data, list) or isinstance(data, GeneratorType):
            for item in data:
                if  item.has_key(key):
                    yield item[key]
    else:
        keylist = elem.split('.')
        key  = keylist[0]
        rkey = '.'.join(keylist[1:])
        if  data.has_key(key):
            res = data[key]
            if  isinstance(res, dict):
                result = access(res, rkey)
                if  isinstance(result, GeneratorType):
                    for item in result:
                        yield item
            elif isinstance(res, list) or isinstance(res, GeneratorType):
                for row in res:
                    result = access(row, rkey)
                    if  isinstance(result, GeneratorType):
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
                if  isinstance(recval, dict):
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

def qlxml_parser(source, prim_key):
    """
    It's a temperary XML parser for DBS2 QL results. The same module as 
    xml_parser below, but only cover the DBS2 QL results, which is storing 
    value in emlement.text other that element.attrib.

    Iterparse method is using to walk through provide source descriptor(
    as xml_parser method)
    
    tag 'row' will be captured, then we construct a result dict base on 
    it's chilren.  
    The input prim_key defines the name of dict.
    
    """
    notations = {}
    context   = ET.iterparse(source, events=("start", "end"))
    root = None
    row = {}
    row[prim_key]={}
    for item in context:
        event, elem = item
        key = elem.tag
        if key != 'row':
            continue
        if event == 'start' :
            root = elem
        if  event == 'end':
            row = {}
            row[prim_key] = {}
            get_children(elem, event, row, prim_key, notations)
            elem.clear()
            yield row
    if  root:
        root.clear()
    source.close()


def xml_parser(source, prim_key, tags=None):
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
                        if  isinstance(att_value, dict):
                            att_value = \
                                dict_helper(elem.attrib[attr], notations)
                        if  isinstance(att_value, str):
                            att_value = adjust_value(att_value)
                        sup[atag] = {attr:att_value}
                else:
                    if  elem.tag == tag:
                        sup[tag] = elem.attrib
        key = elem.tag
        if  key != prim_key:
            continue
        row[key] = dict_helper(elem.attrib, notations)
        row[key].update(sup)
        get_children(elem, event, row, key, notations)
        if  event == 'end':
            elem.clear()
            yield row
    root.clear()
    source.close()

def get_children(elem, event, row, key, notations):
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

        if  isinstance(row[key], dict) and row[key].has_key(child_key):
            val = row[key][child_key]
            if  isinstance(val, list):
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
                if  isinstance(child_dict, dict):
                    newdict = {child_key: child_dict}
                else:
                    newdict = {child_key: {}}
                get_children(child, event, newdict, child_key, notations) 
                row[key][child_key] = newdict[child_key]
            else:
                if  not isinstance(row[key], dict):
                    row[key] = {}
                row[key][child_key] = child_dict
        if  event == 'end':
            child.clear()

def json_parser(source, logger=None):
    """
    JSON parser based on json module. It accepts either source
    descriptor with .read()-supported file-like object or
    data as a string object.
    """
    if  isinstance(source, InstanceType) or isinstance(source, file):
        # got data descriptor
        try:
            jsondict = json.load(source)
        except Exception as exc:
            print_exc(exc)
            source.close()
            raise
        source.close()
    else:
        data = source
        # to prevent unicode/ascii errors like
        # UnicodeDecodeError: 'utf8' codec can't decode byte 0xbf in position
        if  isinstance(data, str):
            data = unicode(data, errors='ignore')
            res  = data.replace('null', '\"null\"')
        else:
            res  = data
        try:
            jsondict = json.loads(res)
        except:
            msg  = "json_parser, WARNING: fail to JSON'ify data:"
            msg += "\n%s\ndata type %s" % (res, type(res))
            if  logger:
                logger.warning(msg)
            else:
                print msg
            jsondict = eval(res, { "__builtins__": None }, {})
    yield jsondict

def plist_parser(source):
    """
    Apple plist parser based on plistlib. It accepts either source
    descriptor with .read()-supported file-like object or
    data as a string object.
    """
    if  isinstance(source, InstanceType) or isinstance(source, file):
        # got data descriptor
        try:
            data = source.read()
        except Exception as exc:
            print_exc(exc)
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
    item = None
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
    if  not isinstance(row, dict):
        return
    for key, val in row.items():
        newkey = mapper(system, key, api)
        if  newkey != key:
            row.pop(key)
            nkey, nval = convert_dot_notation(newkey, val)
            row.update({nkey:nval})
        if  isinstance(val, dict):
            row2das(mapper, system, api, val)
        elif isinstance(val, list):
            for item in val:
                if  isinstance(item, dict):
                    row2das(mapper, system, api, item)

def aggregator(results, expire):
    """
    High-level API, DAS aggregator function.
    """
    for rec in aggregator_helper(results, expire):
        das_id = rec.pop('das_id')
        if  isinstance(das_id, list):
            rec['das_id'] = list(set(das_id))
        else:
            rec['das_id'] = [das_id]
        _ids = rec.pop('_id')
        if  not isinstance(_ids, list):
            if  isinstance(_ids, ObjectId):
                _ids = [str(_ids)]
            else:
                _ids = [_ids]
        rec['cache_id'] = list(set(_ids))
        rec['das']['empty_record'] = 0
        yield rec

def aggregator_helper(results, expire):
    """
    DAS aggregator helper which iterates over all records in results set and
    perform aggregation of records on the primary_key of the record.
    """
    def helper(expire, prim_key, system, cond_keys):
        "Construct a dict out of provided values"
        rdict = dict(expire=expire, primary_key=prim_key, system=system,
                        condition_keys=cond_keys)
        return dict(das=rdict)

    record    = results.next()
    prim_key  = record['das']['primary_key']
    cond_keys = record['das']['condition_keys']
    system    = record['das']['system']
    record.pop('das')
    update = 1
    row = {}
    for row in results:
        row_prim_key  = row['das']['primary_key']
        row_cond_keys = row['das']['condition_keys']
        row_system    = row['das']['system']
        row.pop('das')
        if  row_prim_key != prim_key:
            record.update(helper(expire, prim_key, system, cond_keys))
            yield record
            prim_key = row_prim_key
            record = row
            system = row_system
            cond_keys = list( set(cond_keys+row_cond_keys) )
            continue
        try:
            val1 = dict_value(record, prim_key)
        except:
            record.update(helper(expire, prim_key, system, cond_keys))
            yield record
            record = dict(row)
            system = row_system
            cond_keys = list( set(cond_keys+row_cond_keys) )
            update = 0
            continue
        try:
            val2 = dict_value(row, prim_key)
        except:
            row.update(helper(expire, prim_key, system, cond_keys))
            yield row
            record = dict(row)
            system = row_system
            cond_keys = list( set(cond_keys+row_cond_keys) )
            update = 0
            continue
        if  val1 == val2:
            merge_dict(record, row)
            system = list(set(system) | set(row_system))
            cond_keys = list( set(cond_keys+row_cond_keys) )
            update = 1
        else:
            record.update(helper(expire, prim_key, system, cond_keys))
            yield record
            record = dict(row)
            system = row_system
            cond_keys = list( set(cond_keys+row_cond_keys) )
            update = 0
    if  update: # check if we did update for last row
        record.update(helper(expire, prim_key, system, cond_keys))
        yield record
    else:
        row.update(helper(expire, prim_key, system, cond_keys))
        yield row

def das_diff(rows, compare_keys):
    """Perform diff action for given set of rows and compare_keys"""
    diff_keys = []
    for row in rows:
        for key in compare_keys:
            values = [v for v in DotDict(row).get_values(key)]
            if  len(set(values)) > 1:
                diff_keys.append(key)
        if  diff_keys:
            if  row.has_key('das'):
                row['das'].update({'conflict':diff_keys})
            else:
                row.update({'das':{'conflict':diff_keys}})
        yield row
        diff_keys = []

def unique_filter(rows):
    """
    Unique filter drop duplicate rows.
    """
    old_row = {}
    row = None
    for row in rows:
        row_data = dict(row)
        try:
            del row_data['_id']
            del row_data['das']
            del row_data['das_id']
            del row_data['cache_id']
        except:
            pass
        old_data = dict(old_row)
        try:
            del old_data['_id']
            del old_data['das']
            del old_data['das_id']
            del old_data['cache_id']
        except:
            pass
        if  row_data == old_data:
            continue
        if  old_row:
            yield old_row
        old_row = row
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
            if  isinstance(value, dict):
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
    elif  data_format.lower() == 'dasjson':
        headers.update({'Accept':'text/json+das;text/json;application/json'})
    elif data_format.lower() == 'xml':
        headers.update({'Accept':'text/xml;application/xml'})
    return headers

def filter_with_filters(rows, filters):
    """
    Filter given rows with provided set of filters.
    """
    for row in rows:
        ddict = DotDict(row)
        flist = [(f, ddict._get(f)) for f in filters]
        for idx in flist:
            yield idx
