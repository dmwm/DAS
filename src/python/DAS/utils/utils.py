#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703,W0702,R0911,R0912,R0913,R0914,R0915,C0302

"""
General set of useful utilities used by DAS
"""
from __future__ import print_function

__author__ = "Valentin Kuznetsov"

# system modules
import os
import re
import cgi
import sys
import json
import time
import copy
from   types import GeneratorType
import inspect
import hashlib
import calendar
import datetime
import traceback
import itertools
import xml.etree.cElementTree as ET
from   itertools import groupby
from   bson.objectid import ObjectId
try:
    from HTMLParser import HTMLParser
except ImportError: # python3
    from html.parser import HTMLParser

# python3
if  sys.version.startswith('3.'):
    basestring = str
    unicode = str

# DAS modules
from   DAS.utils.ddict import DotDict, convert_dot_notation
from   DAS.utils.regex import float_number_pattern, int_number_pattern
from   DAS.utils.regex import phedex_node_pattern, cms_tier_pattern
from   DAS.utils.regex import se_pattern, site_pattern, unix_time_pattern
from   DAS.utils.regex import last_time_pattern, date_yyyymmdd_pattern
from   DAS.utils.regex import rr_time_pattern, das_time_pattern
from   DAS.utils.regex import http_ts_pattern

# Define transient fields in DAS records to be removed by hash function
TRANSIENT_FIELDS = ['ts', 'expire']

# Singletons in python
# http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python

# Singleton class
#class Singleton(object):
#    "Implement Singleton behavior"
#    def __new__(cls, *args, **kwargs):
#        "Define single instance and return it back"
#        if  not hasattr(cls, '_instance'):
#            cls._instance = object.__new__(cls, *args, **kwargs)
#        return cls._instance

# Singleton metaclass
class Singleton(type):
    "Implement Singleton behavior as metaclass"
    _instances = {}
    def __call__(cls, *args, **kwargs):
        "Define single instance and return it back"
        if  cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

#Python2
#class MyClassSingleton(object):
#    __metaclass__ = Singleton
#Python3
#class MyClassSingleton(object, metaclass=Singleton):
#    pass

class DASHTMLParser(HTMLParser):
    """
    Minimalistic HTML parser suitable to parsing HTML content from data-providers.
    This class is intended to use in xml/json parsers as HTML handler.
    """
    def __init__(self):
        HTMLParser.__init__(self)
        self.write  = False
        self.output = []
    def handle_starttag(self, tag, attrs):
        "Handle start tag, set write flag to True when encounter body tag"
        if  tag == 'body':
            self.write = True
    def handle_endtag(self, tag):
        "Handle start tag, set write flag to False when encounter body tag"
        if  tag == 'body':
            self.write = False
    def handle_data(self, data):
        "Handle data"
        if  data and self.write:
            self.output.append(data.strip())
    def content(self):
        "Return escaped content of the web page"
        out = '\n'.join(self.output)
        return out and cgi.escape(out, quote=True) or ''

def upper_lower(ilist):
    "Return list of lower and upper words from given list"
    ilist = [i.lower() for i in ilist] + [i.upper() for i in ilist]
    return list(set(ilist))

def md5hash(rec):
    "Return md5 hash of given query"
    if  not isinstance(rec, dict):
        raise NotImplementedError
    # discard timestamp fields from hash calculations since they're dynamic
    record = dict(rec)
    for key in ['ts', 'timestamp']:
        if  key in record:
            del record[key]
    rec = json.JSONEncoder(sort_keys=True).encode(record)
    keyhash = hashlib.md5()
    try:
        keyhash.update(rec)
    except TypeError: # python3
        keyhash.update(rec.encode('ascii'))
    return keyhash.hexdigest()

def add_hash(record):
    "Add hash into given record"
    if  not isinstance(record, dict):
        raise NotImplementedError
    rec = dict(record)
    for key in TRANSIENT_FIELDS:
        if  key in rec:
            del rec[key]
    md5 = md5hash(rec)
    record.update({'hash':md5})

def record_codes(rtype):
    "Return das record code for given record type"
    codes = {'query_record': 0,
             'data_record': 1,
             'empty_record': 2,
             'gridfs_record': 3}
    return codes[rtype]

def get_dbs_instance(url):
    "Extract from DBS url its instance name"
    msg = 'Unsupported DBS url=%s' % url
    if  not url:
        raise Exception(msg)
    if  url.find('DBSReader') != -1: # DBS3
        return '/'.join(url.split('/')[4:6])
    elif url.find('localhost') != -1: # test instance
        return None
    elif url.find('127.0.0.1') != -1: # test instance
        return None
    else:
        raise Exception(msg)

def parse_dbs_url(dbs, url):
    """
    Parse and return main DBS url for given dbs system type and DBS url from
    DAS maps
    """
    if  dbs == 'dbs3':
        parts = url.split('/')
        if  not parts[-1]:
            parts = parts[:-1]
        url = '%s//%s' % (parts[0], '/'.join(parts[2:-1]))
    return url

def convert2ranges(ilist):
    """
    Convert input list into list of ranges.
    http://stackoverflow.com/questions/4628333/converting-a-list-of-integers-into-range-in-python
    """
    # right now just sort input list and return it
    ilist = list(set(sorted([int(i) for i in ilist])))
    ilist.sort()
    res = [[t[0][1], t[-1][1]] for t in \
            (tuple(g[1]) for g in \
                itertools.groupby(enumerate(ilist), lambda i_x: i_x[0] - i_x[1]))]
    return res

def identical_data_records(old, row):
    """Checks if 2 DAS records are identical"""
    old_keys = [k for k in old.keys() if not (k.find('_id')!=-1 or k=='das')]
    old_keys.sort()
    row_keys = [k for k in row.keys() if not (k.find('_id')!=-1 or k=='das')]
    row_keys.sort()
    if  old_keys != row_keys:
        return False
    for key in old_keys:
        if  old[key] != row[key]:
            return False
    return True

def delete_keys(rec, value):
    "Delete dict keys whose values equal to a given one"
    if  value in rec.values():
        to_delete = [k for k, v in rec.items() if v == value]
        for key in to_delete:
            if  key in rec:
                del rec[key]

def deepcopy(obj):
    """Perform full copy of given object into new object"""
    if  isinstance(obj, dict):
        newobj = {}
        for key, val in obj.items():
            if  isinstance(val, dict):
                newobj[key] = deepcopy(val)
            elif isinstance(val, list):
                newobj[key] = list(val)
            else:
                newobj[key] = val
    else:
        newobj = copy.deepcopy(obj)
    return newobj

def dasprint(*args):
    "Invoke normal print function and flush stdout/stderr"
    out = ' '.join([str(a) for a in args])
    print(out)
    sys.__stdout__.flush()
    sys.__stderr__.flush()

def dastimestamp(msg='DAS'):
    """
    Return timestamp in pre-defined format. For simplicity we match
    cherrypy date format.
    """
    tst = time.localtime()
    tstamp = time.strftime('[%d/%b/%Y:%H:%M:%S]', tst)
    return '%s %s %s' % (msg.strip(), tstamp, time.mktime(tst))

def http_timestamp(tstamp=None):
    "Return HTTP complaint time stamp"
    if  isinstance(tstamp, float) or isinstance(tstamp, long) or \
        isinstance(tstamp, int):
        tstamp = time.gmtime(tstamp)
    elif isinstance(tstamp, basestring) and http_ts_pattern.match(tstamp):
        return tstamp
    else:
        tstamp = time.gmtime()
    return time.strftime('%a, %d %b %Y %H:%M:%S GMT', tstamp)

def inspect_module(msg=None):
    "Printout function stack calls"
    if  msg:
        print(msg)
    for item in inspect.stack():
        print(item) # stack item

def print_exc(exc, print_traceback=True):
    """Standard way to print exceptions"""
    stack = inspect.stack()[1]
    caller = "%s:%s" % (stack[1], stack[2])
    print(dastimestamp('DAS ERROR '), type(exc), str(exc), caller)
    if  print_traceback:
        _type, _value, exc_traceback = sys.exc_info()
        traceback.print_tb(exc_traceback, file=sys.stdout)

def parse_filters(query):
    """
    Parse filter list in DAS query and return MongoDB key/value dictionary.
    Be smart not to overwrite spec condition of DAS query.
    """
    spec  = query.get('spec', {})
    filters = query.get('filters', {})
    filters = filters.get('grep', [])
    mdict = {}
    existance = {'$exists': True}
    for flt in filters:
        for key, val in parse_filter(spec, flt).items():
            if  key in mdict:
                value = mdict[key]
                if  isinstance(value, dict) and isinstance(val, dict):
                    mdict[key].update(val)
                elif isinstance(value, dict) and value == existance:
                    mdict[key] = val
                else:
                    msg  = 'Mis-match in filters (%s, %s)' \
                        % ({key:value}, flt)
                    raise Exception(msg)
            else:
                mdict.update({key:val})
    # clean-up exists conditions
    for key, val in mdict.items():
        if  isinstance(val, dict) and len(val.keys()) > 1 and '$exists' in val:
            del val['$exists']
    return mdict

def parse_filter_string(val):
    "Parse filter string value"
    if  isinstance(val, basestring):
        val = val.strip()
        if  val and val[0] == '[' and  val[-1] == ']':
            val = val.replace('[', '').replace(']', '')
            val = val.split(',')
    return val

def parse_filter(spec, flt):
    """
    Parse given filter and return MongoDB key/value dictionary.
    Be smart not to overwrite spec condition of DAS query.
    """
    if  flt.find('=') != -1 and flt.find('!=') == -1 and\
       (flt.find('<') == -1 and flt.find('>') == -1):
        key, val = flt.split('=')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        elif float_number_pattern.match(str(val)):
            val = float(val)
        elif isinstance(val, str) or isinstance(val, unicode):
            if  val.find('*') != -1:
                val = re.compile('%s' % val.replace('*', '.*'))
            val = parse_filter_string(val)
        return {key:val}
    elif flt.find('!=') != -1 and \
       (flt.find('<') == -1 and flt.find('>') == -1):
        key, val = flt.split('!=')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        elif float_number_pattern.match(str(val)):
            val = float(val)
        elif isinstance(val, str) or isinstance(val, unicode):
            if  val.find('*') != -1:
#                val = re.compile('%s' % val.replace('*', '.*'))
                val = re.compile('^(?:(?!%s).)*$' % val.replace('*', '.*'))
            else:
                val = re.compile('^(?:(?!%s).)*$' % val)
            val = parse_filter_string(val)
            return {key: val}
        return {key: {'$ne': val}}
    elif  flt.find('<=') != -1:
        key, val = flt.split('<=')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        if  float_number_pattern.match(str(val)):
            val = float(val)
        return {key: {'$lte': val}}
    elif  flt.find('<') != -1:
        key, val = flt.split('<')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        if  float_number_pattern.match(str(val)):
            val = float(val)
        return {key: {'$lt': val}}
    elif  flt.find('>=') != -1:
        key, val = flt.split('>=')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        if  float_number_pattern.match(str(val)):
            val = float(val)
        return {key: {'$gte': val}}
    elif  flt.find('>') != -1:
        key, val = flt.split('>')
        if  int_number_pattern.match(str(val)):
            val = int(val)
        if  float_number_pattern.match(str(val)):
            val = float(val)
        return {key: {'$gt': val}}
    else:
        if  not spec.get(flt, None) and flt != 'unique':
            return {flt:{'$exists':True}}
    return {}

def size_format(uinput):
    """
    Format file size utility, it converts file size into KB, MB, GB, TB, PB units
    """
    if  not (float_number_pattern.match(str(uinput)) or \
                int_number_pattern.match(str(uinput))):
        return 'N/A'
    try:
        num = float(uinput)
    except Exception as exc:
        print_exc(exc)
        return "N/A"
    base = 1000. # CMS convention to use power of 10
    if  base == 1000.: # power of 10
        xlist = ['', 'KB', 'MB', 'GB', 'TB', 'PB']
    elif base == 1024.: # power of 2
        xlist = ['', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
    for xxx in xlist:
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
    msg  = 'Unacceptable date format, value=%s, type=%s,' \
            % (value, type(value))
    msg += " supported formats are YYYYMMDD or 'YYYMMDD HH:MM:SS'"
    value = str(value)
    pat = date_yyyymmdd_pattern
    if  pat.match(value): # we accept YYYYMMDD
        if  len(value) == 8: # YYYYMMDD
            ddd = datetime.date(int(value[0:4]), # YYYY
                                int(value[4:6]), # MM
                                int(value[6:8])) # DD
        elif len(value) == 17: # YYYYMMDD HH:MM:SS
            ddd = datetime.datetime(\
                    int(value[0:4]),   ## YYYY
                    int(value[4:6]),   ## MM
                    int(value[6:8]),   ## DD
                    int(value[9:11]),  ## HH
                    int(value[12:14]), ## MM
                    int(value[15:17])) ## SS
        else:
            raise Exception(msg)
        return calendar.timegm((ddd.timetuple()))
    else:
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
        msg = 'Unacceptable date format, value=%s, type=%s' % (sec, type(sec))
        raise Exception(msg)

def presentation_datetime(val):
    """
    Convert input value into DAS presentation datetime format
    (YYYY-MM-DD HH:MM:SS)
    """
    das_format = "%Y-%m-%d %H:%M:%S" # isoformat, see datetime.isoformat()
    value = str(val)
    pat   = date_yyyymmdd_pattern
    pat2  = unix_time_pattern
    pat3  = rr_time_pattern
    if pat.match(value): # we accept YYYYMMDD
        res = "%s-%s-%s 00:00:00" % (value[:4], value[4:6], value[6:8])
    elif pat2.match(value):
        res = time.strftime(das_format, time.gmtime(val))
    elif pat3.match(value):
        dformat = "%a %d-%m-%y %H:%M:%S" # Sun 15-05-11 17:25:00
        tup = time.strptime(value.split('.')[0], dformat)
        res = time.strftime(das_format, tup)
    else:
        msg = 'Unacceptable date format, value=%s, type=%s' % (val, type(val))
        raise Exception(msg)
    if  das_time_pattern.match(res):
        return res
    else:
        msg = 'Fail to convert input value="%s" into DAS date format res="%s"' \
                % (val, res)
        raise Exception(msg)

def fix_times(row):
    "Convert creation/modification times into DAS time format"
    rec = DotDict(row)
    times = ['creation_time', 'modification_time', 'create_time', 'end_time']
    def callback(elem, key):
#        print "\n### callback", key, elem
        val = elem[key]
        if  val:
            elem[key] = presentation_datetime(val)
    for key in rec.get_keys():
        if  key.find('creation_time') != -1 or \
            key.find('modification_time') != -1 or \
            key.find('start_time') != -1 or \
            key.find('end_time') != -1:
            rec.set_values(key, callback)

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
    nextd = datestamp(das_date) + datetime.timedelta(days=1)
    return int(time.strftime("%Y%m%d", nextd.timetuple()))

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
        return calendar.timegm(\
                time.strptime(expire, '%a, %d %b %Y %H:%M:%S %Z'))
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

class DictOfNone(dict):
    """Define new dict type whose missing keys always assigned to None"""
    def __missing__ (self, _key):
        """Assign missing key to None"""
        return None

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
        if  key in dict1:
            val = dict1[key]
            if  val == value:
                dict1[key] = [value, value]
            elif isinstance(val, list):
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
    if  isinstance(query, dict):
        record = dict(query)
        # remove transient fields
        for key in ['ts', 'timestamp']:
            if  key in record:
                del record[key]
        query = json.JSONEncoder(sort_keys=True).encode(record)
    try:
        from DAS.extensions.das_hash import _das_hash
        return _das_hash(query)
    except:
        keyhash = hashlib.md5()
        try:
            keyhash.update(query)
        except TypeError: # python3
            keyhash.update(query.encode('ascii'))
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
        print("dump, no results found")
        return
    idx = 0
    for row in reslist:
        print("id : %s" % idx)
        if  isinstance(row, dict):
            print(json.dumps(row))
        else:
            print(row)
        print()
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
                    if  'system' in idict and \
                        idict['system'].find(jdict['system']) == -1:
                        idict['system'] = '%s+%s' % \
                            (idict['system'], jdict['system'])
                    yield dict(idict)
                else:
                    row = dict(idict)
                    for k in ins_keys:
                        row[k] = jdict[k]
                    if  'system' in row and \
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
            if  key in res:
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
            if  key in res:
                rowdict[key] = res[key]
        olist.append(rowdict)
    return olist

def transform_dict2list(indict):
    """
    transform input dictionary into list of dictionaries, e.g.

    .. doctest::
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
    if  key in kwargs:
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
               ('phedex', phedex_node_pattern),#T2_UK_NO
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
    if  key in idict:
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
#     if  not isinstance(smap.keys(), list):
#         raise Exception(msg)
    possible_keys = ['api', 'keys', 'params', 'url', 'expire',
        'lookup', 'format', 'wild_card', 'ckey', 'cert', 'services']
    for item in smap.values():
        if  not isinstance(item, dict):
            raise Exception(msg)
        keys = list(item.keys())
        if  set(keys) | set(possible_keys) != set(possible_keys):
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
    if  'X509_HOST_CERT' in os.environ:
        cert = os.environ['X509_HOST_CERT']
        key  = os.environ['X509_HOST_KEY']

    # Second preference to User Proxy, very common
    elif 'X509_USER_PROXY' in os.environ:
        cert = os.environ['X509_USER_PROXY']
        key  = cert

    # Third preference to User Cert/Proxy combinition
    elif 'X509_USER_CERT' in os.environ:
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
        print(item)
        yield item

def gen_counter(gen, cdict):
    """
    Trace a generator and count number of records. We use dictionary
    as persistent data storage and yield back the generator items.
    """
    count = 1 if isinstance(cdict, dict) else 0
    for item in gen:
        if  count:
            cdict['counter'] += 1
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
            if  key in data:
                yield data[key]
        elif isinstance(data, list) or isinstance(data, GeneratorType):
            for item in data:
                if  key in item:
                    yield item[key]
    else:
        keylist = elem.split('.')
        key  = keylist[0]
        rkey = '.'.join(keylist[1:])
        if  key in data:
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
        if  keys[-1] in val:
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
    try:
        context   = ET.iterparse(source, events=("start", "end"))
    except IOError as exc: # given source is not parseable
        # try different data format, it can be an HTTP error
        try:
            if  isinstance(source, str):
                data = json.loads(source)
                yield data
        except:
            pass
        msg = 'XML parser, data stream is not parseable: %s' % str(exc)
        print_exc(msg)
        context = []

    root = None
    row = {}
    row[prim_key] = {}
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
    if  hasattr(source, "close"):
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
    try:
        context = ET.iterparse(source, events=("start", "end"))
    except IOError as exc: # given source is not parseable
        # try different data format, it can be an HTTP error
        try:
            if  isinstance(source, str):
                data = json.loads(source)
                yield data
                return
        except:
            pass
        msg = 'XML parser, data stream is not parseable: %s' % str(exc)
        print_exc(msg)
        context = []
        parser = DASHTMLParser()
        parser.feed(source)
        err = 'XML parser, data stream is not parseable: %s' % str(exc)
        reason = parser.content()
        jsondict = {'error': err, 'reason': reason}
        yield jsondict
        return
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
                    if  elem.tag == atag and attr in elem.attrib:
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
    if  root:
        root.clear()
    if  hasattr(source, "close"):
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

        if  isinstance(row[key], dict) and child_key in row[key]:
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
    if  hasattr(source, "close"):
        # got data descriptor
        try:
            jsondict = json.load(source)
        except Exception as exc:
            source.seek(0) # seek to start of the source stream
            parser = DASHTMLParser()
            parser.feed(source.read())
            reason = parser.content()
            jsondict = {'error': str(exc), 'reason': reason}
            print_exc(exc)
            source.close()
        source.close()
    else:
        data = source
        # to prevent unicode/ascii errors like
        # UnicodeDecodeError: 'utf8' codec can't decode byte 0xbf in position
        if  isinstance(data, basestring):
            if  not sys.version.startswith('3.'):
                data = unicode(data, errors='ignore')
            res  = data.replace('null', '\"null\"')
        elif isinstance(data, object) and hasattr(data, 'read'): # StringIO
            res  = data.read()
        else:
            res  = data
        try:
            jsondict = json.loads(res)
        except:
            try: # try to parse HTML
                err = 'JSON parser error'
                parser = DASHTMLParser()
                parser.feed(res)
                reason = parser.content()
                jsondict = {'error': err, 'reason': reason}
            except Exception as exc:
                print(exc)
                msg  = "json_parser, WARNING: fail to JSON'ify data:"
                msg += "\n%s\ndata type %s" % (res, type(res))
                if  logger:
                    logger.warning(msg)
                else:
                    print(msg)
                jsondict = eval(res, { "__builtins__": None }, {})
    yield jsondict

def row2das(mapper, system, api, row):
    """
    Transform keys of row into DAS notations, e.g. bytes to size
    If compound key found, e.g. block.replica.name, it will
    be converted into appropriate dict, e.g. {'block':{'replica':{'name':val}}
    """
    if  not isinstance(row, dict):
        return
    for key in row.keys():
        newkey = mapper(system, key, api)
        val = row[key]
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

def aggregator(dasquery, results, expire):
    """
    High-level API, DAS aggregator function.
    """
    # do not aggregate records when dasquery contains multiple select keys, e.g.
    # file,run,lumi
    fields = dasquery._mongo_query.get('fields', [])
    if  isinstance(fields, list) and len(fields) > 1:
        for rec in results:
            for key in fields:
                val = rec[key]
                if not isinstance(val, list):
                    rec[key] = [val]
            yield rec
        return

    old_rec = None
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
        rec['das']['record'] = record_codes('data_record')
        rec['qhash'] = dasquery.qhash
        for key, val in rec.items():
            if  key not in ['das_id', 'das', 'cache_id', '_id']:
                if  isinstance(val, dict):
                    rec[key] = [val]
        # check for duplicate records
        if  rec != old_rec:
            yield rec
        old_rec = rec

def aggregator_helper(results, expire):
    """
    DAS aggregator helper which iterates over all records in results set and
    perform aggregation of records on the primary_key of the record.
    """
    def helper(das_id, expire, pkey, system, srvs, api, ckeys, tstamp, inst):
        "Construct a dict out of provided values"
        rdict = dict(expire=expire, primary_key=pkey, system=system,
                services=srvs,
                api=api, condition_keys=ckeys, ts=tstamp, instance=inst)
        return dict(das=rdict, das_id=das_id)

    record  = next(results)
    pkey    = record['das']['primary_key']
    ckeys   = record['das']['condition_keys']
    system  = record['das']['system'] # CMS systems, e.g. dbs, phedex
    srvs    = record['das']['services'] # DAS services, e.g. combined
    api     = record['das']['api']
    das_id  = record['das_id']
    inst    = record['das'].get('instance', None)
    tstamp  = time.time()
    record.pop('das')
    update = 1
    row = {}
    for row in results:
        row_pkey   = row['das']['primary_key']
        row_ckeys  = row['das']['condition_keys']
        row_system = row['das']['system']
        row_srvs   = row['das']['services']
        row_api    = row['das']['api']
        row_das_id = row['das_id']
        row.pop('das')
        if  row_pkey != pkey:
            args   = (das_id, expire, pkey, system, srvs,
                      api, ckeys, tstamp, inst)
            record.update(helper(*args))
            yield record
            pkey   = row_pkey
            record = row
            system = row_system
            srvs   = row_srvs
            api    = row_api
            das_id = row_das_id
            ckeys  = list( set(ckeys+row_ckeys) )
            continue
        try:
            val1   = dict_value(record, pkey)
        except:
            args   = (das_id, expire, pkey, system, srvs,
                      api, ckeys, tstamp, inst)
            record.update(helper(*args))
            yield record
            record = dict(row)
            system = row_system
            srvs   = row_srvs
            api    = row_api
            das_id = row_das_id
            ckeys  = list( set(ckeys+row_ckeys) )
            update = 0
            continue
        try:
            val2   = dict_value(row, pkey)
        except:
            args   = (das_id, expire, pkey, system, srvs,
                      api, ckeys, tstamp, inst)
            row.update(helper(*args))
            yield row
            record = dict(row)
            system = row_system
            srvs   = row_srvs
            api    = row_api
            das_id = row_das_id
            ckeys  = list( set(ckeys+row_ckeys) )
            update = 0
            continue
        if  val1 == val2 or (pkey == 'summary' and row_pkey == 'summary'):
            merge_dict(record, row)
            system += row_system
            srvs   += row_srvs
            api    += row_api
            das_id += row_das_id
            ckeys   = list( set(ckeys+row_ckeys) )
            update  = 1
        else:
            args   = (das_id, expire, pkey, system, srvs,
                      api, ckeys, tstamp, inst)
            record.update(helper(*args))
            yield record
            record = dict(row)
            system = row_system
            srvs   = row_srvs
            api    = row_api
            das_id = row_das_id
            ckeys  = list( set(ckeys+row_ckeys) )
            update = 0
    args = (das_id, expire, pkey, system, srvs, api, ckeys, tstamp, inst)
    if  update: # check if we did update for last row
        record.update(helper(*args))
        yield record
    else:
        row.update(helper(*args))
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
            if  'das' in row:
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
    if  'html' in msg.lower():
        parser = DASHTMLParser()
        parser.feed(msg)
        return parser.content()
    try:
        err = json.loads(err)
        if  'message' in err:
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
        headers.update({'Accept':'application/json;text/json'})
    elif  data_format.lower() == 'dasjson':
        headers.update({'Accept':'text/json+das;text/json;application/json'})
    elif data_format.lower() == 'xml':
        headers.update({'Accept':'application/xml;text/xml'})
    return headers

def filter_with_filters(rows, filters):
    """
    Filter given rows with provided set of filters.
    """
    for row in rows:
        ddict = DotDict(row)
        flist = [(f, ddict.get(f)) for f in filters]
        for idx in flist:
            yield idx

def api_rows(gen, api):
    "Extract from given stream only rows which belong to given api"
    das  = {}   # get from first row
    pkey = None # get from das record
    idx  = None # get from api list
    for row in gen:
        if  not das:
            das  = row.get('das')
            pkey = das['primary_key'].split('.')[0]
            apis = das.get('api')
            idx  = apis.index(api)
        try:
            data = row[pkey][idx] # get data item for given api index
            nrow = dict(row)  # get copy of the row
            nrow[pkey] = data # replace pkey value with data item
            nrow['das']['system'] = [nrow['das']['system'][idx]]
            nrow['das']['api'] = [nrow['das']['api'][idx]]
            yield nrow
        except:
            pass

def regen(first, gen):
    "Yield given first row and generator back to workflow"
    yield first
    for row in gen:
        yield row

def das_sinfo(row):
    "Extract DAS information from given row"
    sinfo = {}
    das   = row.get('das')
    apis  = das.get('api')
    srvs  = das.get('system')
    for api, srv in zip(apis, srvs):
        sinfo.setdefault(srv, set()).add(api)
    return sinfo

def sort_rows(rows):
    """
    Sort rows, use this function when we can't use set, e.g.
    when DAS returns sorted list and set will change its original order
    """
    old = ''
    for row in rows:
        if  not old:
            old = row
        if  old != row:
            yield old
            old = row
    yield old


class Event(list):
    """Provides simple Event subscription (Publish-Subscribe).

    Stores a list of callable objects. Calling an instance of this will cause a
    call to each item in the list in ascending order by index.

    Example Usage:

    .. doctest::

        >>> def f(x):
        ...     print 'f(%s)' % x
        >>> def g(x):
        ...     print 'g(%s)' % x
        >>> e = Event()
        >>> e()
        >>> e.append(f)
        >>> e(123)
        f(123)
        >>> e.remove(f)
        >>> e()
        >>> e += (f, g)
        >>> e(10)
        f(10)
        g(10)
        >>> del e[0]
        >>> e(2)
        g(2)

    based on: http://stackoverflow.com/a/2022629/1276782
    """
    def __call__(self, *args, **kwargs):
        for f in self:
            try:
                f(*args, **kwargs)
            except Exception as err:
                print(dastimestamp('DAS ERROR IN EVENT NOTIFY'), str(err))

    def __repr__(self):
        return "Event(%s)" % list.__repr__(self)
