#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703,R0911,R0912,R0914,R0915

"""
DAS query utils.
"""

import re
import sys
import json

# python 3
if  sys.version.startswith('3.'):
    unicode = str
    basestring = str


def encode_mongo_query(query):
    """
    Encode mongo query into storage format. MongoDB does not allow storage of
    dict with keys containing "." or MongoDB operators, e.g. $lt. So we
    convert input mongo query spec into list of dicts whose "key"/"value"
    are mongo query spec key/values. For example

    .. doctest::

        spec:{"block.name":"aaa"}

        converted into

        spec:[{"key":"block.name", "value":'"aaa"'}]

    Conversion is done using JSON dumps method.
    """
    if  not query:
        msg = 'Cannot decode, input query=%s' % query
        raise Exception(msg)
    return_query = dict(query)
    speclist = []
    for key, val in return_query.pop('spec').items():
        if hasattr(val, 'pattern'): # it is sre pattern
            val = json.dumps(val.pattern)
            speclist.append({"key":key, "value":val, "pattern":1})
        else:
            val = json.dumps(val)
            speclist.append({"key":key, "value":val})
    return_query['spec'] = speclist
    return return_query

def decode_mongo_query(query):
    """
    Decode query from storage format into mongo format.
    """
    spec = {}
    for item in query.pop('spec'):
        val = json.loads(item['value'])
        if  'pattern' in item:
            val = re.compile(val)
        spec.update({item['key'] : val})
    query['spec'] = spec
    return query

def convert2pattern(query):
    """
    In MongoDB patterns are specified via regular expression.
    Convert input query condition into regular expression patterns.
    Return new MongoDB compiled w/ regex query and query w/ debug info.
    """
    spec    = query.get('spec', {})
    fields  = query.get('fields', None)
    filters = query.get('filters', None)
    aggs    = query.get('aggregators', None)
    inst    = query.get('instance', None)
    system  = query.get('system', None)
    newspec = {}
    verspec = {}
    for key, val in spec.items():
        if  key == 'records':
            continue # skip special records keyword
        if  isinstance(val, str) or isinstance(val, unicode):
            if  val.find('*') != -1:
                if  val == '*':
                    val = {'$exists':True}
                    verspec[key] = val
                else:
                    val = re.compile("^%s" % val.replace('*', '.*'))
                    verspec[key] = val.pattern
            else:
                verspec[key] = val
            newspec[key] = val
        elif isinstance(val, dict):
            cond  = {}
            vcond = {}
            for ckey, cval in val.items():
                if  isinstance(cval, str) or isinstance(cval, unicode):
                    if  cval.find('*') != -1:
                        cval = re.compile("^%s" % cval.replace('*', '.*'))
                        vcond[ckey] = cval.pattern
                    else:
                        vcond[ckey] = cval
                    cond[ckey] = cval
                else:
                    cond[ckey] = cval
                    vcond[ckey] = cval
            newspec[key] = cond
            verspec[key] = vcond
        else:
            newspec[key] = val
            verspec[key] = val
    newquery = dict(spec=newspec, fields=fields)
    newdquery = dict(spec=verspec, fields=fields)
    if  filters:
        newquery.update({'filters': filters})
        newdquery.update({'filters': filters})
    if  aggs:
        newquery.update({'aggregators': aggs})
        newdquery.update({'aggregators': aggs})
    if  inst:
        newquery.update({'instance': inst})
        newdquery.update({'instance': inst})
    if  system:
        newquery.update({'system': system})
        newdquery.update({'system': system})
    return newquery, newdquery

def compare_dicts(input_dict, exist_dict):
    """
    Helper function for compare_specs. It compares key/val pairs of
    Mongo dict conditions, e.g. {'$gt':10}. Return true if exist_dict
    is superset of input_dict
    """
    for key, val in input_dict.items():
        signal = False
        vvv = None
        if  key in exist_dict:
            vvv = exist_dict[key]
        cond = (isinstance(val, int) or isinstance(val, float)) and \
               (isinstance(vvv, int) or isinstance(vvv, float))
        if  key == '$gt':
            if  cond and val > vvv:
                signal = True
        elif  key == '$gte':
            if  cond and val >= vvv:
                signal = True
        elif key == '$lt':
            if  cond and val < vvv:
                signal = True
        elif key == '$lte':
            if  cond and val <= vvv:
                signal = True
        elif key == '$in':
            if  isinstance(val, list) and isinstance(vvv, list):
                if  set(vvv) > set(val):
                    signal = True
        if signal == False:
            return False
    return True

def compare_str(query1, query2):
    """
    Function to compare string from specs of query.
    Return True if query2 is supperset of query1.
    query1&query2 is the string in the pattern::

         ([a-zA-Z0-9_\-\#/*\.])*

    \* is the sign indicates that a sub string of \*::

        ([a-zA-Z0-9_\-\#/*\.])*

    case 1. if query2 is flat query(w/out \*) then query1 must be the same flat one

    case 2. if query1 is start/end w/ \* then query2 must start/end with \*

    case 3. if query2 is start/end w/out \* then query1 must start/end with query2[0]/query[-1]

    case 4. query1&query2 both include \*

        Way to perform a comparision is spliting:

            query1 into X0*X1*X2*X3
            query2 into Xa*Xb*Xc

        foreach X in (Xa, Xb, Xc):

            case 5. X is '':

                continue

                special case:

                    when X0 & Xa are '' or when X3 & Xc are ''
                    we already cover it in case 2

            case 6. X not in query1 then return False

            case 7. X in query1 begin at index:

                case 7-1. X is the first X not '' we looked up in query1.(Xa)
                    last_idx = index ;
                    continue

                case 7-2. X is not the first:

                    try to find the smallest Xb > Xa
                    if and Only if we could find a sequence:

                        satisfy Xc > Xb > Xa, otherwise return False
                        '=' will happen when X0 = Xa
                        then we could return True
    """
    if query2.find('*') == -1:
        if query1.find('*') != -1:
            return False
        elif query1 != query2:
            return False
        return True
    else:
        if query1.endswith('*') and not query2.endswith('*'):
            return False
        if query1.startswith('*') and not query2.startswith('*'):
            return False

        # last_idx to save where we find last X
        last_idx = -1
        dict2 = query2.split('*')
        if dict2[0] != '' and not query1.startswith(dict2[0]):
            return False
        if dict2[-1] != '' and not query1.endswith(dict2[-1]):
            return False
        for x_item in dict2:
            if x_item == '':
                continue
            # cur_idx to save where we find cur X
            cur_idx = query1.find(x_item)
            if cur_idx == -1:
                return False
            else :
                if last_idx == -1:# first X not ''
                    last_idx = cur_idx
                    continue
                else:
                    while cur_idx <= last_idx:
                        mov = query1[cur_idx+1:].find(x_item)
                        if mov == -1:
                            break
                        else:
                            cur_idx += mov +1
                    if cur_idx > last_idx:
                        last_idx = cur_idx
                        continue
                    else:# mov != -1 or cur_idx <= last_idx
                        return False
        # if reach this then:
        return True


def compare_specs(input_query, exist_query):
    """
    Function to compare set of fields and specs of two input mongo
    queries. Return True if results of exist_query are superset 
    of resulst for input_query.
    """
    # check that instance remain the same
    inst1 = input_query.get('instance', None)
    inst2 = exist_query.get('instance', None)
    if  inst1 != inst2:
        return False

    # we use notation query2 is superset of query1
    query1  = dict(input_query)
    query2  = dict(exist_query)

    # delete aggregators during comparision
    for query in [query1, query2]:
        if  'aggregators' in query:
            del query['aggregators']
        if  'filters' in query:
            del query['filters']
        if  'instance' in query:
            del query['instance']

    if  query1 == query2:
        return True

    fields1 = query1.get('fields', None)
    if  not fields1:
        fields1 = []
    spec1   = query1.get('spec', {})

    fields2 = query2.get('fields', None)
    if  not fields2:
        fields2 = []

    if  fields1 != fields2:
        if  fields1 and fields2 and not set(fields2) > set(fields1):
            return False
        elif fields1 and not fields2:
            return False
        elif fields2 and not fields1:
            return False

    spec2   = query2.get('spec', {})

    if  spec2 == {}: # empty conditions for existing query, look at sel. fields
        if  set(fields2) > set(fields1): # set2 is superset of set1
            return True

    # check spec keys, since they applied to data-srv APIs do not
    # allow their comparison for different set of keys.
    if  spec2.keys() != spec1.keys():
        return False

    for key, val1 in spec1.items():
        if  key in spec2:
            val2 = spec2[key]
            if  (isinstance(val1, str) or isinstance(val1, unicode)) and \
                (isinstance(val2, str) or isinstance(val2, unicode)):
                if  not compare_str(val1, val2):
                    return False
            elif  type(val1) != type(val2) and not isinstance(val1, dict)\
                and not isinstance(val2, dict):
                return False
            elif isinstance(val1, dict) and isinstance(val2, dict):
                if  not compare_dicts(val1, val2):
                    return False
            elif isinstance(val2, dict) and isinstance(val1, int):
                if  val1 in val2.values():
                    return True
                return False
            else:
                if  val1 != val2:
                    return False
    return True

