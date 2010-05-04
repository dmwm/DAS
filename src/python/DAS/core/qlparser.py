#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS Query Language code. It consists of MongoParser who communicate with
DAS Analytics and DAS Mapping DBs to fetch registered DAS keys and store
DAS-QL to MongoDB-QL conversion. We use several helper functions to
tests integrity of DAS-QL queries, conversion routine from DAS-QL
syntax to MongoDB one.
"""

__revision__ = "$Id: qlparser.py,v 1.20 2009/10/02 19:03:14 valya Exp $"
__version__ = "$Revision: 1.20 $"
__author__ = "Valentin Kuznetsov"

import re
import time
import types
import traceback

from itertools import groupby
from DAS.utils.utils import oneway_permutations, unique_list, add2dict
from DAS.utils.utils import getarg

DAS_OPERATORS = ['!=', '<=', '<', '>=', '>', '=', 
                 ' not like ', ' like ', 
                 ' between ', ' not in ', ' in ', ' last ']
MONGO_MAP = {
    '>':'$gt',
    '<':'$lt',
    '>=':'$gte',
    '<=':'$lte',
    'in':'$in',
    '!=':'$ne',
    'not in':'$nin',
}

def mongo_exp(cond_list, lookup=False):
    """
    Convert DAS expression into MongoDB syntax. As input we take
    a dictionary of key, operator and value.
    """
    mongo_dict = {}
    for cond in cond_list:
        if  cond == '(' or cond == ')' or cond == 'and':
            continue
        key  = cond['key']
        val  = cond['value']
        oper = cond['op']
        if  type(val) is types.StringType and val.find('%') != -1:
            val = val.replace('%', '*')
        if  mongo_dict.has_key(key):
            existing_value = mongo_dict[key]
            if  type(existing_value) is types.DictType:
                val = existing_value['$all'] + [val]
            else:
                val = [existing_value, val]
            mongo_dict[key] = {'$all' : val}
        else:
            if  MONGO_MAP.has_key(oper):
                if  mongo_dict.has_key(key):
                    mongo_value = mongo_dict[key]
                    mongo_value[MONGO_MAP[oper]] = val
                    mongo_dict[key] = mongo_value
                else:
                    mongo_dict[key] = {MONGO_MAP[oper] : val}
            elif oper == 'like':
                if  lookup:
                    # for expressions: *val* use pattern .*val.*
                    pat = re.compile(val.replace('*', '.*'))
                    mongo_dict[key] = pat
                else:
                    mongo_dict[key] = val
            elif oper == 'not like':
                # TODO, reverse the following:
                msg = 'Operator not like is not supported yet'
                raise Exception(msg)
                # for expressions: *val* use pattern .*val.*
#                pat = re.compile(val.replace('*', '.*'))
#                mongo_dict[key] = pat
            elif oper == '=':
                mongo_dict[key] = val
            elif oper == 'between':
                mongo_dict[key] = {'$in' : [i for i in range(val[0], val[1])]}
            elif oper == 'last':
                mongo_dict[key] = 'last operator'
            else:
                msg = 'Not supported operator %s' % oper
                raise Exception(msg)
    return mongo_dict

def find_index(qlist, tag):
    """Find index of tag in a list qlist"""
    try:
        return qlist.index(tag)
    except:
        return -1
def getnextcond(uinput):
    """
    Find out next where clause condition. It only understand conditions
    between and and or boolean operators
    """
    obj_and = 'and'
    obj_or  = 'or'
    qlist   = [name.strip() for name, group in groupby(uinput.split())]
    idx_and = find_index(qlist, 'and')
    idx_or  = find_index(qlist, 'or')
    idx_between = find_index(qlist, 'between')
    if  idx_and - idx_between == 2 and idx_between != -1:
        idx_and = find_index(qlist[idx_and+1:], 'and')
    if  idx_or  - idx_between == 2 and idx_between != -1:
        idx_or  = find_index(qlist[idx_or+1:], 'or')
    res = None, None, None
    if  idx_and == -1 and idx_or == -1:
        res = None, None, uinput
    if  idx_and != -1 and idx_or != -1:
        if  idx_and < idx_or:
            res = ' '.join(qlist[0:idx_and]), obj_and, \
                  ' '.join(qlist[idx_and+1:len(qlist)])
        else:
            res = ' '.join(qlist[0:idx_or]), obj_or, \
                  ' '.join(qlist[idx_or+1:len(qlist)])
    if  idx_and != -1 and idx_or == -1:
        res = ' '.join(qlist[0:idx_and]), obj_and, \
              ' '.join(qlist[idx_and+1:len(qlist)])
    if  idx_and == -1 and idx_or != -1:
        res = ' '.join(qlist[0:idx_or]), obj_or, \
              ' '.join(qlist[idx_or+1:len(qlist)])
    return res

def getconditions(uinput):
    """
    Find out where clause conditions and store them in output dictionary
    """
    sublist = uinput.split(' where ')
    rdict   = {}
    if  len(sublist)>1:
        substr = ' '.join(sublist[1:])
        counter = 0
        while 1:
            cond, oper, rest = getnextcond(substr)
            substr = rest
            if  not cond:
                break
            rdict['q%s' % counter] = cond
            counter += 1
        if  substr:
            rdict['q%s' % counter] = substr
    return rdict

def findbracketobj(uinput, dbs_func=True):
    """
    Find out bracket object, e.g. ((test or test) or test)
    """
    if  dbs_func:
        if  uinput.find('count(') != -1 or uinput.find('sum(') != -1:
            return
    left = uinput.find('(')
    robj = ''
    if  left != -1:
        rcount = 0
        for char in uinput[left:]:
            robj += char
            if  char == '(':
                rcount += 1
            if  char == ')':
                rcount -= 1
            if  char == ')' and not rcount:
                break
        if  rcount:
            msg = "Object '%s' has un-equal number of left & right brackets" \
                % uinput
            raise Exception(msg)
    return robj
        
class MongoParser(object):
    """
    DAS Mongo query parser. 
    """
    def __init__(self, config):
        self.map = config['dasmapping']
        self.analytics = config['dasanalytics']
        self.daskeys = self.map.daskeys()
        self.operators = DAS_OPERATORS

    def decompose(self, query):
        """Extract selection keys and conditions from input Mongo query"""
        skeys = getarg(query, 'fields', [])
        cond  = getarg(query, 'spec', {})
        return skeys, cond

    def services(self, query):
        """Find out DAS services to use for provided query"""
        skeys, cond = self.decompose(query)
        if  type(skeys) is types.StringType:
            skeys = [skeys]
        sdict = {}
        # look-up services from Mapping DB
        for key in skeys + cond.keys():
            for service, keys in self.daskeys.items():
                if  key in keys:
                    if  sdict.has_key(service):
                        vlist = sdict[service]
                        if  key not in vlist:
                            sdict[service] = vlist + [key]
                    else:
                        sdict[service] = [key]
        return sdict

    def params(self, query):
        """
        Return dictionary of parameters to be used in DAS Core:
        selection keys, conditions and services.
        """
        skeys, cond = self.decompose(query)
        return dict(selkeys=skeys, conditions=cond, services=self.services(query))

    def dasql2mongo(self, query, lookup=False):
        """
        Convert DAS QL expression into Mongo query. It can map into several Mongo
        queries. We will return a dict of {system:mongo_query}.
        """
        findbracketobj(query) # check brackets in a query
        wsplit = query.split(' where ')
        skeys  = wsplit[0].replace('find ', '').strip().split(',')
        cond   = {}
        if  len(wsplit) == 2:
            condlist = []
            for item in self.condition_parser(query):
                if  type(item) is types.DictType:
                    key = item['key']
                    oper = item['op']
                    value = item['value']
                    for system in self.map.list_systems():
                        try:
                            lkeys = self.map.lookup_keys(system, key, 
                                                         value=value)
                            for nkey in lkeys:
                                if  nkey != 'date':
                                    cdict = dict(key=nkey, op=oper, value=value)
                                    if  cdict not in condlist:
                                        condlist.append(cdict)
                        except:
                            pass
                elif  type(item) is types.StringType and item.strip() == 'or':
                    msg  = "\nDAS currently do not support OR operator."
                    msg += "\nAny OR'ing operation can be split into multiple DAS queries,"
                    msg += "\nfind a,b,c where x=1 or y=2 is equivalent to 2 queries:\n"
                    msg += "\nfind a,b,c where x=1"
                    msg += "\nand"
                    msg += "\nfind a,b,c where y=1"
                    raise Exception(msg)
            cond = mongo_exp(condlist, lookup)
            mongo_query = dict(spec=cond, fields=skeys + ['das'])
            self.analytics.add_query(query, mongo_query)
            return mongo_query
        msg = "\nUnable to convert input DAS-QL expression '%s' into MongoDB query"\
                % query
        raise Exception(msg)

    def condition_parser(self, uinput):
        """Parse condition in given query"""
        uinput  = uinput.split(' order by ')[0]
        sublist = uinput.split(' where ')
        olist = []

        ########## internal helper function
        def add_to_list(cond, olist):
            """Helper function to add condition to the list"""
            if  not cond:
                return
            cond_dict = self.make_cond_dict(cond)
            tot_lb = 0
            tot_rb = 0
            for key, val in cond_dict.items():
                lbr = str(val).count('(')
                rbr = str(val).count(')')
                if  lbr: 
                    val = val.replace('(', '')
                    cond_dict[key] = val
                if  rbr: 
                    val = val.replace(')', '')
                    cond_dict[key] = val
                tot_lb += lbr
                tot_rb += rbr
            if  tot_lb:
                for i in range(0, tot_lb):
                    olist.append('(')
            olist.append(cond_dict)
            if  tot_rb:
                for i in range(0, tot_rb):
                    olist.append(')')
        ########## end of internal function

        if  len(sublist)>1:
            substr = ' '.join(sublist[1:])
            while 1:
                cond, oper, rest = getnextcond(substr)
                substr = rest
                # parse cond to extract brackets and make triples (key, op, val)
                add_to_list(cond, olist)
                if  oper:
                    olist.append(oper.strip())
                if  not cond:
                    break
            add_to_list(rest, olist)
        return olist

    def make_cond_dict(self, exp):
        """Output of provided expression make a dict key op value"""

        # find operator whose position is closest to key
        olist = []
        for oper in self.operators:
            pos = exp.find(oper)
            if  pos != -1:
                olist.append((pos, oper))
#                break
        olist.sort()
        # now we have list of position/operators pairs, let's find out
        # which operator has least preferences, e.g we got expression
        # a=A=2&B<=2, in this case we find position for '=', '<' and '<='
        # so we must distinguish case of '<' and '<='
        nlist = []
        for item in olist:
            if  not nlist:
                nlist.append(item)
            else:
                idx0  = nlist[0][0]
                oper0 = nlist[0][1]
                idx1  = item[0]
                oper1 = item[1]
                if  idx1 == idx0: # find out which oper has least preference
                    oper1_pos = self.operators.index(oper1)
                    oper0_pos = self.operators.index(oper0)
                    if  oper1_pos < oper0_pos:
                        nlist[0] = item
        if  not olist:
            return
        oper = nlist[0][-1]

        # split expression into key op value and analyze the value
        key, value = exp.split(oper, 1)
        if  key.strip() == 'system':
            if  value.strip() == 'all':
                value = self.qlmap.keys()
        if  key == 'date': # convert date into seconds since epoch
            if  oper != ' last ': # we already converted date
                pat = re.compile('[0-2]0[0-9][0-9][0-1][0-9][0-3][0-9]')
                if  pat.match(value): # we accept YYYYMMDD
                    d = datetime.date(int(value[0:4]), # YYYY
                                      int(value[4:6]), # MM
                                      int(value[6:8])) # DD
                    value = time.mktime(d.timetuple())
                else:
                    msg = 'Unacceptable date format'
                    raise Exception(msg)
        if  oper == ' in ' or oper == ' not in ':
            value = value.strip()
            if  value[0] != '(' and value[-1] != ')':
                msg = "Value for '%s' operators not enclosed with brackets"\
                    % oper 
                raise Exception(msg)
            value = [i.strip() for i in \
                    value.replace('(', '').replace(')', '').split(',')]
        if  oper == ' between ':
            msg = "Unsupported syntax for value of between operator"
            try:
                value = value.split('and')
                if  len(value) != 2:
                    raise Exception(msg)
            except:
                raise Exception(msg)
            value = [i.strip() for i in value]
        if  oper == ' last ':
            msg = "Unsupported syntax for value of between operator"
            pat = re.compile('^[0-9][0-9](h|m)$')
            if  not pat.match(value):
                raise Exception(msg)
            oper = ' = '
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
            value = [date1, date2]
        if  type(value) is types.StringType:
            value = value.strip()
            if  value and value[0]=='[' and value[-1]==']':
                value = eval(value)
        return {'key':key.strip(), 'op':oper.strip(), 'value':value}

    def lookupquery(self, system, query):
        """
        In order to look-up records in cache DB, we need to loose
        our constraints on input query. We look-up spec part of the query
        and loose all constrains over there,
        see MongoDB API, http://api.mongodb.org/python/
        """
        spec    = getarg(query, 'spec', {})
        fields  = getarg(query, 'fields', None)
        newspec = {}
        for key, val in spec.items():
            ksplit = key.split('.') # split key into entity.attribute
            if  ksplit[0] not in self.daskeys[system]:
                continue
            if  type(val) is types.StringType or type(val) is types.UnicodeType:
                val = re.compile('.*%s.*' % val)
            newspec[key] = val
        newspec['das.system'] = system
        return dict(spec=newspec, fields=fields)

