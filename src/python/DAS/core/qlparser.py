#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Query parser for DAS
"""
__revision__ = "$Id: qlparser.py,v 1.4 2009/05/07 00:58:23 valya Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "Valentin Kuznetsov"

import types
from itertools import groupby

def antrlparser(uinput):
    """
    Parser based on ANTRL. It returns the following dict for
    query = find dataset, run where site = T2
    {'ORDERING': [], 
     'FIND_KEYWORDS': ['dataset', 'run'], 
     'ORDER_BY_KEYWORDS': [], 
     'WHERE_CONSTRAINTS': [{'value': 'T2', 'key': 'site', 'op': '='}, {'bracket': 'T2'}]}
    """
    from Wrapper import Wrapper
    parserObj = Wrapper()
    tokens = parserObj.parseQuery(uinput)
    return tokens

def dasqlparser(uinput):
    """
    Main routine which does the parsing of input user query
    it creates an output dictionary with selection list, list of queries
    for execution by DAS service and condition list. Here is an example 
    {'condlist': {
       'q1': 'admin=VK', 
       'q0': 'site = T2', 
       'q2': 'storage=castor'}, 
     'input': 'find dataset, run, bfield where site = T2 and admin=VK and storage=castor', 
     'queries': {
       'q1': 'find dataset,run,bfield where admin=VK', 
       'q0': 'find dataset,run,bfield where site = T2', 
       'q2': 'find dataset,run,bfield where storage=castor'}, 
     'sellist': ['dataset', 'run', 'bfield'], 
     'query': 'find dataset, run, bfield where q0 and q1 and q2'}

    """
    uinput = uinput.strip()
    rdict  = {}
    rdict['input'] = uinput

    bckdict  = {}
    counter  = 0
    while 1:
        bckobj = findbracketobj(uinput)
        if  not bckobj:
            break
        bidx = 'bobj_%s' % counter
        bckdict[bidx] = bckobj
        counter += 1
        uinput = uinput.replace(bckobj, bidx)
    
    sellist  = getselectkeys(uinput)
    condlist = getconditions(uinput) 
    query    = uinput
    queries  = {}
    for key, val in condlist.items():
        query   = query.replace(val, key)
        if  val.find('bobj_') != -1:
            val = bckdict[val]
        queries[key] = 'find %s where %s' % (','.join(sellist), val) 
#    rdict['bckobj'] = bckdict
    rdict['query'] = query
    rdict['sellist'] = sellist
    rdict['condlist'] = condlist
    rdict['queries'] = queries
    return rdict
    

def getselectkeys(uinput):
    "Extract selection keys from provided user input query"
    uinput = uinput.split(' where')[0]
    uinput = uinput.replace('find ', '').replace('plot ', '')
    return [x.strip() for x in uinput.split(',')]
        
def find_index(qlist, tag):
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
    idx_and = uinput.find(obj_and)
    idx_or  = uinput.find(obj_or)
    qlist = [name.strip() for name, group in groupby(uinput.split())]
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
    
#    obj_and = ' and '
#    obj_or  = ' or '
#    idx_and = uinput.find(obj_and)
#    idx_or  = uinput.find(obj_or)
#    if  idx_and == -1 and idx_or == -1:
#        return None, None, uinput
#    if  idx_and != -1 and idx_or != -1:
#        if  idx_and < idx_or:
#            return uinput[0:idx_and], obj_and, uinput[idx_and+len(obj_and):]
#        else:
#            return uinput[0:idx_or], obj_or, uinput[idx_or+len(obj_or):]
#    if  idx_and != -1 and idx_or == -1:
#        return uinput[0:idx_and], obj_and, uinput[idx_and+len(obj_and):]
#    if  idx_and == -1 and idx_or != -1:
#        return uinput[0:idx_or], obj_or, uinput[idx_or+len(obj_or):]

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
            cond, op, rest = getnextcond(substr)
            substr = rest
            if  not cond:
                break
            rdict['q%s' % counter] = cond
            counter += 1
        if  substr:
            rdict['q%s' % counter] = substr
    return rdict

def findbracketobj(uinput):
    """
    Find out bracket object, e.g. ((test or test) or test)
    """
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
        
class QLParser(object):
    def __init__(self, imap):
        self.prefix    = ['find', 'plot', 'view']
        self.operators = ['!=', '<=', '<', '>=', '>', '=', 
                          ' not like ', ' like ', 
                          ' between ', ' not in ', ' in ']
        self.qlmap = imap #{'dbs': [list of keys], ...}
        self.known_keys = [k for i in self.qlmap.values() for k in i]

    def make_cond_dict(self, exp):
        """Output of provided expression make a dict key op value"""

        # find operator whose position is closest to key
        olist = []
        for op in self.operators:
            pos = exp.find(op)
            if  pos != -1:
                olist.append((pos, op))
        olist.sort()
        if  not olist:
            return
        op = olist[0][-1]

        # split expression into key op value and analyze the value
        key, value = exp.split(op, 1)
        if  op == ' in ' or op == ' not in ':
            value = value.strip()
            if  value[0] != '(' and value[-1] != ')':
                msg = "Value for '%s' operators not enclosed with brackets"\
                    % op 
                raise Exception(msg)
            value = [i.strip() for i in \
                    value.replace('(', '').replace(')', '').split(',')]
        if  op == ' between ':
            msg = "Unsupported syntax for value of between operator"
            try:
                value = value.split('and')
                if  len(value) != 2:
                    raise Exception(msg)
            except:
                raise Exception(msg)
            value = [i.strip() for i in value]
        if  type(value) is types.StringType:
            value = value.strip()
        return {'key':key.strip(), 'op':op.strip(), 'value':value}

    def condition_parser(self, uinput):
        """Parse condition in given query"""
        uinput  = uinput.split(' order by ')[0]
        sublist = uinput.split(' where ')
        rdict   = {}
        olist = []

        ########## internal helper function
        def add_to_list(cond, olist):
            if  not cond:
                return
            cond_dict = self.make_cond_dict(cond)
            tot_lb = 0
            tot_rb = 0
            newcond = {}
            for k, v in cond_dict.items():
                lb = str(v).count('(')
                rb = str(v).count(')')
                if  lb: 
                    v = v.replace('(', '')
                    cond_dict[k] = v
                if  rb: 
                    v = v.replace(')', '')
                    cond_dict[k] = v
                tot_lb += lb
                tot_rb += rb
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
                cond, op, rest = getnextcond(substr)
                substr = rest
                # parse cond to extract brackets and make triples (key, op, val)
                add_to_list(cond, olist)
                if  op:
                    olist.append(op.strip())
                if  not cond:
                    break
            add_to_list(rest, olist)
        return olist

    def params(self, query):
        """
        Check and analyze input query and return a bundle of
        selkeys, allkeys, conditions, orderby.
        """
        rdict = {} # return dict
        query = query.strip()
        # check brackets
        lb = query.count('(')
        rb = query.count(')')
        if  lb != rb:
            msg =  "Unequal number of ( and ) brackets"
            raise Exception(msg)
        # check presence of correct action
        first_word = query.split()[0]
        if  first_word not in self.prefix:
            msg = "Unsupported keyword '%s'" % last_word
            raise Exception(msg)
        # check presence of where
        if  query.find(' where') != -1:
            last_word = query.split(',')[-1]
            if  last_word not in self.known_keys:
                msg = "Unsupported keyword '%s'" % last_word
                Exception(msg)
        # check presence of order by
        cond = self.conditions(query)
        rdict['conditions'] = cond
        if  cond and (cond[-1] == 'and' or cond[-1] == 'or'):
            msg = "Unbounded boolean expression at the end of query"
            raise Exception(msg)
#        if  cond:
#            val  = cond[-1]['value']
#            if  type(val) is types.StringType: 
#                vlist = val.split(' ')
#                if  len(vlist) > 1:
#                    msg = "Unsupported keyword '%s'" % vlist[1]
#                    raise Exception(msg)
        # check order by value
        order_by_list, order_by = self.order_by(query)
        rdict['order_by_list'] = order_by_list
        rdict['order_by_order'] = order_by
        rdict['selkeys'] = self.selkeys(query)
        rdict['allkeys'] = self.allkeys(query)
        # check if all keys are valid ones
        for k in rdict['allkeys']:
            if  k not in self.known_keys:
                if  k.find('count') !=-1 or k.find('sum') != -1: # DBS
                    k = ''.join(k.split('(')[-1]).split(')')[0]
                    if  k not in self.known_keys:
                        msg = "Unsupported key '%s'" % k
                        raise Exception(msg)
                else:
                    msg = "Unsupported key '%s'" % k
                    raise Exception(msg)
        return rdict
        
    def selkeys(self, query):
        """return list of selection keys in a query"""
        uinput = query.strip().lower()
        if  uinput.find(' where ') != -1:
            uinput = uinput.split(' where')[0]
        elif uinput.find(' order by ') != -1:
            uinput = uinput.split(' order by ')[0]
        for pr in self.prefix:
            uinput = uinput.replace(pr, '')
        return [x.strip() for x in uinput.split(',')]

    def conditions(self, query):
        """return a list of conditions"""
        query = query.strip().lower()
        cond  = self.condition_parser(query)
        lb = 0
        rb = 0
        for i in cond:
            if  i == '(':
                lb +=1
            if  i == ')':
                rb +=1
        if  lb != rb:
            msg =  "Unequal number of ( and ) brackets\n"
            raise Exception(msg)
        return cond

    def allkeys(self, query):
        """return list of selection and conditions keys"""
        query = query.strip().lower()
        olist = self.selkeys(query)
        for item in self.conditions(query):
            if  type(item) is types.DictType:
                olist.append(item['key'])
        order_by_list, order_by = self.order_by(query)
        olist = olist + order_by_list
        olist.sort()
        return [name.strip() for name, group in groupby(olist)]

    def order_by(self, query):
        """return sort order"""
        order_by_list = []
        order_by = None
        query = query.strip().lower()
        if  query.find(' order by ') != -1:
            uinput = query.lower().split(' order by ')[-1]
            qlist  = uinput.split()
            if  qlist[-1] == 'asc':
                order_by = 'asc'
                order_by_list = ' '.join(qlist[:-1]).split(',')
            elif qlist[-1] == 'desc':
                order_by = 'desc'
                order_by_list = ' '.join(qlist[:-1]).split(',')
            else:
                order_by_list = ' '.join(qlist).split(',')
        return order_by_list, order_by
