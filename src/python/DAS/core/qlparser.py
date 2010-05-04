#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS lexer/parser/analyzer. DAS operates with input query. It should be
mapped into set of sub-queries executed by registered in DAS data-services.
Lexer:  holds operators, keys, provide basic functions to extract selection
        keys, constrains, etc.
Parser: check input query, construct set of unique keys to select,
        construct unique set of data-services to be used. Finally it
        analyzes input query and construct data-service specific sub-
        queries (analyzer).

Analyzer algorithm:
===================
1. get list of all keys in a query
   a. all keys = selection keys, where clause keys and order by keys
2. map them to list of data-services
   a. leave only those services represented in selection keys
3. walk through conditions
   a. if data-service for condition key is not in a list add it
   b. construct dictionary of sub-query conditions
4. perform one-way permutation of data-service from found list,
which provides a list of relation keys
5. construct unique set of keys = selection keys + releation keys
6. update sub-query dictionary with sub-queries based on
find ulist where <cond>
where condition being already present in that ditionary, see step 3b.
7. prevent bracket-objects (a set of conditions embraced with brackets)
from being use more then one data-service. Example:
(dataset=bla or hlt=ok) is prohibitted, since dataset (DBS) and hlt (RunSum)
while dataset=bla or hlt=ok is allowed.

Output:
=======
The lexer/parser/analyzer output provides everything required by DAS to
execute sub-queries. The "daslist" (see below for example) contains a list
of dictionaries whose keys are data-service and values are sub-queries.
The list is constructed based on input query and boolean operators. For
"OR" operator the entries in daslist are executed separately by DAS and
appended to final results. While for "AND" operation a single entry
in daslist is kept. Results from data-service in this case are multiplexed
by DAS to create final result.

General example:
================
find intlumi,dataset where site=T2_UK or hlt=OK
{ 
 'order_by_list': [], 
 'selkeys': ['dataset', 'intlumi'], 
 'unique_services': ['dbs', 'lumidb', 'runsum'], 
 'order_by_order': None, 
 'services': {'sitedb': ['site'], 'dbs': ['dataset', 'site'], 
              'lumidb': ['intlumi'], 'phedex': ['site'], 'runsum': ['hlt']}, 
 'daslist': [{'dbs': 'find dataset,intlumi,run,lumi where site = T2_UK', 
              'lumidb': 'find dataset,intlumi,run,lumi', 
              'runsum': 'find dataset,intlumi,run,lumi'}, 
             {'dbs': 'find dataset,intlumi,run,lumi', 
              'lumidb': 'find dataset,intlumi,run,lumi', 
              'runsum': 'find dataset,intlumi,run,lumi where hlt = OK'}], 
 'conditions': [{'value': 'T2_UK', 'key': 'site', 'op': '='}, 
                 'or', 
                {'value': 'OK', 'key': 'hlt', 'op': '='}], 
 'allkeys': ['dataset', 'hlt', 'intlumi', 'site'], 
 'unique_keys': ['dataset', 'intlumi', 'lumi', 'run']}
"""

__revision__ = "$Id: qlparser.py,v 1.10 2009/06/12 14:48:56 valya Exp $"
__version__ = "$Revision: 1.10 $"
__author__ = "Valentin Kuznetsov"

import re
import types
from itertools import groupby
from DAS.utils.utils import oneway_permutations, unique_list

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
    parserobj = Wrapper()
    tokens = parserobj.parseQuery(uinput)
    return tokens

def mergecond(icond):
    """
    For given condition list coming from QL parser create condition
    expression. The icond can contain brackets, and/or and dict of
    key op value.
    """
    cond = ""
    for item in icond:
        if  type(item) is types.StringType:
            cond += ' %s ' % item
        if  type(item) is types.DictType:
            cond += ' %s %s %s ' % (item['key'], item['op'], item['value'])
    return cond

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
            cond, oper, rest = getnextcond(substr)
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
        
class QLLexer(object):
    """
    DAS QL lexer. It holds DAS operators, reserved keywords and data-service
    maps. Its task to tokenize input query into set of
    - selection keys
    - order_by keys
    - create condition dictionaries, {'value':'bla', 'op':'=', 'key':'run'}
    """
    def __init__(self, imap):
        self.prefix    = ['find ', 'plot ', 'view ']
        self.operators = ['!=', '<=', '<', '>=', '>', '=', 
                          ' not like ', ' like ', 
                          ' between ', ' not in ', ' in ']
        self.qlmap = imap #{'dbs': [list of keys], ...}
        self.known_keys = [k for i in self.qlmap.values() for k in i]

    def fix_reserved_keywords(self, query):
        """
        Lowering all reserved keywords in a query
        """
        for word in self.prefix + self.operators + [' and ', ' or ']:
            query = query.replace(str(word).upper(), word)
        return query
        
    def findservices(self, key):
        """
        For given key find a list of corresponding data-services
        """
        for srv, keys in self.qlmap.items():
            if  key in keys:
                yield srv
        
    def condkeys(self, exp):
        """
        Function to find out keys used in condition expression
        Please note condition expression can be embrase in brackets,
        and we assume there is no spaces between key operator value.
        """
        if  not exp:
            return
        conditions = exp.replace('(', '').replace(')', '')
#        conditions = conditions.replace(' and ', '').replace(' or ', '')
#        for cond in conditions.split():
#            cdict = self.make_cond_dict(cond) 
#            print "cdict", cond, cdict
#            yield cdict['key']
        substr = conditions
        while 1:
            cond, oper, rest = getnextcond(substr)
            substr = rest
            if  cond:
                condition = cond
            else:
                condition = rest
            cdict = self.make_cond_dict(condition) 
            yield cdict['key']
            if  not cond:
                break
            
            
    def make_cond_dict(self, exp):
        """Output of provided expression make a dict key op value"""

        # find operator whose position is closest to key
        olist = []
        for oper in self.operators:
            pos = exp.find(oper)
            if  pos != -1:
                olist.append((pos, oper))
        olist.sort()
        if  not olist:
            return
        oper = olist[0][-1]

        # split expression into key op value and analyze the value
        key, value = exp.split(oper, 1)
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
        if  type(value) is types.StringType:
            value = value.strip()
        return {'key':key.strip(), 'op':oper.strip(), 'value':value}

    def selkeys(self, query):
        """return list of selection keys in a query"""
        query = self.fix_reserved_keywords(query)
        uinput = query.strip()
        if  uinput.find(' where ') != -1:
            uinput = uinput.split(' where')[0]
        elif uinput.find(' order by ') != -1:
            uinput = uinput.split(' order by ')[0]
        for prx in self.prefix:
            uinput = uinput.replace(prx, '')
        return [x.strip() for x in uinput.split(',')]

    def conditions(self, query):
        """return a list of conditions"""
        query = self.fix_reserved_keywords(query)
        query = query.strip()
        cond  = self.condition_parser(query)
        lbr   = 0
        rbr   = 0
        for i in cond:
            if  i == '(':
                lbr += 1
            if  i == ')':
                rbr += 1
        if  lbr != rbr:
            msg  =  "Unequal number of ( and ) brackets\n"
            raise Exception(msg)
        return cond

    def allkeys(self, query):
        """return list of selection and conditions keys"""
        query = self.fix_reserved_keywords(query)
        query = query.strip()
        olist = self.selkeys(query)
        for item in self.conditions(query):
            if  type(item) is types.DictType:
                key = item['key'].strip()
                if  key not in olist:
                    olist.append(key)
        order_by_list, order_by = self.order_by(query)
        olist = olist + order_by_list
        return olist

    def order_by(self, query):
        """
        Check if query contains order by expression and return
        order_by keys and ordering
        """
        query = self.fix_reserved_keywords(query)
        order_by_list = []
        order_by = None
        query = query.strip().lower()
        if  query.find(' order by ') != -1:
            uinput = query.lower().split(' order by ')[-1]
            qlist  = uinput.split()
            if  qlist[-1] == 'asc':
                order_by = 'asc'
                order_by_list = ''.join(qlist[:-1]).split(',')
            elif qlist[-1] == 'desc':
                order_by = 'desc'
                order_by_list = ''.join(qlist[:-1]).split(',')
            else:
                order_by_list = ''.join(qlist).split(',')
        return order_by_list, order_by

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

class QLParser(QLLexer):
    """
    DAS QL parser class. It is responsible for
    - check validity of the query and all keywords
    - find selection, order_by, all keys
    - construct unique set of services to be used for query
    - construct unique set of keys to be retrieved for query
    """
    def __init__(self, imap):
        QLLexer.__init__(self, imap)

    def check(self, query, rdict):
        """
        Check and analyze input query and return a bundle of
        selkeys, allkeys, conditions, orderby.
        """
        query = self.fix_reserved_keywords(query)
#        rdict = self.params(query)
        query = query.strip()
        # check brackets
        lbr = query.count('(')
        rbr = query.count(')')
        if  lbr != rbr:
            msg =  "Unequal number of ( and ) brackets"
            raise Exception(msg)
        # check presence of correct action
        first_word = query.split()[0] + ' '
        if  first_word not in self.prefix:
            msg = "Unsupported keyword '%s'" % first_word
            raise Exception(msg)
        # check presence of where
        if  query.find(' where') != -1:
            last_word = query.split(',')[-1]
            if  last_word not in self.known_keys:
                msg = "Unsupported keyword '%s'" % last_word
                Exception(msg)
        # check presence of order by
        cond = rdict['conditions']
        if  cond and (cond[-1] == 'and' or cond[-1] == 'or'):
            msg = "Unbounded boolean expression at the end of query"
            raise Exception(msg)
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

    def params(self, query):
        """
        Parse input query and create output param dict with:
        - selection keys
        - order by keys
        - conditions dict
        - all keys = selection keys + condition keys + order by keys
        - services
        - unique set of keys and service necessary to run a query
        """
        query = self.fix_reserved_keywords(query)
        rdict = {}
        rdict['conditions']      = self.conditions(query)
        order_by_list, order_by  = self.order_by(query)
        rdict['order_by_list']   = order_by_list
        rdict['order_by_order']  = order_by
        rdict['selkeys']         = unique_list(self.selkeys(query))
        rdict['allkeys']         = unique_list(self.allkeys(query))
        rdict['services']        = self.services(query)
        services, ulist, daslist = self.uniq_services(query)
        rdict['unique_services'] = unique_list(services)
        if  len(services) == 1:
            rdict['unique_keys'] = rdict['selkeys']
        else:
            rdict['unique_keys'] = unique_list(ulist)
        rdict['daslist']         = daslist
        self.check(query, rdict)
        return rdict
        
    def services(self, query):
        """
        Look-up all services whose keys match the query
        """
        query = self.fix_reserved_keywords(query)
        sdict = {}
        akeys = unique_list(self.allkeys(query))
        for key in akeys:
            for service, keys in self.qlmap.items():
                if  key in keys:
                    if  sdict.has_key(service):
                        vlist = sdict[service]
                        sdict[service] = vlist +[key]
                    else:
                        sdict[service] = [key]
        return sdict

    def uniq_services(self, query):
        """
        Look-up unique data-service and keys required to perform the query.
        For instance if user provide
        find dataset, block
        the 'block' is common key between DBS and phedex, but DBS
        can answer this query without phedex
        """
        query = self.fix_reserved_keywords(query)
        skeys = unique_list(self.selkeys(query))
        akeys = unique_list(self.allkeys(query))
        oneservice = None
        for srv, keys in self.qlmap.items():
            if  set(akeys) & set(keys) == set(akeys):
                oneservice = srv
                break
        if  oneservice: # all keys from one data-service
            uservices = [oneservice]
            ulist = skeys
            daslist = [{oneservice:query}]
            return uservices, ulist, daslist
        else:
            uservices = [] # unique set of services for list of keys
#            for key in skeys:
            for key in akeys:
                srvs = [i for i in self.findservices(key)]
                if  not uservices:
                    uservices = [i for i in srvs]
                else:
                    int_srvs = set(srvs) & set(uservices)
                    if  not int_srvs:
                        uservices += [i for i in srvs]
        cond_exp = mergecond(self.conditions(query))
        bckdict  = {}
        counter  = 0
        while 1:
            bckobj = findbracketobj(cond_exp)
            if  not bckobj:
                break
            bidx = 'bobj_%s' % counter
            bckdict[bidx] = bckobj
            counter += 1
            cond_exp = cond_exp.replace(bckobj, bidx)

        substr = cond_exp
        final  = ''
        idx    = 0
        conddict = {}
        while 1:
            cond, oper, rest = getnextcond(substr)
            substr = rest
            if  cond:
                condition = cond
            else:
                condition = rest
            # need to walk through all conditions inside of brackets
            # and get all keys
            bracket_found = 0
            if  condition.find('bobj_') != -1:
                condition = bckdict[condition]
                bracket_found = 1
            cond_keys = [i for i in self.condkeys(condition)]
            cond_srvs = []
            for key in cond_keys:
                srvs = [i for i in self.findservices(key)]
                int_srvs = set(srvs) & set(uservices)
                if  not int_srvs:
                    uservices += [i for i in srvs if i not in uservices]
                    cond_srvs = list(srvs)
                else:
#                    cond_srvs = list(int_srvs)
                    cond_srvs += list(int_srvs)
            # check if we have more then 1 data-srv for bracket obj
            if  bracket_found:
                if  len(cond_srvs) > 1:
                    msg  = "Unsupported conditions: mix of conditions from more"
                    msg += " then on data-service inside of brackets"
                    raise Exception(msg)
            srvkey = 'srv%s' % idx
            conddict[srvkey] = condition, cond_srvs
            final += ' ' + srvkey
            if  oper:
                final += ' ' + oper
            if  not cond:
                break
            idx += 1
        # find relation keys in uservices
        pairs = [i for i in oneway_permutations(uservices)]
        ulist = list(skeys)
        for pair in pairs:
            list0 = self.qlmap[pair[0]]
            list1 = self.qlmap[pair[1]]
            ulist += [i for i in list(set(list0) & set(list1)) if i not in ulist\
                        and i.find('.') == -1]
        # find final set of sub-queries to be executed by DAS
        ustr = ','.join(ulist)
        daslist = []
        for exp in final.split(' or '):
            dasqueries = {}
            for item in exp.split(' and '):
                cond, srvlist = conddict[item.strip()]
                subquery  = 'find %s where %s' % (ustr, cond)
                for srv in srvlist:
                    if  dasqueries.has_key(srv):
                        dasqueries[srv] = dasqueries[srv] + ' and ' + cond
                    else:
                        dasqueries[srv] = subquery
            for srv in list(set(uservices) - set(dasqueries.keys())):
                dasqueries[srv] = 'find %s' % ustr
            daslist.append(dasqueries)
#        print "query", query
#        print "uservices", uservices
#        print "cond_exp", cond_exp
#        print "ulist", ulist
#        print "final", final
#        print "conddict", conddict
#        print "daslist", daslist
        return uservices, ulist, daslist

