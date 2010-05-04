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

__revision__ = "$Id: qlparser.py,v 1.14 2009/08/07 03:21:41 valya Exp $"
__version__ = "$Revision: 1.14 $"
__author__ = "Valentin Kuznetsov"

import re
import time
import types
from itertools import groupby
from DAS.utils.utils import oneway_permutations, unique_list, add2dict

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

def mongo_exp(cond_list):
    """
    Convert DAS expression into MongoDB syntax. As input we take
    a dictionary of key, operator and value.
    """
    # TODO: for and brackets operators I can combine everything in a
    # single dictionary. While if operator OR is met, I can return list of
    # mongo_dicts for processing, since list represent list of OR'ed results
    print "mongo_exp, input", cond_list
    mongo_list = []
    mongo_dict = {}
    for cond in cond_list:
        if  cond == '(' or cond == ')' or cond == 'and':
            continue
        if  cond == 'or':
            mongo_list.append(mongo_dict)
        key  = cond['key']
        val  = cond['value']
        oper = cond['op']
        print "key, op, val", key, oper, val
        if  MONGO_MAP.has_key(oper):
            if  mongo_dict.has_key(key):
                mongo_value = mongo_dict[key]
                mongo_value[MONGO_MAP[oper]] = val
                mongo_dict[key] = mongo_value
            else:
                mongo_dict[key] = {MONGO_MAP[oper] : val}
        elif oper == 'like':
            # for expressions: *val* use pattern .*val.*
            pat = re.compile(val.replace('*', '.*'))
            mongo_dict[key] = pat
        elif oper == 'not like':
            # TODO, reverse the following:
            # for expressions: *val* use pattern .*val.*
            pat = re.compile(val.replace('*', '.*'))
            mongo_dict[key] = pat
        elif oper == '=':
            mongo_dict[key] = val
        elif oper == 'between':
            mongo_dict[key] = {'$in' : [i for i in range(val[0], val[1])]}
        elif oper == 'last':
            mongo_dict[key] = 'last operator'
        else:
            msg = 'Not supported operator %s' % oper
            raise Exception(msg)
        if  mongo_dict not in mongo_list:
            mongo_list.append(mongo_dict)
    return mongo_list

# NOTE: I used this method originally, so will keep it around for a while
# the rest of the code should use QLLexer.query_parser instead which
# provides the same functionality
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
        cond_exp = parts[1]
        for cond in cond_exp.split(' and '):
            for oper in DAS_OPERATORS:
                if  cond.find(oper) != -1:
                    clist = cond.split(oper)
                    params[clist[0].strip()]=(oper.strip(), clist[1].strip())
                    break
    return selkeys, params

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
        
class QLLexer(object):
    """
    DAS QL lexer. It holds DAS operators, reserved keywords and data-service
    maps. Its task to tokenize input query into set of
    - selection keys
    - order_by keys
    - create condition dictionaries, {'value':'bla', 'op':'=', 'key':'run'}
    """
    def __init__(self, imap, params, funclist=[], weights={}):
        self.prefix    = ['find ', 'plot ', 'view ']
        self.operators = DAS_OPERATORS
        self.qlmap = imap #{'dbs': [list of keys], ...}
        self.qlparams = params #{'dbs': [list of params passed to srv], ...}
        self.reserved_keys = ['system', 'date']
        self.known_keys = [k for i in self.qlmap.values() for k in i]
        self.known_params = [k for i in self.qlparams.values() for k in i]
        self.known_func = funclist
        # TODO: services weights will be used to pick-up least expensive
        # data-service to use if common key found accross multiple
        # data-services. This should be assigned elswhere, in DAS entity DB.
        if  weights:
            self.service_weights = weights
        else:
            self.service_weights = {
                'dbs': 5,
                'phedex': 10,
                'sitedb': 0,
                'runsum': 6,
                'lumidb': 7,
                'dq': 8,
            }
        for key in imap.keys():
            if  not self.service_weights.has_key(key):
                self.service_weights[key] = 1

    def das_functions(self, uinput):
        """
        Extract from given query a list of DAS aggregation functions.
        Modify query to plain form and return plain queyry and DAS functions.
        For example:
        find das_sum(a,b),c where d=1
        we will return
        find a,b,c where d=1
        functions = {'das_sum': ['a','b']}
        """
        functions = {}
        # look-up if we have any DAS function in input query
        found = 0
        for func in self.known_func:
            if  uinput.find(func) != -1:
                found = 1
        if  not found:
            return uinput, functions

        # if found we need to extract them
        cond = ''
        if  uinput.find(' where ') != -1:
            usplit = uinput.split(' where ')
            uinput = usplit[0]
            cond = ' where ' + usplit[1]
        for prx in self.prefix:
            if  uinput.find(prx) == 0:
                uinput = uinput.replace(prx, '')
                break
        expression = uinput
        bckdict = {}
        counter = 0
        while 1:
            bckobj = findbracketobj(expression, dbs_func=False)
            if  not bckobj:
                break
            bidx = '_bobj_%s' % counter
            bckdict[bidx] = bckobj.replace('(', '').replace(')', '')
            counter += 1
            expression = expression.replace(bckobj, bidx)
        selkeys = []
        for item in expression.split(','):
            item = item.strip()
            found = 0
            for func in self.known_func:
                if  item.find(func) == 0:
                    bobj = item.replace(func, '')
                    functions[func] = bckdict[bobj]
                    found = bckdict[bobj]
                    break
            if  found:
                for elem in found.split(','):
                    elem = elem.strip()
                    if  elem not in selkeys:
                        selkeys.append(elem)
            else:
                if  item not in selkeys:
                    selkeys.append(item)
        query = prx + ','.join(selkeys) + cond
        return query, functions
        
    def fix_reserved_keywords(self, query):
        """
        Lowering all reserved keywords in a query
        """
        for word in self.prefix + self.operators + [' and ', ' or ']:
            query = query.replace(str(word).upper(), word)
        return query
        
    def find_namespaces(self, query):
        """
        DAS-QL syntax allows namespace in a query, e.g.
        find phedex:block where block=bla
        This method analyze the query and build namespace dict.
        Namespace names cut out from the query for further processing.
        """
        namespace = {}
        newquery  = str(query)
        pat = re.compile('[a-zA-Z:]')
        for word in query.split():
            if  word.find(':') != -1 and pat.match(word):
                srv, keyword = word.split(':')
                if  srv in self.qlmap.keys():
                    add2dict(namespace, srv, [keyword])
                    self.service_weights[srv] = -1
                    newquery = newquery.replace(word, keyword)
                else:
                    msg = "Found unkonwn service '%s' in input query" % srv
                    raise Exception(msg)
        return newquery, namespace
        
    def findservices(self, key):
        """
        For given key find a list of corresponding data-services
        """
        for srv, keys in self.qlmap.items():
            if  key in keys:
                yield srv
        for srv, args in self.qlparams.items():
            if  key in args:
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
                break
        olist.sort()
        if  not olist:
            return
        oper = olist[0][-1]

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
    Initialized with two maps:
    - map of service names and data-service QL keys, e.g.
       {'dbs':['run', 'dataset', ...], ...}
    - map of service names and data-service input parameters, e.g.
       {'lumidb':['run'], ...}
    """
    def __init__(self, imap, params, funclist=[], weights={}):
        if  imap.keys() != params.keys():
            msg  = 'Provided service names from map of data-service QL keys'
            msg += 'does not match with'
            msg += 'service names from map of data-service parameters.\n'
            msg += '%s != %s' % (imap.keys(), params.keys())
            raise Exception(msg)
        QLLexer.__init__(self, imap, params, funclist, weights)

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
            if  k.find('.') != -1: # key is in form key.attr
                k = k.split('.')[0]
            if  k not in self.known_keys and k not in self.known_params\
                and k not in self.reserved_keys:
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
        query, namespace = self.find_namespaces(query)
        query, functions = self.das_functions(query)
        rdict = {}
        rdict['functions']       = functions
        rdict['conditions']      = self.conditions(query)
        order_by_list, order_by  = self.order_by(query)
        rdict['order_by_list']   = order_by_list
        rdict['order_by_order']  = order_by
        rdict['selkeys']         = unique_list(self.selkeys(query))
        rdict['allkeys']         = unique_list(self.allkeys(query))
        services, uservices, ulist, daslist = self.uniq_services(query)
        rdict['services']        = services
        rdict['unique_services'] = unique_list(uservices)
        if  len(uservices) == 1:
            rdict['unique_keys'] = rdict['selkeys']
        else:
            rdict['unique_keys'] = unique_list(ulist)
        rdict['daslist']         = daslist
        self.check(query, rdict)
#        print "qlparser", rdict
#        import sys
#        sys.exit(0)
        return rdict
        
    def services(self, query):
        """
        Look-up all services whose keys match the query
        """
        query = self.fix_reserved_keywords(query)
        sdict = {}
        skeys = unique_list(self.selkeys(query))
        akeys = unique_list(self.allkeys(query))
        params = list(set(akeys) - set(skeys))
        allkeys = []
        for key in skeys:
#            entity = key.split('.')[0]
            for service, keys in self.qlmap.items():
#                if  entity in keys:
                if  key in keys:
                    if  key not in allkeys:
                        allkeys.append(key)
                    if  sdict.has_key(service):
                        vlist = sdict[service]
                        sdict[service] = vlist +[key]
                    else:
                        sdict[service] = [key]
        for key in params:
            for service, keys in self.qlparams.items():
                if  key in keys:
                    if  key not in allkeys:
                        allkeys.append(key)
                    if  sdict.has_key(service):
                        vlist = sdict[service]
                        sdict[service] = vlist +[key]
                    else:
                        sdict[service] = [key]
        # I need to assing service weight to distinguish a case
        # when several services can answer the same query
        overlap = [s for s, k in sdict.items() if k == allkeys]
        weight_tuple = [(w, s) for s, w in self.service_weights.items()\
             if s in overlap]
        weight_tuple.sort()
        if  weight_tuple:
            srv_winner = weight_tuple[0][1]
            for srv in list(sdict.keys()):
                if  srv != srv_winner:
                    del sdict[srv]
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
        skeys = unique_list(self.selkeys(query)) # selection keys
#        entities = [i.split('.')[0] for i in skeys]
        akeys = unique_list(self.allkeys(query)) # all keys
        ckeys = [row['key'] for row in self.conditions(query) \
                    if type(row) is types.DictType] # condition keys
        ckeys = unique_list(ckeys)

        services = self.services(query)
        if  len(services.keys()) == 1: # all keys from one data-service
            uservices = services.keys()
            ulist = skeys
            daslist = [{services.keys()[0]:query}]
            return services, uservices, ulist, daslist
        else:
            uservices = [] # unique set of services for list of keys
            onesrvcoverage = None
            for srv in services.keys():
                srv_keys = self.qlmap[srv]
                srv_args = self.qlparams[srv]
                if  ckeys:
                    cond1 = set(srv_keys) & set(skeys)# srv keys covers skeys
#                    cond1 = set(srv_keys) & set(entities)# srv keys covers entities
                    if  not srv_args:
                        cond2 = True
                    else:
                        cond2 = set(srv_args) & set(ckeys)# params covers ckeys
                    if  cond1 and cond2:
                        uservices.append(srv)
                        covlist1 = list(cond1)
                        covlist1.sort()
                        covlist2 = list(set(srv_args) & set(ckeys))
                        covlist2.sort()
                        if  covlist1 == skeys and covlist2 == ckeys:
#                        if  covlist1 == entities and covlist2 == ckeys:
                            onesrvcoverage = srv
                            break
                else:
                    if  set(srv_keys) & set(skeys):# srv keys covers skeys
#                    if  set(srv_keys) & set(entities):# srv keys covers entities
                        uservices.append(srv)
                        onesrvcoverage = srv
                        break

        # one service can cover selection and parameter keys
        if  onesrvcoverage:
            uservices = [onesrvcoverage]

        if  not uservices:
            msg = 'Unable to find unique set of services out of %s' % services
            raise Exception(msg)
        # find relation keys in uservices
        pairs = [i for i in oneway_permutations(uservices)]
        ulist = list(skeys)
        for pair in pairs:
            list0 = self.qlmap[pair[0]]
            list1 = self.qlmap[pair[1]]
            ulist += [i for i in list(set(list0) & set(list1)) if i not in ulist\
                        and i.find('.') == -1]

        # based on conditions figure which data services to query
        # first we construct condition dict in a form {'srv0':'condition'} and
        # 'final' list of conditions in a form srv0 and|or srv1, etc.
        # then we disallow conditions from different data-services 
        # if they're outside of common brackets.
        # Finally we construct DAS queries with proper conditions

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
                    cond_srvs += list(int_srvs)
            cond_srvs = unique_list(cond_srvs)
            # check if we have more then 1 data-srv for bracket obj
            if  bracket_found:
                if  len(cond_srvs) > 1:
                    msg  = "Unsupported conditions: mix of conditions from more"
                    msg += " then on data-service inside of brackets"
                    msg += "condition services %s" % cond_srvs
                    raise Exception(msg)
            srvkey = 'srv%s' % idx
            conddict[srvkey] = condition, cond_srvs
            final += ' ' + srvkey
            if  oper:
                final += ' ' + oper
            if  not cond:
                break
            idx += 1
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
#        print
#        print "query", query
#        print "services", services
#        print "uservices", uservices
#        print "cond_exp", cond_exp
#        print "ulist", ulist
#        print "final", final
#        print "conddict", conddict
#        print "daslist", daslist
        return services, uservices, ulist, daslist

