#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103,C0301,R0201

"""
DAS Query Language parser based on PLY.
"""

__revision__ = "$Id: $"
__version__ = "$Revision: $"
__author__ = "Gordon Ball and Valentin Kuznetsov"

import os
import sys
import ply.lex
import ply.yacc
import re

from   DAS.utils.utils import convert2date
from   DAS.core.das_ql import das_filters, das_operators, das_mapreduces
from   DAS.core.das_ql import das_aggregators, das_special_keys

def lexer_error(query, pos, error):
    """Produce pretty formatted message about invalid query"""
    msg  = '\nUnable to parser input query\n'
    msg += 'QUERY: %s\n' % query
    msg += '-' * (len('QUERY: ') + pos) + '^' + '\n'
    msg += error
    return msg

class DASPLY(object):
    """
    DAS QL parser based on PLY lexer/parser.
    """
    def __init__(self, parserdir, daskeys, dassystems,
                 operators=None, specials=None, filters=None,
                 aggregators=None, mapreduces=None, verbose=0):
        self.daskeys = daskeys
        self.verbose = verbose
        self.lexer   = None # defined at run time using self.build()
        self.parser  = None # defined at run time using self.build()

        self.dassystems = dassystems
        self.parsertab_dir = parserdir
        if  not os.path.isdir(self.parsertab_dir):
            msg = 'Directory %s does not exists' % self.parsertab_dir
            raise Exception(msg)
        
        # test if we have been given a list of desired operators/filters
        # /aggregators, if not get the lists from das_ql
        operators = operators if operators else das_operators()
        filters = filters if filters else das_filters()
        aggregators = aggregators if aggregators else das_aggregators()
        mapreduces = mapreduces if mapreduces else das_mapreduces()
        specials = specials if specials else das_special_keys()
        
        # build a map of token->token.type which we can use in the
        # enlarged VALUE rule
        self.tokenmap = {}
        for o in operators:
            self.tokenmap[o] = 'OPERATOR'
        for f in filters:
            self.tokenmap[f] = 'FILTER'
        for a in aggregators:
            self.tokenmap[a] = 'AGGREGATOR'
        for m in mapreduces:
            self.tokenmap[m] = 'MAPREDUCE'
        for s in specials:
            self.tokenmap[s] = 'SPECIALKEY'

    tokens = [
        'DASKEY',
        'IPADDR',
        'PIPE',
        'AGGREGATOR',
        'FILTER',
        'COMMA',
        'LSQUARE',
        'RSQUARE',
        'LPAREN',
        'RPAREN',
        'OPERATOR',
        'FILTER_OPERATOR',
        'VALUE',
        'NUMBER',
        'DATE',
        'MAPREDUCE',
        'SPECIALKEY',
    ]

    t_OPERATOR = r'='
    t_FILTER_OPERATOR = r'>|<|<=|>='
    t_PIPE = r'\|'
    t_COMMA    = r'\,'
    t_LSQUARE = r'\['
    t_RSQUARE = r'\]'
    t_LPAREN = r'\('
    t_RPAREN = r'\)'
    # A string containing ignored characters (spaces and tabs)
    t_ignore  = ' \t'

### !!!IMPORTANT!!!
### PLEASE NOTE the order of functions in a file regulates the order of regex
### which will be applied during lexer step
    
    def t_IPADDR(self, t):
        r'([0-9]{1,3}\.){3,3}[0-9]{1,3}'
        return t

    def t_DATE(self, t):
        r'[0-2]0[0-9][0-9][0-1][0-9][0-3][0-9]|[0-9]+[dhm]'
        if  t.value.find('h') == -1 and \
            t.value.find('d') == -1 and \
            t.value.find('m') == -1:
            t.value = int(t.value)
        return t

    def t_NUMBER(self, t):
        r'-?\d+\.{0,1}\d*'
        if  t.value.find('.') != -1:
            t.value = float(t.value)
        else:
            t.value = int(t.value)
        return t

    def t_VALUE(self, t):
        r'''[a-zA-Z/*][a-zA-Z_0-9/*\-#\.]+|'.*?'|".*?"'''
        # test for a filter/aggregator/operator
        if t.value in self.tokenmap:
            #change the token type to the appropriate one
            #eg, FILTER
            t.type = self.tokenmap[t.value]
            return t

        #test for a DASKEY
        # DASKEY if
        # 1. lowercase word without dots in self.daskeys
        if t.value in self.daskeys:
            t.type = 'DASKEY'
            return t
        # 2. lowercase.AnyCase(.AnyCase...)
        if re.match(r'[a-z_]+(\.[a-zA-Z_]+)+', t.value):
            t.type = 'DASKEY'
            return t
        # 3. das_id is also a DASKEY
        if re.match(r'das_id', t.value):
            t.type = 'DASKEY'
            return t
        # 4. records is also a DASKEY
        if re.match(r'records', t.value):
            t.type = 'DASKEY'
            return t
        
        
        # strip quotation marks, if included
        # anything in quotation marks can't have been a
        # operator/filter/aggregator/DASKEY
        if t.value[0] in ("'", '"'):
            t.value = t.value[1:-1]
        
        # it's probably just a plain old string
        return t

    def test_lexer(self, data):
        """Test provided data with our lexer"""
        self.lexer.input(data)
        tok = None
        while True:
            try:
                tok = self.lexer.token()
            except:
                error = sys.exc_info()[1][0]
                if  not tok:
                    pos = 0
                else:
                    pos = data.index(str(tok.value)) + len(str(tok.value))
                msg = lexer_error(data, pos, error)
                raise Exception(msg)
            if  self.verbose:
                print tok
            if  not tok:
                break

    def t_error(self, t):
        """Error handling rule"""
        raise Exception("Illegal character '%s'" % t)

    def p_overall0(self, p):
        """overall : keys PIPE pipelist"""
        p[0] = {'keys': p[1], 'pipe': p[3]}

    def p_overall1(self, p):
        """overall : keys"""
        p[0] = {'keys': p[1], 'pipe': []}

    def p_pipelist0(self, p):
        """pipelist : pipelist PIPE pipefunc"""
        p[0] = p[1] + [p[3]]

    def p_pipelist1(self, p):
        """pipelist : pipefunc"""
        p[0] = [p[1]]

    # lexer logic, we can have daskey=value, but due to ambiguity of daskey
    # defition it is allowed to have <daskey>=<daskey> where second <daskey>
    # can be a simple word, e.g. system=sitedb
    def p_keyop0(self, p):
        """keyop : DASKEY OPERATOR VALUE
                 | DASKEY OPERATOR DASKEY
                 | DASKEY OPERATOR NUMBER
                 | DASKEY OPERATOR IPADDR
                 | SPECIALKEY OPERATOR DATE
                 | SPECIALKEY OPERATOR VALUE
                 | SPECIALKEY OPERATOR array
                 | DASKEY OPERATOR array"""
        p[0] = ('keyop', p[1], p[2], p[3])

    def p_keyop1(self, p):
        """keyop : DASKEY"""
        p[0] = ('keyop', p[1], None, None)
        
    def p_keys0(self, p):
        """keys : keys keyop"""
        p[0] = p[1] + [p[2]]

    def p_keys1(self, p):
        """keys : keyop"""
        p[0] = [p[1]]
    
    def p_array(self, p):
        """array : LSQUARE list RSQUARE"""
        p[0] = tuple(['array'] + p[2])

    def p_list(self, p):
        """list : list COMMA DATE
                | list COMMA NUMBER"""
        p[0] = p[1] + [p[3]]
        
    def p_list2(self, p):
        """list : DATE
                | NUMBER"""
        p[0] = [p[1]]

    def p_list_for_filter(self, p):
        """oneexp : DASKEY OPERATOR VALUE
                  | DASKEY OPERATOR NUMBER
                  | DASKEY FILTER_OPERATOR VALUE
                  | DASKEY FILTER_OPERATOR NUMBER
                  | DASKEY"""
        val = ''
        for idx in range(0, len(p)):
            if  p[idx]:
                val += str(p[idx])
        p[0] = [val]

    def p_list_for_filter2(self, p):
        """list_for_filter : list_for_filter COMMA oneexp"""
        p[0] = p[1] + p[3]

    def p_list_for_filter3(self, p):
        """list_for_filter : oneexp"""
        p[0] = p[1]

    def p_pipefunc0(self, p):
        """pipefunc : FILTER list_for_filter"""
        p[0] = tuple(['filter', p[1], p[2]])

    def p_pipefunc1(self, p): #should probably merge this with p_arglist
        """pipefunc : FILTER"""
        p[0] = tuple(['filter', p[1], None])

    def p_mapreduce(self, p): #should probably merge this with p_arglist
        """pipefunc : MAPREDUCE"""
        p[0] = tuple(['mapreduce', p[1], None])

    def p_oneagg(self, p):
        """oneagg : AGGREGATOR LPAREN DASKEY RPAREN"""
        p[0] = tuple(['aggregator', p[1], p[3]])

    def p_agglist0(self, p):
        """agglist : agglist COMMA oneagg"""
        p[0] = p[1] + [p[3]]

    def p_agglist1(self, p):
        """agglist : oneagg"""
        p[0] = [p[1]]

    def p_pipefunc2(self, p):
        """pipefunc : agglist"""
        p[0] = tuple(['aggregators'] + p[1])

    def p_error(self, p):
        """Error rule for syntax errors"""
        raise Exception("Syntax error in input, %s" % p)

    def build(self, **kwargs):
        """Dynamic lexer/parser builder"""
        self.lexer  = ply.lex.lex(module=self, **kwargs)
        self.parser = ply.yacc.yacc(module=self, outputdir=self.parsertab_dir, 
                        debug=self.verbose, **kwargs)
        
def ply2mongo(query):
    """
    DAS-QL query  : file block=123 | grep file.size | sum(file.size)
    PLY query     : {'keys': [('keyop', 'file', None, None), 
                              ('keyop', 'block', '=', 123)], 
                     'pipe': [('filter', 'grep', ['file.size']), 
                              ('aggregators', ('aggregator', 'sum', 'file.size'))]}
    Mongo query   : {'fields': [u'file'], 
                     'spec': {u'block.name': 123}, 'filters': ['file.size'],
                     'aggregators': [('sum', 'file.size')]}
    """
    mongodict = {}
    if  query.has_key('pipe'):
        for item in query['pipe']:
            if  item[0] == 'filter':
                dasfilter, name, args = item
                if  dasfilter == 'filter' and name == 'grep':
                    mongodict['filters'] = args
            if  item[0] == 'aggregators':
                aggs = [(k[1], k[2]) for k in item[1:]]
                mongodict['aggregators'] = aggs
            if  item[0] == 'mapreduce':
                _, name, _ = item
                mongodict['mapreduce'] = name
    fields = []
    spec   = {}
    for _, name, oper, val in query['keys']:
        dasname = name 
        if  oper and val: # real condition
            value = val
            if  oper == 'last':
                value = {'$in' : convert2date(value)}
            if  oper == 'in':
                value = {'$in' : list(val[1:])}
            if  oper == 'between':
                vlist = list(val[1:])
                vlist.sort()
                value = {'$gte' : vlist[0], '$lte': vlist[-1]}
        else: # selection field
            fields.append(name)
            value = '*'
        if  spec.has_key(dasname):
            exist_value = spec[dasname]
            if  isinstance(exist_value, list):
                array = [r for r in exist_value] + [value]
            elif exist_value == '*' and value != '*':
                array = value
            else:
                array = [exist_value, value]
            spec[dasname] = array
        else:
            spec[dasname] = value
    if  fields:
        mongodict['fields'] = fields
        for key in fields:
            if  len(spec.keys()) != 1 and spec.has_key(key):
                del spec[key]
    else:
        if  len(spec.keys()) == 1:
            mongodict['fields'] = [spec.keys()[0].split('.')[0]]
        else:
            mongodict['fields'] = None
    mongodict['spec'] = spec
    # VERY SPECIAL case. If user asks to look-up all records, by using
    # records keyword we clean-up fields in mongo query.
    if  mongodict['fields'] == ['records']:
        mongodict['fields'] = None
    return mongodict

