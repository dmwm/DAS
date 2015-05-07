#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=C0103,C0301,R0201,W0703

"""
DAS Query Language parser based on PLY.
"""
from __future__ import print_function

__author__ = "Gordon Ball and Valentin Kuznetsov"

import os
import sys
import ply.lex
import ply.yacc
import re

from   DAS.utils.utils import das_dateformat, print_exc
from   DAS.core.das_ql import das_filters, das_operators, das_mapreduces
from   DAS.core.das_ql import das_aggregators, das_special_keys
from   DAS.core.das_ql import das_db_keywords
from   DAS.utils.regex import date_yyyymmdd_pattern

def das_parser_error(query, error):
    """Return DAS keyword which cause parser to burk"""
    print("DAS PARSER ERROR", error)
    try:
        # LexToken returns tok.type, tok.value, tok.lineno, and tok.lexpos
        _msg, tok = error.split("LexToken")
        tokarr = tok.split(',')
        pos = int(tokarr[3].split(')')[0])
        return 'DAS could not parse your query at %s ' % query[pos:].split()[0]
    except Exception as _exc:
        return error

def parser_error(error):
    """Return human readable DAS parser error string"""
    print("ERROR", error)
    try:
        # LexToken returns tok.type, tok.value, tok.lineno, and tok.lexpos
        msg, tok = error.split("LexToken")
        tokarr = tok.split(',')
        return "%s error %s=%s, at line %s, position %s" \
                % (msg.strip(), tokarr[0].lower(), tokarr[1], tokarr[2], tokarr[3])
    except:
        return error

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
        'DASKEY_ATTR',
        'IPADDR',
        'PIPE',
        'AGGREGATOR',
        'FILTER',
        'COMMA',
        'LSQUARE',
        'RSQUARE',
        'LPAREN',
        'RPAREN',
        'EQUAL',
        'OPERATOR',
        'FILTER_OPERATOR',
        'VALUE',
        'NUMBER',
        'DATE',
        'DATE_STR',
        'MAPREDUCE',
        'SPECIALKEY',
    ]

    t_EQUAL = r'='
    t_FILTER_OPERATOR = r'!=|<=|>=|>|<'
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
        r"20\d\d(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])|'20\d\d(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])\s[0-2]\d:[0-6]\d:[0-6]\d'|\"20\d\d(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])\s[0-2]\d:[0-6]\d:[0-6]\d\""
        t.value = t.value.replace("'", "").replace('"', '')
        return t

    def t_DATE_STR(self, t):
        r'[0-9]+[dhm]'
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
        r'''[a-zA-Z/*][a-zA-Z_0-9@/*\-#\.]+|'.*?'|".*?"|y|n|\*'''
        # test if query starts with find and/or contains where (DBS-QL)
        if  t.value == 'find' or t.value == 'where':
            msg = 'Not a valid DAS query, DBS-QL keyword: %s' % t.value
            raise Exception(msg)

        if  t.value == 'and' or t.value == 'or':
            msg = 'DAS-QL does not support AND|OR'
            raise Exception(msg)

        # test for a filter/aggregator/operator
        if  t.value in self.tokenmap:
            #change the token type to the appropriate one
            #eg, FILTER
            t.type = self.tokenmap[t.value]
            return t

        #test for a DASKEY
        # DASKEY if
        # 1. lowercase word without dots in self.daskeys
        if  t.value in self.daskeys:
            t.type = 'DASKEY'
            return t
        # 2. lowercase.AnyCase(.AnyCase...)
        if  re.match(r'[a-z_]+(\.[a-zA-Z_]+)+', t.value):
            t.type = 'DASKEY_ATTR'
            return t
        # 3. das_id is also a DASKEY
        if  re.match(r'das_id', t.value):
            t.type = 'DASKEY'
            return t
        # 4. assign das_db_keywords as DASKEY's
        for keyword in das_db_keywords():
            if  re.match(keyword, t.value):
                t.type = 'DASKEY'
                return t
        
        # strip quotation marks, if included
        # anything in quotation marks can't have been a
        # operator/filter/aggregator/DASKEY
        if  t.value[0] in ("'", '"'):
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
                print(tok)
            if  not tok:
                break

    def err_msg(self, err):
        pos  = self.lexer.lexpos-1
        pad  = ' '*pos
        msg  = "DASPLY lexer error, position=%s:\n" % pos
        msg += str(self.lexer.lexdata) + '\n'
        msg += pad + '^\n'
        msg += err + '\n'
        return msg

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

    def p_opvalue(self, p):
        """keyop : DASKEY EQUAL VALUE
                 | DASKEY EQUAL DASKEY_ATTR
                 | SPECIALKEY EQUAL VALUE"""
        p[0] = ('keyop', p[1], p[2], p[3])

    def p_opkey(self, p):
        """keyop : DASKEY EQUAL DASKEY"""
        p[0] = ('keyop', p[1], p[2], p[3])

    def p_key_comma_key(self, p):
        """keyop : DASKEY COMMA DASKEY
                 | DASKEY COMMA DASKEY COMMA DASKEY"""
        olist = [i for i in p if i and i != ',']
        p[0] = ('keyop', tuple(olist), None, None)

    def p_opnumber(self, p):
        """keyop : DASKEY EQUAL NUMBER
                 | SPECIALKEY EQUAL NUMBER"""
        p[0] = ('keyop', p[1], p[2], p[3])

    # last only valid in 'date last DATE_STR'
    def p_opdate(self, p):
        """keyop : DASKEY EQUAL DATE
                 | SPECIALKEY EQUAL DATE
                 | SPECIALKEY OPERATOR DATE_STR"""
        if p[2] == '=' :
            p[0] = ('keyop', p[1], p[2], p[3])
        elif p[2] == 'last' and p[1] == 'date':
            p[0] = ('keyop', p[1], p[2], p[3])
        else:
            err = "'%s %s %s' is not valid, " % (p[1], p[2], p[3])
            if p[2] == 'last':
                err += "'last' is only valid with key 'date'"
            else:
                err += "'%s' is expecting an array with '[]'" % p[2]
            p.error = self.err_msg(err)
            raise Exception(p.error)

    # 'date in array' is not valid,
    def p_oparry(self, p):
        """keyop : DASKEY OPERATOR array
                 | SPECIALKEY OPERATOR array"""
        if p[1] == 'date' and p[2] != 'last':
            if p[2] == 'between':
                p[0] = ('keyop', p[1], p[2], p[3])
            else :
                err  = "'%s %s %s' is not valid, " % (p[1], p[2], p[3])
                err += "'in' is not supported by 'date'"
                p.error = self.err_msg(err)
                raise Exception(p.error)
        elif p[2] != 'last':
            p[0] = ('keyop', p[1], p[2], p[3])
        else:
            err = "'%s %s %s' is not valid, " % (p[1], p[2], p[3])
            p.error = self.err_msg(err)
            raise Exception(p.error)

    def p_opip(self, p):
        """keyop : DASKEY EQUAL IPADDR"""
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

    def p_err1_op(self, p):
        """keyop : DASKEY OPERATOR VALUE
                 | SPECIALKEY OPERATOR VALUE
                 | DASKEY OPERATOR NUMBER
                 | SPECIALKEY OPERATOR NUMBER
                 | DASKEY OPERATOR DATE
                 | SPECIALKEY OPERATOR DATE"""
        err = "'%s %s %s' is not valid, " % (p[1], p[2], p[3])
        p.error = self.err_msg(err)
        raise Exception(p.error)

    def p_err2_op(self, p):
        """keyop : VALUE EQUAL VALUE
                 | VALUE EQUAL NUMBER
                 | VALUE OPERATOR VALUE
                 | VALUE OPERATOR array
                 | VALUE OPERATOR NUMBER
                 | VALUE EQUAL DATE
                 | VALUE OPERATOR DATE"""
        err  = "'%s %s %s' is not valid, " % (p[1], p[2], p[3])
        err += "'%s' is not a daskey" % p[1]
        p.error = self.err_msg(err)
        raise Exception(p.error)

    def p_err3_op(self, p):
        """keyop : SPECIALKEY"""
        err = "'%s' is not support without values " % p[1]
        if  p[1] == 'date':
            err += "please explore date info via attribute in filter"
        p.error = self.err_msg(err)
        raise Exception(p.error)

    def p_err4_op(self, p):
        """keyop : VALUE"""
        p.error = self.err_msg("'%s' value is misplaced" % p[1])
        raise Exception(p.error)

    def p_date_for_filter(self, p):
        """oneexp : DASKEY EQUAL DATE
                  | DASKEY FILTER_OPERATOR DATE
                  | DASKEY_ATTR EQUAL DATE
                  | DASKEY_ATTR FILTER_OPERATOR DATE"""
        val = ''
        if len(p) == 4:
            val += str(p[1]) + str(p[2]) + str(das_dateformat(p[3]))
        p[0] = [val]

    def p_list_for_filter(self, p):
        """oneexp : DASKEY EQUAL VALUE
                  | DASKEY EQUAL NUMBER
                  | DASKEY FILTER_OPERATOR VALUE
                  | DASKEY FILTER_OPERATOR NUMBER
                  | DASKEY_ATTR EQUAL LSQUARE VALUE RSQUARE
                  | DASKEY_ATTR EQUAL VALUE
                  | DASKEY_ATTR EQUAL NUMBER
                  | DASKEY_ATTR FILTER_OPERATOR VALUE
                  | DASKEY_ATTR FILTER_OPERATOR NUMBER"""
        val = ''
        for idx in xrange(0, len(p)):
            if  p[idx] or p[idx] == 0:
                val += str(p[idx])
        p[0] = [val]

    def p_key_for_filter(self, p):
        """oneexp : DASKEY
                  | DASKEY_ATTR"""
        p[0] = [str(p[1])]

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
        """oneagg : AGGREGATOR LPAREN DASKEY RPAREN
                  | AGGREGATOR LPAREN DASKEY_ATTR RPAREN"""
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
        if  p:
            raise Exception("DASPLY, syntax error in input, %s" % p)
        raise Exception("DAS unable to parse input query")

    def build(self, **kwargs):
        """Dynamic lexer/parser builder"""
        if  not self.lexer:
            self.lexer  = ply.lex.lex(module=self, **kwargs)
        if  not self.parser:
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
    filters = {}
    if  'pipe' in query:
        for item in query['pipe']:
            if  item[0] == 'filter':
                dasfilter, name, args = item
                if  dasfilter == 'filter' and name in ['grep', 'sort']:
                    if  name in filters:
                        filters[name] += args
                    else:
                        filters[name]  = args
                if  dasfilter == 'filter' and name == 'unique':
                    filters['unique'] = 1
            if  item[0] == 'aggregators':
                aggs = [(k[1], k[2]) for k in item[1:]]
                mongodict['aggregators'] = aggs
            if  item[0] == 'mapreduce':
                _, name, _ = item
                mongodict['mapreduce'] = name
        if  filters:
            mongodict['filters'] = filters
    fields = []
    spec   = {}
    inst   = None
    system = None
    for _, name, oper, val in query['keys']:
        if  name == 'instance':
            inst = val
            continue
        if  name == 'system':
            system = val
            continue
        dasname = name 
        if  (oper and val) or (oper and val==0): # real condition
            value = val
            if  name == 'date' and oper == '=':
                value = das_dateformat(value)
            if  oper == 'in':
                vlist = list(val[1:])
                if name == 'date':
                    vlist = [das_dateformat(x) for x in vlist]
                value = {'$in' : vlist}
            if  oper == 'between':
                vlist = list(val[1:])
                if name == 'date':
                    vlist = [das_dateformat(x) for x in vlist]
                vlist.sort()
                value = {'$gte' : vlist[0], '$lte': vlist[-1]}
        else: # selection field
            if  isinstance(name, list) or isinstance(name, tuple):
                for item in name:
                    if  item not in fields:
                        fields.append(item)
            elif isinstance(name, basestring):
                if  name not in fields:
                    fields.append(name)
            else:
                msg = 'Unsupported data type, %s, type=%s' % (name, type(name))
                raise Exception(msg)
            value = '*'
        if  dasname in spec:
            exist_value = spec[dasname]
            if  isinstance(exist_value, list) or isinstance(exist_value, tuple):
                array = [r for r in exist_value] + [value]
            elif exist_value == '*' and value != '*':
                array = value
            elif value == '*' and exist_value != '*':
                array = exist_value
            elif value == '*' and exist_value == '*':
                array = value
            else:
                array = [exist_value, value]
            spec[dasname] = array
        else:
            if  isinstance(dasname, list) or isinstance(dasname, tuple):
                for item in dasname:
                    spec[item] = value
            else:
                spec[dasname] = value
    if  fields:
        mongodict['fields'] = fields
        for key in fields:
            if  len(spec.keys()) != 1 and key in spec:
                if spec[key] == '*' :
                    del spec[key]
    else:
        if  len(spec.keys()) == 1:
            mongodict['fields'] = [spec.keys()[0].split('.')[0]]
        else:
            mongodict['fields'] = None
    mongodict['spec'] = spec
    if  inst:
        mongodict['instance'] = inst
    if  system:
        mongodict['system'] = system
    allowed_single_keys = ['records', 'queries', 'status', 'tier', 'type', \
            'group', 'release', 'primary_dataset', 'era']
    if  len(spec.keys()) == 1 and spec.values() == ['*'] and \
            spec.keys()[0] not in allowed_single_keys:
        msg = 'Single DAS key with no conditions, mongodict=%s' \
            % mongodict
        raise Exception(msg)
    return mongodict

