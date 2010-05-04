#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103,R0201

"""
DAS Query Language lexer.
"""

__revision__ = "$Id: das_lexer.py,v 1.1 2010/04/30 16:28:23 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import sys
import types
import ply.lex as lex
from DAS.core.das_ql import das_reserved

def das_lexer_error(query, pos, error):
    """Produce pretty formatted message about invalid query"""
    msg  = '\nUnable to parser input query\n'
    msg += 'QUERY: %s\n' % query
    msg += '-' * (len('QUERY: ') + pos) + '^' + '\n'
    msg += error
    return msg

class DASLexer(object):
    """DAS lexer"""
    def __init__(self, daskeys, verbose=None):
        self.daskeys = daskeys
        self.verbose = verbose
        self.lexer   = None # defined at run time using self.build()

    tokens = [
        'DASKEY',
        'NUMBER',
        'SPACE',
        'LIST',
        'RESERVED',
        'WORD',
    ]

    # Regular expression rules for simple tokens
    t_SPACE    = r'\ '
    t_LIST     = r'\[\d+,\s*\d+\]'
    t_RESERVED = r'|'.join(das_reserved())

    def t_DASKEY(self, t):
        r'[a-z_]+' # in DAS we use only lower case keywords
        if  t.value in self.daskeys or t.value in das_reserved():
            return t
        else:
            msg = "Unknown DAS key: %s" % t.value
            raise Exception(msg)

    def t_WORD(self, t):
        r'[a-zA-Z/*][a-zA-Z_0-9/*]+|[0-9]+[dhm]'
        cond1 = t.value not in self.daskeys
        list1 = t.value.split()
        cond2 = set(list1) & set(self.daskeys)
        if  cond1 and not cond2:
            return t
        else:
            msg = "Unknown DAS key value: %s" % t.value
            raise Exception(msg)

    def t_NUMBER(self, t):
        r'\d+'
        t.value = int(t.value)
        return t

    def t_DATE(self, t):
        r'[0-2]0[0-9][0-9][0-1][0-9][0-3][0-9]'
        t.value = int(t.value)
        return t

    # Ignored characters
    t_ignore = " \t"

    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += t.value.count("\n")
        
    def t_error(self, t):
        'lexer error'
        msg = "Invalid DAS operator '%s'" % t.value[0]
        print msg
        raise Exception(msg)
#        t.lexer.skip(1)

    def build(self, **kwargs):
        """Build the lexer"""
        self.lexer = lex.lex(module=self, **kwargs)

    def test(self, data):
        """Test provided data"""
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
                    obj = tok.value
                    if  type(obj) is types.StringType:
                        pos = tok.lexpos + len(tok.value)
                    else:
                        pos = tok.lexpos + int(obj)
                msg = das_lexer_error(data, pos, error)
                raise Exception(msg)
#                raise
            if  not tok:
                break
            if  self.verbose:
                print tok

    def input(self, data):
        """Lexer input method"""
        return self.lexer.input(data)

    def token(self):
        """Lexer token method"""
        return self.lexer.token()
    
def test():
    """Local test for DAS lexer"""
    import traceback
    DAS_KEYS = ['lat', 'lon', 'date', 'site']
    # Build the lexer
    lexer = DASLexer(DAS_KEYS, verbose=1)
    lexer.build()
    print "build lexer", lexer, lexer.tokens
    query = "lat"
    print "test lexer:", query
    lexer.test(query)
    print
    query = "lat=2 lon=2"
    print "test lexer:", query
    lexer.test(query)
    query = "date=2010010112"
    print "test lexer:", query
    lexer.test(query)
    print
    query = "lat=2 bla"
    print "test lexer:", query
    try:
        lexer.test(query)
    except Exception, _:
        traceback.print_exc()
    print
    query = "lat=2 lon>1"
    print "test lexer:", query
    try:
        lexer.test(query)
    except Exception, _:
        traceback.print_exc()
    print
    query = "site = New York"
    lexer.test(query)
    print "test lexer:", query
    print
    query = "date last 24h"
    lexer.test(query)
    print "test lexer:", query
    print

if  __name__ == '__main__':
    test()
