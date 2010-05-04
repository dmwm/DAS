# $ANTLR 3.1.3 Mar 17, 2009 19:23:44 SqlDAS.g 2009-05-12 09:11:00

import sys
from antlr3 import *
from antlr3.compat import set, frozenset


# for convenience in actions
HIDDEN = BaseRecognizer.HIDDEN

# token types
COMMA=5
T__42=42
T__28=28
T__23=23
T__13=13
T__21=21
T__19=19
DOT=10
T__39=39
T__30=30
T__17=17
T__27=27
T__24=24
AMP=11
T__34=34
T__15=15
T__35=35
T__36=36
T__20=20
WS=12
EQ=6
T__44=44
LT=7
GT=8
T__14=14
T__33=33
T__22=22
T__29=29
KW=4
T__43=43
T__31=31
T__40=40
EOF=-1
T__16=16
T__32=32
T__38=38
T__37=37
T__26=26
T__25=25
NOT=9
T__41=41
T__18=18


class SqlDASLexer(Lexer):

    grammarFileName = "SqlDAS.g"
    antlr_version = version_str_to_tuple("3.1.3 Mar 17, 2009 19:23:44")
    antlr_version_str = "3.1.3 Mar 17, 2009 19:23:44"

    def __init__(self, input=None, state=None):
        if state is None:
            state = RecognizerSharedState()
        super(SqlDASLexer, self).__init__(input, state)


        self.dfa2 = self.DFA2(
            self, 2,
            eot = self.DFA2_eot,
            eof = self.DFA2_eof,
            min = self.DFA2_min,
            max = self.DFA2_max,
            accept = self.DFA2_accept,
            special = self.DFA2_special,
            transition = self.DFA2_transition
            )






    # $ANTLR start "T__13"
    def mT__13(self, ):

        try:
            _type = T__13
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:7:7: ( 'count' )
            # SqlDAS.g:7:9: 'count'
            pass 
            self.match("count")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__13"



    # $ANTLR start "T__14"
    def mT__14(self, ):

        try:
            _type = T__14
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:8:7: ( 'sum' )
            # SqlDAS.g:8:9: 'sum'
            pass 
            self.match("sum")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__14"



    # $ANTLR start "T__15"
    def mT__15(self, ):

        try:
            _type = T__15
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:9:7: ( '(' )
            # SqlDAS.g:9:9: '('
            pass 
            self.match(40)



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__15"



    # $ANTLR start "T__16"
    def mT__16(self, ):

        try:
            _type = T__16
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:10:7: ( ')' )
            # SqlDAS.g:10:9: ')'
            pass 
            self.match(41)



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__16"



    # $ANTLR start "T__17"
    def mT__17(self, ):

        try:
            _type = T__17
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:11:7: ( 'WHERE' )
            # SqlDAS.g:11:9: 'WHERE'
            pass 
            self.match("WHERE")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__17"



    # $ANTLR start "T__18"
    def mT__18(self, ):

        try:
            _type = T__18
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:12:7: ( 'where' )
            # SqlDAS.g:12:9: 'where'
            pass 
            self.match("where")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__18"



    # $ANTLR start "T__19"
    def mT__19(self, ):

        try:
            _type = T__19
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:13:7: ( 'select' )
            # SqlDAS.g:13:9: 'select'
            pass 
            self.match("select")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__19"



    # $ANTLR start "T__20"
    def mT__20(self, ):

        try:
            _type = T__20
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:14:7: ( 'SELECT' )
            # SqlDAS.g:14:9: 'SELECT'
            pass 
            self.match("SELECT")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__20"



    # $ANTLR start "T__21"
    def mT__21(self, ):

        try:
            _type = T__21
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:15:7: ( 'find' )
            # SqlDAS.g:15:9: 'find'
            pass 
            self.match("find")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__21"



    # $ANTLR start "T__22"
    def mT__22(self, ):

        try:
            _type = T__22
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:16:7: ( 'FIND' )
            # SqlDAS.g:16:9: 'FIND'
            pass 
            self.match("FIND")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__22"



    # $ANTLR start "T__23"
    def mT__23(self, ):

        try:
            _type = T__23
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:17:7: ( 'and' )
            # SqlDAS.g:17:9: 'and'
            pass 
            self.match("and")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__23"



    # $ANTLR start "T__24"
    def mT__24(self, ):

        try:
            _type = T__24
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:18:7: ( 'AND' )
            # SqlDAS.g:18:9: 'AND'
            pass 
            self.match("AND")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__24"



    # $ANTLR start "T__25"
    def mT__25(self, ):

        try:
            _type = T__25
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:19:7: ( 'order' )
            # SqlDAS.g:19:9: 'order'
            pass 
            self.match("order")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__25"



    # $ANTLR start "T__26"
    def mT__26(self, ):

        try:
            _type = T__26
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:20:7: ( 'ORDER' )
            # SqlDAS.g:20:9: 'ORDER'
            pass 
            self.match("ORDER")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__26"



    # $ANTLR start "T__27"
    def mT__27(self, ):

        try:
            _type = T__27
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:21:7: ( 'by' )
            # SqlDAS.g:21:9: 'by'
            pass 
            self.match("by")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__27"



    # $ANTLR start "T__28"
    def mT__28(self, ):

        try:
            _type = T__28
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:22:7: ( 'BY' )
            # SqlDAS.g:22:9: 'BY'
            pass 
            self.match("BY")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__28"



    # $ANTLR start "T__29"
    def mT__29(self, ):

        try:
            _type = T__29
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:23:7: ( 'or' )
            # SqlDAS.g:23:9: 'or'
            pass 
            self.match("or")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__29"



    # $ANTLR start "T__30"
    def mT__30(self, ):

        try:
            _type = T__30
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:24:7: ( 'OR' )
            # SqlDAS.g:24:9: 'OR'
            pass 
            self.match("OR")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__30"



    # $ANTLR start "T__31"
    def mT__31(self, ):

        try:
            _type = T__31
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:25:7: ( 'in' )
            # SqlDAS.g:25:9: 'in'
            pass 
            self.match("in")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__31"



    # $ANTLR start "T__32"
    def mT__32(self, ):

        try:
            _type = T__32
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:26:7: ( 'IN' )
            # SqlDAS.g:26:9: 'IN'
            pass 
            self.match("IN")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__32"



    # $ANTLR start "T__33"
    def mT__33(self, ):

        try:
            _type = T__33
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:27:7: ( 'not' )
            # SqlDAS.g:27:9: 'not'
            pass 
            self.match("not")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__33"



    # $ANTLR start "T__34"
    def mT__34(self, ):

        try:
            _type = T__34
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:28:7: ( 'NOT' )
            # SqlDAS.g:28:9: 'NOT'
            pass 
            self.match("NOT")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__34"



    # $ANTLR start "T__35"
    def mT__35(self, ):

        try:
            _type = T__35
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:29:7: ( 'like' )
            # SqlDAS.g:29:9: 'like'
            pass 
            self.match("like")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__35"



    # $ANTLR start "T__36"
    def mT__36(self, ):

        try:
            _type = T__36
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:30:7: ( 'LIKE' )
            # SqlDAS.g:30:9: 'LIKE'
            pass 
            self.match("LIKE")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__36"



    # $ANTLR start "T__37"
    def mT__37(self, ):

        try:
            _type = T__37
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:31:7: ( 'COUNT' )
            # SqlDAS.g:31:9: 'COUNT'
            pass 
            self.match("COUNT")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__37"



    # $ANTLR start "T__38"
    def mT__38(self, ):

        try:
            _type = T__38
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:32:7: ( 'SUM' )
            # SqlDAS.g:32:9: 'SUM'
            pass 
            self.match("SUM")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__38"



    # $ANTLR start "T__39"
    def mT__39(self, ):

        try:
            _type = T__39
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:33:7: ( 'asc' )
            # SqlDAS.g:33:9: 'asc'
            pass 
            self.match("asc")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__39"



    # $ANTLR start "T__40"
    def mT__40(self, ):

        try:
            _type = T__40
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:34:7: ( 'ASC' )
            # SqlDAS.g:34:9: 'ASC'
            pass 
            self.match("ASC")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__40"



    # $ANTLR start "T__41"
    def mT__41(self, ):

        try:
            _type = T__41
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:35:7: ( 'desc' )
            # SqlDAS.g:35:9: 'desc'
            pass 
            self.match("desc")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__41"



    # $ANTLR start "T__42"
    def mT__42(self, ):

        try:
            _type = T__42
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:36:7: ( 'DESC' )
            # SqlDAS.g:36:9: 'DESC'
            pass 
            self.match("DESC")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__42"



    # $ANTLR start "T__43"
    def mT__43(self, ):

        try:
            _type = T__43
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:37:7: ( 'between' )
            # SqlDAS.g:37:9: 'between'
            pass 
            self.match("between")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__43"



    # $ANTLR start "T__44"
    def mT__44(self, ):

        try:
            _type = T__44
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:38:7: ( 'BETWEEN' )
            # SqlDAS.g:38:9: 'BETWEEN'
            pass 
            self.match("BETWEEN")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__44"



    # $ANTLR start "KW"
    def mKW(self, ):

        try:
            _type = KW
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:50:4: ( ( 'a' .. 'z' | '0' .. '9' | 'A' .. 'Z' ) ( 'a' .. 'z' | '0' .. '9' | 'A' .. 'Z' | '.' | '-' | '_' | ':' | '#' | '/' | '*' | '%' | '&' )* )
            # SqlDAS.g:50:6: ( 'a' .. 'z' | '0' .. '9' | 'A' .. 'Z' ) ( 'a' .. 'z' | '0' .. '9' | 'A' .. 'Z' | '.' | '-' | '_' | ':' | '#' | '/' | '*' | '%' | '&' )*
            pass 
            if (48 <= self.input.LA(1) <= 57) or (65 <= self.input.LA(1) <= 90) or (97 <= self.input.LA(1) <= 122):
                self.input.consume()
            else:
                mse = MismatchedSetException(None, self.input)
                self.recover(mse)
                raise mse

            # SqlDAS.g:50:45: ( 'a' .. 'z' | '0' .. '9' | 'A' .. 'Z' | '.' | '-' | '_' | ':' | '#' | '/' | '*' | '%' | '&' )*
            while True: #loop1
                alt1 = 2
                LA1_0 = self.input.LA(1)

                if (LA1_0 == 35 or (37 <= LA1_0 <= 38) or LA1_0 == 42 or (45 <= LA1_0 <= 58) or (65 <= LA1_0 <= 90) or LA1_0 == 95 or (97 <= LA1_0 <= 122)) :
                    alt1 = 1


                if alt1 == 1:
                    # SqlDAS.g:
                    pass 
                    if self.input.LA(1) == 35 or (37 <= self.input.LA(1) <= 38) or self.input.LA(1) == 42 or (45 <= self.input.LA(1) <= 58) or (65 <= self.input.LA(1) <= 90) or self.input.LA(1) == 95 or (97 <= self.input.LA(1) <= 122):
                        self.input.consume()
                    else:
                        mse = MismatchedSetException(None, self.input)
                        self.recover(mse)
                        raise mse



                else:
                    break #loop1



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "KW"



    # $ANTLR start "COMMA"
    def mCOMMA(self, ):

        try:
            _type = COMMA
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:115:8: ( ( ',' ) )
            # SqlDAS.g:115:9: ( ',' )
            pass 
            # SqlDAS.g:115:9: ( ',' )
            # SqlDAS.g:115:10: ','
            pass 
            self.match(44)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "COMMA"



    # $ANTLR start "DOT"
    def mDOT(self, ):

        try:
            _type = DOT
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:116:6: ( ( '.' ) )
            # SqlDAS.g:116:7: ( '.' )
            pass 
            # SqlDAS.g:116:7: ( '.' )
            # SqlDAS.g:116:8: '.'
            pass 
            self.match(46)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "DOT"



    # $ANTLR start "GT"
    def mGT(self, ):

        try:
            _type = GT
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:117:5: ( ( '>' ) )
            # SqlDAS.g:117:6: ( '>' )
            pass 
            # SqlDAS.g:117:6: ( '>' )
            # SqlDAS.g:117:7: '>'
            pass 
            self.match(62)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "GT"



    # $ANTLR start "LT"
    def mLT(self, ):

        try:
            _type = LT
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:118:5: ( ( '<' ) )
            # SqlDAS.g:118:6: ( '<' )
            pass 
            # SqlDAS.g:118:6: ( '<' )
            # SqlDAS.g:118:7: '<'
            pass 
            self.match(60)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "LT"



    # $ANTLR start "EQ"
    def mEQ(self, ):

        try:
            _type = EQ
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:119:5: ( ( '=' ) )
            # SqlDAS.g:119:6: ( '=' )
            pass 
            # SqlDAS.g:119:6: ( '=' )
            # SqlDAS.g:119:7: '='
            pass 
            self.match(61)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "EQ"



    # $ANTLR start "NOT"
    def mNOT(self, ):

        try:
            _type = NOT
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:120:6: ( ( '!' ) )
            # SqlDAS.g:120:7: ( '!' )
            pass 
            # SqlDAS.g:120:7: ( '!' )
            # SqlDAS.g:120:8: '!'
            pass 
            self.match(33)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "NOT"



    # $ANTLR start "AMP"
    def mAMP(self, ):

        try:
            _type = AMP
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:121:6: ( ( '&' ) )
            # SqlDAS.g:121:7: ( '&' )
            pass 
            # SqlDAS.g:121:7: ( '&' )
            # SqlDAS.g:121:8: '&'
            pass 
            self.match(38)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "AMP"



    # $ANTLR start "WS"
    def mWS(self, ):

        try:
            _type = WS
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:122:5: ( ( ' ' | '\\r' | '\\t' | '\\n' ) )
            # SqlDAS.g:122:6: ( ' ' | '\\r' | '\\t' | '\\n' )
            pass 
            if (9 <= self.input.LA(1) <= 10) or self.input.LA(1) == 13 or self.input.LA(1) == 32:
                self.input.consume()
            else:
                mse = MismatchedSetException(None, self.input)
                self.recover(mse)
                raise mse

            #action start
            _channel=HIDDEN;
            #action end



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "WS"



    def mTokens(self):
        # SqlDAS.g:1:8: ( T__13 | T__14 | T__15 | T__16 | T__17 | T__18 | T__19 | T__20 | T__21 | T__22 | T__23 | T__24 | T__25 | T__26 | T__27 | T__28 | T__29 | T__30 | T__31 | T__32 | T__33 | T__34 | T__35 | T__36 | T__37 | T__38 | T__39 | T__40 | T__41 | T__42 | T__43 | T__44 | KW | COMMA | DOT | GT | LT | EQ | NOT | AMP | WS )
        alt2 = 41
        alt2 = self.dfa2.predict(self.input)
        if alt2 == 1:
            # SqlDAS.g:1:10: T__13
            pass 
            self.mT__13()


        elif alt2 == 2:
            # SqlDAS.g:1:16: T__14
            pass 
            self.mT__14()


        elif alt2 == 3:
            # SqlDAS.g:1:22: T__15
            pass 
            self.mT__15()


        elif alt2 == 4:
            # SqlDAS.g:1:28: T__16
            pass 
            self.mT__16()


        elif alt2 == 5:
            # SqlDAS.g:1:34: T__17
            pass 
            self.mT__17()


        elif alt2 == 6:
            # SqlDAS.g:1:40: T__18
            pass 
            self.mT__18()


        elif alt2 == 7:
            # SqlDAS.g:1:46: T__19
            pass 
            self.mT__19()


        elif alt2 == 8:
            # SqlDAS.g:1:52: T__20
            pass 
            self.mT__20()


        elif alt2 == 9:
            # SqlDAS.g:1:58: T__21
            pass 
            self.mT__21()


        elif alt2 == 10:
            # SqlDAS.g:1:64: T__22
            pass 
            self.mT__22()


        elif alt2 == 11:
            # SqlDAS.g:1:70: T__23
            pass 
            self.mT__23()


        elif alt2 == 12:
            # SqlDAS.g:1:76: T__24
            pass 
            self.mT__24()


        elif alt2 == 13:
            # SqlDAS.g:1:82: T__25
            pass 
            self.mT__25()


        elif alt2 == 14:
            # SqlDAS.g:1:88: T__26
            pass 
            self.mT__26()


        elif alt2 == 15:
            # SqlDAS.g:1:94: T__27
            pass 
            self.mT__27()


        elif alt2 == 16:
            # SqlDAS.g:1:100: T__28
            pass 
            self.mT__28()


        elif alt2 == 17:
            # SqlDAS.g:1:106: T__29
            pass 
            self.mT__29()


        elif alt2 == 18:
            # SqlDAS.g:1:112: T__30
            pass 
            self.mT__30()


        elif alt2 == 19:
            # SqlDAS.g:1:118: T__31
            pass 
            self.mT__31()


        elif alt2 == 20:
            # SqlDAS.g:1:124: T__32
            pass 
            self.mT__32()


        elif alt2 == 21:
            # SqlDAS.g:1:130: T__33
            pass 
            self.mT__33()


        elif alt2 == 22:
            # SqlDAS.g:1:136: T__34
            pass 
            self.mT__34()


        elif alt2 == 23:
            # SqlDAS.g:1:142: T__35
            pass 
            self.mT__35()


        elif alt2 == 24:
            # SqlDAS.g:1:148: T__36
            pass 
            self.mT__36()


        elif alt2 == 25:
            # SqlDAS.g:1:154: T__37
            pass 
            self.mT__37()


        elif alt2 == 26:
            # SqlDAS.g:1:160: T__38
            pass 
            self.mT__38()


        elif alt2 == 27:
            # SqlDAS.g:1:166: T__39
            pass 
            self.mT__39()


        elif alt2 == 28:
            # SqlDAS.g:1:172: T__40
            pass 
            self.mT__40()


        elif alt2 == 29:
            # SqlDAS.g:1:178: T__41
            pass 
            self.mT__41()


        elif alt2 == 30:
            # SqlDAS.g:1:184: T__42
            pass 
            self.mT__42()


        elif alt2 == 31:
            # SqlDAS.g:1:190: T__43
            pass 
            self.mT__43()


        elif alt2 == 32:
            # SqlDAS.g:1:196: T__44
            pass 
            self.mT__44()


        elif alt2 == 33:
            # SqlDAS.g:1:202: KW
            pass 
            self.mKW()


        elif alt2 == 34:
            # SqlDAS.g:1:205: COMMA
            pass 
            self.mCOMMA()


        elif alt2 == 35:
            # SqlDAS.g:1:211: DOT
            pass 
            self.mDOT()


        elif alt2 == 36:
            # SqlDAS.g:1:215: GT
            pass 
            self.mGT()


        elif alt2 == 37:
            # SqlDAS.g:1:218: LT
            pass 
            self.mLT()


        elif alt2 == 38:
            # SqlDAS.g:1:221: EQ
            pass 
            self.mEQ()


        elif alt2 == 39:
            # SqlDAS.g:1:224: NOT
            pass 
            self.mNOT()


        elif alt2 == 40:
            # SqlDAS.g:1:228: AMP
            pass 
            self.mAMP()


        elif alt2 == 41:
            # SqlDAS.g:1:232: WS
            pass 
            self.mWS()







    # lookup tables for DFA #2

    DFA2_eot = DFA.unpack(
        u"\1\uffff\2\31\2\uffff\24\31\11\uffff\15\31\1\114\1\116\1\117\1"
        u"\31\1\121\1\31\1\123\1\124\10\31\1\135\4\31\1\142\2\31\1\145\1"
        u"\146\1\147\1\150\1\31\1\uffff\1\31\2\uffff\1\31\1\uffff\1\31\2"
        u"\uffff\1\155\1\156\6\31\1\uffff\4\31\1\uffff\1\171\1\172\4\uffff"
        u"\4\31\2\uffff\1\177\1\u0080\1\31\1\u0082\1\u0083\1\u0084\1\31\1"
        u"\u0086\1\u0087\1\31\2\uffff\1\u0089\1\u008a\2\31\2\uffff\1\u008d"
        u"\3\uffff\1\u008e\2\uffff\1\u008f\2\uffff\2\31\3\uffff\1\u0092\1"
        u"\u0093\2\uffff"
        )

    DFA2_eof = DFA.unpack(
        u"\u0094\uffff"
        )

    DFA2_min = DFA.unpack(
        u"\1\11\1\157\1\145\2\uffff\1\110\1\150\1\105\1\151\1\111\1\156\1"
        u"\116\1\162\1\122\1\145\1\105\1\156\1\116\1\157\1\117\1\151\1\111"
        u"\1\117\1\145\1\105\11\uffff\1\165\1\155\1\154\1\105\1\145\1\114"
        u"\1\115\1\156\1\116\1\144\1\143\1\104\1\103\3\43\1\164\1\43\1\124"
        u"\2\43\1\164\1\124\1\153\1\113\1\125\1\163\1\123\1\156\1\43\1\145"
        u"\1\122\1\162\1\105\1\43\1\144\1\104\4\43\1\145\1\uffff\1\105\2"
        u"\uffff\1\167\1\uffff\1\127\2\uffff\2\43\1\145\1\105\1\116\1\143"
        u"\1\103\1\164\1\uffff\1\143\1\105\1\145\1\103\1\uffff\2\43\4\uffff"
        u"\1\162\1\122\1\145\1\105\2\uffff\2\43\1\124\3\43\1\164\2\43\1\124"
        u"\2\uffff\2\43\1\145\1\105\2\uffff\1\43\3\uffff\1\43\2\uffff\1\43"
        u"\2\uffff\1\156\1\116\3\uffff\2\43\2\uffff"
        )

    DFA2_max = DFA.unpack(
        u"\1\172\1\157\1\165\2\uffff\1\110\1\150\1\125\1\151\1\111\1\163"
        u"\1\123\1\162\1\122\1\171\1\131\1\156\1\116\1\157\1\117\1\151\1"
        u"\111\1\117\1\145\1\105\11\uffff\1\165\1\155\1\154\1\105\1\145\1"
        u"\114\1\115\1\156\1\116\1\144\1\143\1\104\1\103\3\172\1\164\1\172"
        u"\1\124\2\172\1\164\1\124\1\153\1\113\1\125\1\163\1\123\1\156\1"
        u"\172\1\145\1\122\1\162\1\105\1\172\1\144\1\104\4\172\1\145\1\uffff"
        u"\1\105\2\uffff\1\167\1\uffff\1\127\2\uffff\2\172\1\145\1\105\1"
        u"\116\1\143\1\103\1\164\1\uffff\1\143\1\105\1\145\1\103\1\uffff"
        u"\2\172\4\uffff\1\162\1\122\1\145\1\105\2\uffff\2\172\1\124\3\172"
        u"\1\164\2\172\1\124\2\uffff\2\172\1\145\1\105\2\uffff\1\172\3\uffff"
        u"\1\172\2\uffff\1\172\2\uffff\1\156\1\116\3\uffff\2\172\2\uffff"
        )

    DFA2_accept = DFA.unpack(
        u"\3\uffff\1\3\1\4\24\uffff\1\41\1\42\1\43\1\44\1\45\1\46\1\47\1"
        u"\50\1\51\52\uffff\1\21\1\uffff\1\22\1\17\1\uffff\1\20\1\uffff\1"
        u"\23\1\24\10\uffff\1\2\4\uffff\1\32\2\uffff\1\13\1\33\1\14\1\34"
        u"\4\uffff\1\25\1\26\12\uffff\1\11\1\12\4\uffff\1\27\1\30\1\uffff"
        u"\1\35\1\36\1\1\1\uffff\1\5\1\6\1\uffff\1\15\1\16\2\uffff\1\31\1"
        u"\7\1\10\2\uffff\1\37\1\40"
        )

    DFA2_special = DFA.unpack(
        u"\u0094\uffff"
        )

            
    DFA2_transition = [
        DFA.unpack(u"\2\41\2\uffff\1\41\22\uffff\1\41\1\37\4\uffff\1\40\1"
        u"\uffff\1\3\1\4\2\uffff\1\32\1\uffff\1\33\1\uffff\12\31\2\uffff"
        u"\1\35\1\36\1\34\2\uffff\1\13\1\17\1\26\1\30\1\31\1\11\2\31\1\21"
        u"\2\31\1\25\1\31\1\23\1\15\3\31\1\7\3\31\1\5\3\31\6\uffff\1\12\1"
        u"\16\1\1\1\27\1\31\1\10\2\31\1\20\2\31\1\24\1\31\1\22\1\14\3\31"
        u"\1\2\3\31\1\6\3\31"),
        DFA.unpack(u"\1\42"),
        DFA.unpack(u"\1\44\17\uffff\1\43"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\45"),
        DFA.unpack(u"\1\46"),
        DFA.unpack(u"\1\47\17\uffff\1\50"),
        DFA.unpack(u"\1\51"),
        DFA.unpack(u"\1\52"),
        DFA.unpack(u"\1\53\4\uffff\1\54"),
        DFA.unpack(u"\1\55\4\uffff\1\56"),
        DFA.unpack(u"\1\57"),
        DFA.unpack(u"\1\60"),
        DFA.unpack(u"\1\62\23\uffff\1\61"),
        DFA.unpack(u"\1\64\23\uffff\1\63"),
        DFA.unpack(u"\1\65"),
        DFA.unpack(u"\1\66"),
        DFA.unpack(u"\1\67"),
        DFA.unpack(u"\1\70"),
        DFA.unpack(u"\1\71"),
        DFA.unpack(u"\1\72"),
        DFA.unpack(u"\1\73"),
        DFA.unpack(u"\1\74"),
        DFA.unpack(u"\1\75"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\76"),
        DFA.unpack(u"\1\77"),
        DFA.unpack(u"\1\100"),
        DFA.unpack(u"\1\101"),
        DFA.unpack(u"\1\102"),
        DFA.unpack(u"\1\103"),
        DFA.unpack(u"\1\104"),
        DFA.unpack(u"\1\105"),
        DFA.unpack(u"\1\106"),
        DFA.unpack(u"\1\107"),
        DFA.unpack(u"\1\110"),
        DFA.unpack(u"\1\111"),
        DFA.unpack(u"\1\112"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\3\31\1\113\26\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\3\31\1\115\26\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\120"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\122"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\125"),
        DFA.unpack(u"\1\126"),
        DFA.unpack(u"\1\127"),
        DFA.unpack(u"\1\130"),
        DFA.unpack(u"\1\131"),
        DFA.unpack(u"\1\132"),
        DFA.unpack(u"\1\133"),
        DFA.unpack(u"\1\134"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\136"),
        DFA.unpack(u"\1\137"),
        DFA.unpack(u"\1\140"),
        DFA.unpack(u"\1\141"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\143"),
        DFA.unpack(u"\1\144"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\151"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\152"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\153"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\154"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\157"),
        DFA.unpack(u"\1\160"),
        DFA.unpack(u"\1\161"),
        DFA.unpack(u"\1\162"),
        DFA.unpack(u"\1\163"),
        DFA.unpack(u"\1\164"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\165"),
        DFA.unpack(u"\1\166"),
        DFA.unpack(u"\1\167"),
        DFA.unpack(u"\1\170"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\173"),
        DFA.unpack(u"\1\174"),
        DFA.unpack(u"\1\175"),
        DFA.unpack(u"\1\176"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\u0081"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\u0085"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\u0088"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\u008b"),
        DFA.unpack(u"\1\u008c"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0090"),
        DFA.unpack(u"\1\u0091"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u"\1\31\1\uffff\2\31\3\uffff\1\31\2\uffff\16\31\6\uffff"
        u"\32\31\4\uffff\1\31\1\uffff\32\31"),
        DFA.unpack(u""),
        DFA.unpack(u"")
    ]

    # class definition for DFA #2

    class DFA2(DFA):
        pass


 



def main(argv, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    from antlr3.main import LexerMain
    main = LexerMain(SqlDASLexer)
    main.stdin = stdin
    main.stdout = stdout
    main.stderr = stderr
    main.execute(argv)


if __name__ == '__main__':
    main(sys.argv)
