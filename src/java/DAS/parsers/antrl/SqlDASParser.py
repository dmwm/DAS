from __future__ import absolute_import
# $ANTLR 3.1.3 Mar 17, 2009 19:23:44 SqlDAS.g 2009-05-14 13:46:34

import sys
from antlr3 import *
from antlr3.compat import set, frozenset
          
from .ValidateKeyword import *


kws = []
okws = []
constraints = []
orderingkw = []




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

# token names
tokenNames = [
    "<invalid>", "<EOR>", "<DOWN>", "<UP>", 
    "KW", "COMMA", "EQ", "LT", "GT", "NOT", "DOT", "AMP", "WS", "'count'", 
    "'sum'", "'('", "')'", "'WHERE'", "'where'", "'select'", "'SELECT'", 
    "'find'", "'FIND'", "'and'", "'AND'", "'order'", "'ORDER'", "'by'", 
    "'BY'", "'or'", "'OR'", "'in'", "'IN'", "'not'", "'NOT'", "'like'", 
    "'LIKE'", "'COUNT'", "'SUM'", "'asc'", "'ASC'", "'desc'", "'DESC'", 
    "'between'", "'BETWEEN'"
]




class SqlDASParser(Parser):
    grammarFileName = "SqlDAS.g"
    antlr_version = version_str_to_tuple("3.1.3 Mar 17, 2009 19:23:44")
    antlr_version_str = "3.1.3 Mar 17, 2009 19:23:44"
    tokenNames = tokenNames

    def __init__(self, input, state=None, *args, **kwargs):
        if state is None:
            state = RecognizerSharedState()

        super(SqlDASParser, self).__init__(input, state, *args, **kwargs)

        self.dfa3 = self.DFA3(
            self, 3,
            eot = self.DFA3_eot,
            eof = self.DFA3_eof,
            min = self.DFA3_min,
            max = self.DFA3_max,
            accept = self.DFA3_accept,
            special = self.DFA3_special,
            transition = self.DFA3_transition
            )

        self.dfa16 = self.DFA16(
            self, 16,
            eot = self.DFA16_eot,
            eof = self.DFA16_eof,
            min = self.DFA16_min,
            max = self.DFA16_max,
            accept = self.DFA16_accept,
            special = self.DFA16_special,
            transition = self.DFA16_transition
            )






                


        

              	




    # $ANTLR start "stmt"
    # SqlDAS.g:29:1: stmt : select selectList ( where constraintList )? ( order by orderList )? ;
    def stmt(self, ):

        try:
            try:
                # SqlDAS.g:29:6: ( select selectList ( where constraintList )? ( order by orderList )? )
                # SqlDAS.g:29:8: select selectList ( where constraintList )? ( order by orderList )?
                pass 
                self._state.following.append(self.FOLLOW_select_in_stmt42)
                self.select()

                self._state.following.pop()
                self._state.following.append(self.FOLLOW_selectList_in_stmt44)
                self.selectList()

                self._state.following.pop()
                # SqlDAS.g:29:26: ( where constraintList )?
                alt1 = 2
                LA1_0 = self.input.LA(1)

                if ((17 <= LA1_0 <= 18)) :
                    alt1 = 1
                if alt1 == 1:
                    # SqlDAS.g:29:27: where constraintList
                    pass 
                    self._state.following.append(self.FOLLOW_where_in_stmt47)
                    self.where()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_constraintList_in_stmt49)
                    self.constraintList()

                    self._state.following.pop()



                # SqlDAS.g:29:50: ( order by orderList )?
                alt2 = 2
                LA2_0 = self.input.LA(1)

                if ((25 <= LA2_0 <= 26)) :
                    alt2 = 1
                if alt2 == 1:
                    # SqlDAS.g:29:51: order by orderList
                    pass 
                    self._state.following.append(self.FOLLOW_order_in_stmt54)
                    self.order()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_by_in_stmt56)
                    self.by()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_orderList_in_stmt58)
                    self.orderList()

                    self._state.following.pop()







                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "stmt"


    # $ANTLR start "orderList"
    # SqlDAS.g:32:1: orderList : ( orderList1 | orderList1 orderKw );
    def orderList(self, ):

        try:
            try:
                # SqlDAS.g:32:11: ( orderList1 | orderList1 orderKw )
                alt3 = 2
                alt3 = self.dfa3.predict(self.input)
                if alt3 == 1:
                    # SqlDAS.g:32:12: orderList1
                    pass 
                    self._state.following.append(self.FOLLOW_orderList1_in_orderList68)
                    self.orderList1()

                    self._state.following.pop()


                elif alt3 == 2:
                    # SqlDAS.g:33:5: orderList1 orderKw
                    pass 
                    self._state.following.append(self.FOLLOW_orderList1_in_orderList74)
                    self.orderList1()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_orderKw_in_orderList76)
                    self.orderKw()

                    self._state.following.pop()



                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "orderList"


    # $ANTLR start "orderList1"
    # SqlDAS.g:35:1: orderList1 : okw= KW ( COMMA okw= KW )* ;
    def orderList1(self, ):

        okw = None

        try:
            try:
                # SqlDAS.g:35:12: (okw= KW ( COMMA okw= KW )* )
                # SqlDAS.g:35:13: okw= KW ( COMMA okw= KW )*
                pass 
                okw=self.match(self.input, KW, self.FOLLOW_KW_in_orderList186)
                #action start
                okws.append(str(okw.text))
                #action end
                #action start
                validateKw(okw.text)
                #action end
                # SqlDAS.g:36:4: ( COMMA okw= KW )*
                while True: #loop4
                    alt4 = 2
                    LA4_0 = self.input.LA(1)

                    if (LA4_0 == COMMA) :
                        alt4 = 1


                    if alt4 == 1:
                        # SqlDAS.g:37:3: COMMA okw= KW
                        pass 
                        self.match(self.input, COMMA, self.FOLLOW_COMMA_in_orderList1101)
                        okw=self.match(self.input, KW, self.FOLLOW_KW_in_orderList1108)
                        #action start
                        okws.append(str(okw.text))
                        #action end
                        #action start
                        validateKw(okw.text)
                        #action end


                    else:
                        break #loop4




                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "orderList1"


    # $ANTLR start "orderKw"
    # SqlDAS.g:41:1: orderKw : oing= ordering ;
    def orderKw(self, ):

        oing = None


        try:
            try:
                # SqlDAS.g:41:10: (oing= ordering )
                # SqlDAS.g:41:11: oing= ordering
                pass 
                self._state.following.append(self.FOLLOW_ordering_in_orderKw134)
                oing = self.ordering()

                self._state.following.pop()
                #action start
                orderingkw.append(str(((oing is not None) and [self.input.toString(oing.start,oing.stop)] or [None])[0]))
                #action end




                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "orderKw"

    class ordering_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.ordering_return, self).__init__()





    # $ANTLR start "ordering"
    # SqlDAS.g:43:1: ordering : ( asc | desc );
    def ordering(self, ):

        retval = self.ordering_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:43:11: ( asc | desc )
                alt5 = 2
                LA5_0 = self.input.LA(1)

                if ((39 <= LA5_0 <= 40)) :
                    alt5 = 1
                elif ((41 <= LA5_0 <= 42)) :
                    alt5 = 2
                else:
                    nvae = NoViableAltException("", 5, 0, self.input)

                    raise nvae

                if alt5 == 1:
                    # SqlDAS.g:43:12: asc
                    pass 
                    self._state.following.append(self.FOLLOW_asc_in_ordering146)
                    self.asc()

                    self._state.following.pop()


                elif alt5 == 2:
                    # SqlDAS.g:43:16: desc
                    pass 
                    self._state.following.append(self.FOLLOW_desc_in_ordering148)
                    self.desc()

                    self._state.following.pop()


                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "ordering"


    # $ANTLR start "selectList"
    # SqlDAS.g:44:1: selectList : kw= gkw ( COMMA kw= gkw )* ;
    def selectList(self, ):

        kw = None


        try:
            try:
                # SqlDAS.g:44:12: (kw= gkw ( COMMA kw= gkw )* )
                # SqlDAS.g:44:13: kw= gkw ( COMMA kw= gkw )*
                pass 
                self._state.following.append(self.FOLLOW_gkw_in_selectList159)
                kw = self.gkw()

                self._state.following.pop()
                #action start
                kws.append(str(((kw is not None) and [self.input.toString(kw.start,kw.stop)] or [None])[0]))
                #action end
                #action start
                validateKw(((kw is not None) and [self.input.toString(kw.start,kw.stop)] or [None])[0])
                #action end
                # SqlDAS.g:45:4: ( COMMA kw= gkw )*
                while True: #loop6
                    alt6 = 2
                    LA6_0 = self.input.LA(1)

                    if (LA6_0 == COMMA) :
                        alt6 = 1


                    if alt6 == 1:
                        # SqlDAS.g:46:3: COMMA kw= gkw
                        pass 
                        self.match(self.input, COMMA, self.FOLLOW_COMMA_in_selectList174)
                        self._state.following.append(self.FOLLOW_gkw_in_selectList181)
                        kw = self.gkw()

                        self._state.following.pop()
                        #action start
                        kws.append(str(((kw is not None) and [self.input.toString(kw.start,kw.stop)] or [None])[0]))
                        #action end
                        #action start
                        validateKw(((kw is not None) and [self.input.toString(kw.start,kw.stop)] or [None])[0])
                        #action end


                    else:
                        break #loop6




                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "selectList"

    class gkw_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.gkw_return, self).__init__()





    # $ANTLR start "gkw"
    # SqlDAS.g:52:1: gkw : ( KW | ( ( 'count' | 'sum' ) '(' KW ')' ) );
    def gkw(self, ):

        retval = self.gkw_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:52:5: ( KW | ( ( 'count' | 'sum' ) '(' KW ')' ) )
                alt7 = 2
                LA7_0 = self.input.LA(1)

                if (LA7_0 == KW) :
                    alt7 = 1
                elif ((13 <= LA7_0 <= 14)) :
                    alt7 = 2
                else:
                    nvae = NoViableAltException("", 7, 0, self.input)

                    raise nvae

                if alt7 == 1:
                    # SqlDAS.g:52:7: KW
                    pass 
                    self.match(self.input, KW, self.FOLLOW_KW_in_gkw324)


                elif alt7 == 2:
                    # SqlDAS.g:52:12: ( ( 'count' | 'sum' ) '(' KW ')' )
                    pass 
                    # SqlDAS.g:52:12: ( ( 'count' | 'sum' ) '(' KW ')' )
                    # SqlDAS.g:52:13: ( 'count' | 'sum' ) '(' KW ')'
                    pass 
                    if (13 <= self.input.LA(1) <= 14):
                        self.input.consume()
                        self._state.errorRecovery = False

                    else:
                        mse = MismatchedSetException(None, self.input)
                        raise mse


                    self.match(self.input, 15, self.FOLLOW_15_in_gkw335)
                    self.match(self.input, KW, self.FOLLOW_KW_in_gkw338)
                    self.match(self.input, 16, self.FOLLOW_16_in_gkw341)





                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "gkw"


    # $ANTLR start "constraintList"
    # SqlDAS.g:54:1: constraintList : constraint1 (rel= logicalOp constraint1 )* ;
    def constraintList(self, ):

        rel = None


        try:
            try:
                # SqlDAS.g:54:16: ( constraint1 (rel= logicalOp constraint1 )* )
                # SqlDAS.g:54:18: constraint1 (rel= logicalOp constraint1 )*
                pass 
                self._state.following.append(self.FOLLOW_constraint1_in_constraintList353)
                self.constraint1()

                self._state.following.pop()
                # SqlDAS.g:54:30: (rel= logicalOp constraint1 )*
                while True: #loop8
                    alt8 = 2
                    LA8_0 = self.input.LA(1)

                    if ((23 <= LA8_0 <= 24) or (29 <= LA8_0 <= 30)) :
                        alt8 = 1


                    if alt8 == 1:
                        # SqlDAS.g:55:2: rel= logicalOp constraint1
                        pass 
                        self._state.following.append(self.FOLLOW_logicalOp_in_constraintList363)
                        rel = self.logicalOp()

                        self._state.following.pop()
                        #action start
                        constraints.append(str(((rel is not None) and [self.input.toString(rel.start,rel.stop)] or [None])[0]));
                        #action end
                        self._state.following.append(self.FOLLOW_constraint1_in_constraintList372)
                        self.constraint1()

                        self._state.following.pop()


                    else:
                        break #loop8




                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "constraintList"

    class lopen_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.lopen_return, self).__init__()





    # $ANTLR start "lopen"
    # SqlDAS.g:58:1: lopen : lb ( lb )* ;
    def lopen(self, ):

        retval = self.lopen_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:58:8: ( lb ( lb )* )
                # SqlDAS.g:58:10: lb ( lb )*
                pass 
                self._state.following.append(self.FOLLOW_lb_in_lopen383)
                self.lb()

                self._state.following.pop()
                # SqlDAS.g:58:12: ( lb )*
                while True: #loop9
                    alt9 = 2
                    LA9_0 = self.input.LA(1)

                    if (LA9_0 == 15) :
                        alt9 = 1


                    if alt9 == 1:
                        # SqlDAS.g:58:13: lb
                        pass 
                        self._state.following.append(self.FOLLOW_lb_in_lopen385)
                        self.lb()

                        self._state.following.pop()


                    else:
                        break #loop9



                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "lopen"

    class ropen_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.ropen_return, self).__init__()





    # $ANTLR start "ropen"
    # SqlDAS.g:59:1: ropen : rb ( rb )* ;
    def ropen(self, ):

        retval = self.ropen_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:59:8: ( rb ( rb )* )
                # SqlDAS.g:59:10: rb ( rb )*
                pass 
                self._state.following.append(self.FOLLOW_rb_in_ropen395)
                self.rb()

                self._state.following.pop()
                # SqlDAS.g:59:12: ( rb )*
                while True: #loop10
                    alt10 = 2
                    LA10_0 = self.input.LA(1)

                    if (LA10_0 == 16) :
                        alt10 = 1


                    if alt10 == 1:
                        # SqlDAS.g:59:13: rb
                        pass 
                        self._state.following.append(self.FOLLOW_rb_in_ropen397)
                        self.rb()

                        self._state.following.pop()


                    else:
                        break #loop10



                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "ropen"


    # $ANTLR start "constraint1"
    # SqlDAS.g:60:1: constraint1 : (kl= lopen constraint (rel= logicalOp constraint )* kr= ropen | constraint );
    def constraint1(self, ):

        kl = None

        rel = None

        kr = None


        try:
            try:
                # SqlDAS.g:60:17: (kl= lopen constraint (rel= logicalOp constraint )* kr= ropen | constraint )
                alt12 = 2
                LA12_0 = self.input.LA(1)

                if (LA12_0 == 15) :
                    alt12 = 1
                elif (LA12_0 == KW) :
                    alt12 = 2
                else:
                    nvae = NoViableAltException("", 12, 0, self.input)

                    raise nvae

                if alt12 == 1:
                    # SqlDAS.g:60:19: kl= lopen constraint (rel= logicalOp constraint )* kr= ropen
                    pass 
                    self._state.following.append(self.FOLLOW_lopen_in_constraint1415)
                    kl = self.lopen()

                    self._state.following.pop()
                    #action start
                    c1 = {}
                    #action end
                    #action start
                    c1['bracket'] = str(((kl is not None) and [self.input.toString(kl.start,kl.stop)] or [None])[0])
                    #action end
                    #action start
                    if str(((kl is not None) and [self.input.toString(kl.start,kl.stop)] or [None])[0]) != '': constraints.append(c1)
                    #action end
                    self._state.following.append(self.FOLLOW_constraint_in_constraint1441)
                    self.constraint()

                    self._state.following.pop()
                    # SqlDAS.g:62:3: (rel= logicalOp constraint )*
                    while True: #loop11
                        alt11 = 2
                        LA11_0 = self.input.LA(1)

                        if ((23 <= LA11_0 <= 24) or (29 <= LA11_0 <= 30)) :
                            alt11 = 1


                        if alt11 == 1:
                            # SqlDAS.g:62:4: rel= logicalOp constraint
                            pass 
                            self._state.following.append(self.FOLLOW_logicalOp_in_constraint1453)
                            rel = self.logicalOp()

                            self._state.following.pop()
                            #action start
                            constraints.append(str(((rel is not None) and [self.input.toString(rel.start,rel.stop)] or [None])[0]));
                            #action end
                            self._state.following.append(self.FOLLOW_constraint_in_constraint1473)
                            self.constraint()

                            self._state.following.pop()


                        else:
                            break #loop11
                    self._state.following.append(self.FOLLOW_ropen_in_constraint1500)
                    kr = self.ropen()

                    self._state.following.pop()
                    #action start
                    c = {}
                    #action end
                    #action start
                    c['bracket'] = str(((kr is not None) and [self.input.toString(kr.start,kr.stop)] or [None])[0])
                    #action end
                    #action start
                    if str(((kr is not None) and [self.input.toString(kr.start,kr.stop)] or [None])[0]) != '':constraints.append(c)
                    #action end


                elif alt12 == 2:
                    # SqlDAS.g:65:5: constraint
                    pass 
                    self._state.following.append(self.FOLLOW_constraint_in_constraint1514)
                    self.constraint()

                    self._state.following.pop()



                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "constraint1"


    # $ANTLR start "constraint"
    # SqlDAS.g:67:1: constraint : (kw= KW op= compOpt val= spaceValue | kw= KW op1= inpython '(' val1= valueList ')' | kw= KW op2= like val2= KW | kw= KW op3= between val3= betValue );
    def constraint(self, ):

        kw = None
        val2 = None
        op = None

        val = None

        op1 = None

        val1 = None

        op2 = None

        op3 = None

        val3 = None


        try:
            try:
                # SqlDAS.g:67:12: (kw= KW op= compOpt val= spaceValue | kw= KW op1= inpython '(' val1= valueList ')' | kw= KW op2= like val2= KW | kw= KW op3= between val3= betValue )
                alt13 = 4
                LA13_0 = self.input.LA(1)

                if (LA13_0 == KW) :
                    LA13 = self.input.LA(2)
                    if LA13 == 43 or LA13 == 44:
                        alt13 = 4
                    elif LA13 == 31 or LA13 == 32:
                        alt13 = 2
                    elif LA13 == 33 or LA13 == 34 or LA13 == 35 or LA13 == 36:
                        alt13 = 3
                    elif LA13 == EQ or LA13 == LT or LA13 == GT or LA13 == NOT:
                        alt13 = 1
                    else:
                        nvae = NoViableAltException("", 13, 1, self.input)

                        raise nvae

                else:
                    nvae = NoViableAltException("", 13, 0, self.input)

                    raise nvae

                if alt13 == 1:
                    # SqlDAS.g:67:14: kw= KW op= compOpt val= spaceValue
                    pass 
                    kw=self.match(self.input, KW, self.FOLLOW_KW_in_constraint525)
                    #action start
                    c = {}
                    #action end
                    #action start
                    c['key'] = str(kw.text)
                    #action end
                    #action start
                    validateKw(kw.text)
                    #action end
                    self._state.following.append(self.FOLLOW_compOpt_in_constraint539)
                    op = self.compOpt()

                    self._state.following.pop()
                    #action start
                    c['op'] = str(((op is not None) and [self.input.toString(op.start,op.stop)] or [None])[0])
                    #action end
                    self._state.following.append(self.FOLLOW_spaceValue_in_constraint552)
                    val = self.spaceValue()

                    self._state.following.pop()
                    #action start
                    c['value'] = str(((val is not None) and [self.input.toString(val.start,val.stop)] or [None])[0])
                    #action end
                    #action start
                    constraints.append(c)
                    #action end


                elif alt13 == 2:
                    # SqlDAS.g:71:2: kw= KW op1= inpython '(' val1= valueList ')'
                    pass 
                    kw=self.match(self.input, KW, self.FOLLOW_KW_in_constraint567)
                    #action start
                    c = {}
                    #action end
                    #action start
                    c['key'] = str(kw.text)
                    #action end
                    #action start
                    validateKw(kw.text)
                    #action end
                    self._state.following.append(self.FOLLOW_inpython_in_constraint581)
                    op1 = self.inpython()

                    self._state.following.pop()
                    #action start
                    c['op'] = str(((op1 is not None) and [self.input.toString(op1.start,op1.stop)] or [None])[0])
                    #action end
                    self.match(self.input, 15, self.FOLLOW_15_in_constraint592)
                    self._state.following.append(self.FOLLOW_valueList_in_constraint598)
                    val1 = self.valueList()

                    self._state.following.pop()
                    #action start
                    c['value'] = str(((val1 is not None) and [self.input.toString(val1.start,val1.stop)] or [None])[0])
                    #action end
                    #action start
                    constraints.append(c);
                    #action end
                    self.match(self.input, 16, self.FOLLOW_16_in_constraint608)


                elif alt13 == 3:
                    # SqlDAS.g:77:2: kw= KW op2= like val2= KW
                    pass 
                    kw=self.match(self.input, KW, self.FOLLOW_KW_in_constraint634)
                    #action start
                    c = {}
                    #action end
                    #action start
                    c['key'] = str(kw.text)
                    #action end
                    #action start
                    validateKw(kw.text)
                    #action end
                    self._state.following.append(self.FOLLOW_like_in_constraint648)
                    op2 = self.like()

                    self._state.following.pop()
                    #action start
                    c['op'] = str(((op2 is not None) and [self.input.toString(op2.start,op2.stop)] or [None])[0])
                    #action end
                    val2=self.match(self.input, KW, self.FOLLOW_KW_in_constraint659)
                    #action start
                    c['value'] = str(val2.text)
                    #action end
                    #action start
                    constraints.append(c)
                    #action end


                elif alt13 == 4:
                    # SqlDAS.g:81:3: kw= KW op3= between val3= betValue
                    pass 
                    kw=self.match(self.input, KW, self.FOLLOW_KW_in_constraint676)
                    #action start
                    c = {}
                    #action end
                    #action start
                    c['key'] = str(kw.text)
                    #action end
                    #action start
                    validateKw(kw.text)
                    #action end
                    self._state.following.append(self.FOLLOW_between_in_constraint690)
                    op3 = self.between()

                    self._state.following.pop()
                    #action start
                    c['op'] = str(((op3 is not None) and [self.input.toString(op3.start,op3.stop)] or [None])[0])
                    #action end
                    self._state.following.append(self.FOLLOW_betValue_in_constraint700)
                    val3 = self.betValue()

                    self._state.following.pop()
                    #action start
                    c['value'] = str(((val3 is not None) and [self.input.toString(val3.start,val3.stop)] or [None])[0])
                    #action end
                    #action start
                    constraints.append(c)
                    #action end



                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "constraint"

    class spaceValue_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.spaceValue_return, self).__init__()





    # $ANTLR start "spaceValue"
    # SqlDAS.g:85:1: spaceValue : KW ( KW )* ;
    def spaceValue(self, ):

        retval = self.spaceValue_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:85:12: ( KW ( KW )* )
                # SqlDAS.g:85:14: KW ( KW )*
                pass 
                self.match(self.input, KW, self.FOLLOW_KW_in_spaceValue749)
                # SqlDAS.g:85:17: ( KW )*
                while True: #loop14
                    alt14 = 2
                    LA14_0 = self.input.LA(1)

                    if (LA14_0 == KW) :
                        alt14 = 1


                    if alt14 == 1:
                        # SqlDAS.g:85:18: KW
                        pass 
                        self.match(self.input, KW, self.FOLLOW_KW_in_spaceValue752)


                    else:
                        break #loop14



                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "spaceValue"

    class valueList_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.valueList_return, self).__init__()





    # $ANTLR start "valueList"
    # SqlDAS.g:86:1: valueList : KW ( COMMA KW )* ;
    def valueList(self, ):

        retval = self.valueList_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:86:11: ( KW ( COMMA KW )* )
                # SqlDAS.g:86:13: KW ( COMMA KW )*
                pass 
                self.match(self.input, KW, self.FOLLOW_KW_in_valueList761)
                # SqlDAS.g:86:16: ( COMMA KW )*
                while True: #loop15
                    alt15 = 2
                    LA15_0 = self.input.LA(1)

                    if (LA15_0 == COMMA) :
                        alt15 = 1


                    if alt15 == 1:
                        # SqlDAS.g:86:18: COMMA KW
                        pass 
                        self.match(self.input, COMMA, self.FOLLOW_COMMA_in_valueList765)
                        self.match(self.input, KW, self.FOLLOW_KW_in_valueList767)


                    else:
                        break #loop15



                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "valueList"

    class betValue_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.betValue_return, self).__init__()





    # $ANTLR start "betValue"
    # SqlDAS.g:87:1: betValue : KW andpython KW ;
    def betValue(self, ):

        retval = self.betValue_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:87:10: ( KW andpython KW )
                # SqlDAS.g:87:12: KW andpython KW
                pass 
                self.match(self.input, KW, self.FOLLOW_KW_in_betValue777)
                self._state.following.append(self.FOLLOW_andpython_in_betValue779)
                self.andpython()

                self._state.following.pop()
                self.match(self.input, KW, self.FOLLOW_KW_in_betValue781)



                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "betValue"


    # $ANTLR start "where"
    # SqlDAS.g:88:1: where : ( 'WHERE' | 'where' ) ;
    def where(self, ):

        try:
            try:
                # SqlDAS.g:88:7: ( ( 'WHERE' | 'where' ) )
                # SqlDAS.g:88:8: ( 'WHERE' | 'where' )
                pass 
                if (17 <= self.input.LA(1) <= 18):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "where"

    class compOpt_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.compOpt_return, self).__init__()





    # $ANTLR start "compOpt"
    # SqlDAS.g:89:1: compOpt : ( ( EQ ) | ( LT ) | ( GT ) | ( NOT ) ( EQ ) | ( EQ ) ( GT ) | ( EQ ) ( LT ) | ( LT ) ( EQ ) | ( GT ) ( EQ ) | ( LT ) ( GT ) );
    def compOpt(self, ):

        retval = self.compOpt_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:89:10: ( ( EQ ) | ( LT ) | ( GT ) | ( NOT ) ( EQ ) | ( EQ ) ( GT ) | ( EQ ) ( LT ) | ( LT ) ( EQ ) | ( GT ) ( EQ ) | ( LT ) ( GT ) )
                alt16 = 9
                alt16 = self.dfa16.predict(self.input)
                if alt16 == 1:
                    # SqlDAS.g:89:11: ( EQ )
                    pass 
                    # SqlDAS.g:89:11: ( EQ )
                    # SqlDAS.g:89:12: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt801)





                elif alt16 == 2:
                    # SqlDAS.g:90:4: ( LT )
                    pass 
                    # SqlDAS.g:90:4: ( LT )
                    # SqlDAS.g:90:5: LT
                    pass 
                    self.match(self.input, LT, self.FOLLOW_LT_in_compOpt808)





                elif alt16 == 3:
                    # SqlDAS.g:91:4: ( GT )
                    pass 
                    # SqlDAS.g:91:4: ( GT )
                    # SqlDAS.g:91:5: GT
                    pass 
                    self.match(self.input, GT, self.FOLLOW_GT_in_compOpt815)





                elif alt16 == 4:
                    # SqlDAS.g:92:4: ( NOT ) ( EQ )
                    pass 
                    # SqlDAS.g:92:4: ( NOT )
                    # SqlDAS.g:92:5: NOT
                    pass 
                    self.match(self.input, NOT, self.FOLLOW_NOT_in_compOpt822)



                    # SqlDAS.g:92:9: ( EQ )
                    # SqlDAS.g:92:10: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt825)





                elif alt16 == 5:
                    # SqlDAS.g:93:4: ( EQ ) ( GT )
                    pass 
                    # SqlDAS.g:93:4: ( EQ )
                    # SqlDAS.g:93:5: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt832)



                    # SqlDAS.g:93:8: ( GT )
                    # SqlDAS.g:93:9: GT
                    pass 
                    self.match(self.input, GT, self.FOLLOW_GT_in_compOpt835)





                elif alt16 == 6:
                    # SqlDAS.g:94:4: ( EQ ) ( LT )
                    pass 
                    # SqlDAS.g:94:4: ( EQ )
                    # SqlDAS.g:94:5: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt842)



                    # SqlDAS.g:94:8: ( LT )
                    # SqlDAS.g:94:9: LT
                    pass 
                    self.match(self.input, LT, self.FOLLOW_LT_in_compOpt845)





                elif alt16 == 7:
                    # SqlDAS.g:95:4: ( LT ) ( EQ )
                    pass 
                    # SqlDAS.g:95:4: ( LT )
                    # SqlDAS.g:95:5: LT
                    pass 
                    self.match(self.input, LT, self.FOLLOW_LT_in_compOpt852)



                    # SqlDAS.g:95:8: ( EQ )
                    # SqlDAS.g:95:9: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt855)





                elif alt16 == 8:
                    # SqlDAS.g:96:4: ( GT ) ( EQ )
                    pass 
                    # SqlDAS.g:96:4: ( GT )
                    # SqlDAS.g:96:5: GT
                    pass 
                    self.match(self.input, GT, self.FOLLOW_GT_in_compOpt862)



                    # SqlDAS.g:96:8: ( EQ )
                    # SqlDAS.g:96:9: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt865)





                elif alt16 == 9:
                    # SqlDAS.g:97:4: ( LT ) ( GT )
                    pass 
                    # SqlDAS.g:97:4: ( LT )
                    # SqlDAS.g:97:5: LT
                    pass 
                    self.match(self.input, LT, self.FOLLOW_LT_in_compOpt872)



                    # SqlDAS.g:97:8: ( GT )
                    # SqlDAS.g:97:9: GT
                    pass 
                    self.match(self.input, GT, self.FOLLOW_GT_in_compOpt875)





                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "compOpt"

    class logicalOp_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.logicalOp_return, self).__init__()





    # $ANTLR start "logicalOp"
    # SqlDAS.g:99:1: logicalOp : ( andpython | orpython ) ;
    def logicalOp(self, ):

        retval = self.logicalOp_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:99:11: ( ( andpython | orpython ) )
                # SqlDAS.g:99:12: ( andpython | orpython )
                pass 
                # SqlDAS.g:99:12: ( andpython | orpython )
                alt17 = 2
                LA17_0 = self.input.LA(1)

                if ((23 <= LA17_0 <= 24)) :
                    alt17 = 1
                elif ((29 <= LA17_0 <= 30)) :
                    alt17 = 2
                else:
                    nvae = NoViableAltException("", 17, 0, self.input)

                    raise nvae

                if alt17 == 1:
                    # SqlDAS.g:99:13: andpython
                    pass 
                    self._state.following.append(self.FOLLOW_andpython_in_logicalOp884)
                    self.andpython()

                    self._state.following.pop()


                elif alt17 == 2:
                    # SqlDAS.g:99:23: orpython
                    pass 
                    self._state.following.append(self.FOLLOW_orpython_in_logicalOp886)
                    self.orpython()

                    self._state.following.pop()






                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "logicalOp"


    # $ANTLR start "select"
    # SqlDAS.g:100:1: select : ( 'select' | 'SELECT' | 'find' | 'FIND' ) ;
    def select(self, ):

        try:
            try:
                # SqlDAS.g:100:9: ( ( 'select' | 'SELECT' | 'find' | 'FIND' ) )
                # SqlDAS.g:100:10: ( 'select' | 'SELECT' | 'find' | 'FIND' )
                pass 
                if (19 <= self.input.LA(1) <= 22):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "select"


    # $ANTLR start "andpython"
    # SqlDAS.g:101:1: andpython : ( 'and' | 'AND' ) ;
    def andpython(self, ):

        try:
            try:
                # SqlDAS.g:101:11: ( ( 'and' | 'AND' ) )
                # SqlDAS.g:101:12: ( 'and' | 'AND' )
                pass 
                if (23 <= self.input.LA(1) <= 24):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "andpython"


    # $ANTLR start "order"
    # SqlDAS.g:102:1: order : ( 'order' | 'ORDER' ) ;
    def order(self, ):

        try:
            try:
                # SqlDAS.g:102:8: ( ( 'order' | 'ORDER' ) )
                # SqlDAS.g:102:9: ( 'order' | 'ORDER' )
                pass 
                if (25 <= self.input.LA(1) <= 26):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "order"


    # $ANTLR start "by"
    # SqlDAS.g:103:1: by : ( 'by' | 'BY' ) ;
    def by(self, ):

        try:
            try:
                # SqlDAS.g:103:5: ( ( 'by' | 'BY' ) )
                # SqlDAS.g:103:6: ( 'by' | 'BY' )
                pass 
                if (27 <= self.input.LA(1) <= 28):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "by"


    # $ANTLR start "orpython"
    # SqlDAS.g:104:1: orpython : ( 'or' | 'OR' ) ;
    def orpython(self, ):

        try:
            try:
                # SqlDAS.g:104:10: ( ( 'or' | 'OR' ) )
                # SqlDAS.g:104:11: ( 'or' | 'OR' )
                pass 
                if (29 <= self.input.LA(1) <= 30):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "orpython"

    class inpython_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.inpython_return, self).__init__()





    # $ANTLR start "inpython"
    # SqlDAS.g:105:1: inpython : ( 'in' | 'IN' ) ;
    def inpython(self, ):

        retval = self.inpython_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:105:10: ( ( 'in' | 'IN' ) )
                # SqlDAS.g:105:11: ( 'in' | 'IN' )
                pass 
                if (31 <= self.input.LA(1) <= 32):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse





                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "inpython"


    # $ANTLR start "notpython"
    # SqlDAS.g:106:1: notpython : ( 'not' | 'NOT' ) ;
    def notpython(self, ):

        try:
            try:
                # SqlDAS.g:106:11: ( ( 'not' | 'NOT' ) )
                # SqlDAS.g:106:12: ( 'not' | 'NOT' )
                pass 
                if (33 <= self.input.LA(1) <= 34):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "notpython"

    class like_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.like_return, self).__init__()





    # $ANTLR start "like"
    # SqlDAS.g:107:1: like : ( 'like' | 'LIKE' | 'not' 'like' | 'NOT' 'LIKE' ) ;
    def like(self, ):

        retval = self.like_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:107:7: ( ( 'like' | 'LIKE' | 'not' 'like' | 'NOT' 'LIKE' ) )
                # SqlDAS.g:107:8: ( 'like' | 'LIKE' | 'not' 'like' | 'NOT' 'LIKE' )
                pass 
                # SqlDAS.g:107:8: ( 'like' | 'LIKE' | 'not' 'like' | 'NOT' 'LIKE' )
                alt18 = 4
                LA18 = self.input.LA(1)
                if LA18 == 35:
                    alt18 = 1
                elif LA18 == 36:
                    alt18 = 2
                elif LA18 == 33:
                    alt18 = 3
                elif LA18 == 34:
                    alt18 = 4
                else:
                    nvae = NoViableAltException("", 18, 0, self.input)

                    raise nvae

                if alt18 == 1:
                    # SqlDAS.g:107:9: 'like'
                    pass 
                    self.match(self.input, 35, self.FOLLOW_35_in_like990)


                elif alt18 == 2:
                    # SqlDAS.g:107:18: 'LIKE'
                    pass 
                    self.match(self.input, 36, self.FOLLOW_36_in_like994)


                elif alt18 == 3:
                    # SqlDAS.g:107:27: 'not' 'like'
                    pass 
                    self.match(self.input, 33, self.FOLLOW_33_in_like998)
                    self.match(self.input, 35, self.FOLLOW_35_in_like1000)


                elif alt18 == 4:
                    # SqlDAS.g:107:42: 'NOT' 'LIKE'
                    pass 
                    self.match(self.input, 34, self.FOLLOW_34_in_like1004)
                    self.match(self.input, 36, self.FOLLOW_36_in_like1006)






                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "like"


    # $ANTLR start "count"
    # SqlDAS.g:108:1: count : ( 'count' | 'COUNT' ) ;
    def count(self, ):

        try:
            try:
                # SqlDAS.g:108:8: ( ( 'count' | 'COUNT' ) )
                # SqlDAS.g:108:9: ( 'count' | 'COUNT' )
                pass 
                if self.input.LA(1) == 13 or self.input.LA(1) == 37:
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "count"


    # $ANTLR start "sum"
    # SqlDAS.g:109:1: sum : ( 'sum' | 'SUM' ) ;
    def sum(self, ):

        try:
            try:
                # SqlDAS.g:109:6: ( ( 'sum' | 'SUM' ) )
                # SqlDAS.g:109:7: ( 'sum' | 'SUM' )
                pass 
                if self.input.LA(1) == 14 or self.input.LA(1) == 38:
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "sum"


    # $ANTLR start "asc"
    # SqlDAS.g:110:1: asc : ( 'asc' | 'ASC' ) ;
    def asc(self, ):

        try:
            try:
                # SqlDAS.g:110:6: ( ( 'asc' | 'ASC' ) )
                # SqlDAS.g:110:7: ( 'asc' | 'ASC' )
                pass 
                if (39 <= self.input.LA(1) <= 40):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "asc"


    # $ANTLR start "desc"
    # SqlDAS.g:111:1: desc : ( 'desc' | 'DESC' ) ;
    def desc(self, ):

        try:
            try:
                # SqlDAS.g:111:7: ( ( 'desc' | 'DESC' ) )
                # SqlDAS.g:111:8: ( 'desc' | 'DESC' )
                pass 
                if (41 <= self.input.LA(1) <= 42):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse






                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "desc"

    class between_return(ParserRuleReturnScope):
        def __init__(self):
            super(SqlDASParser.between_return, self).__init__()





    # $ANTLR start "between"
    # SqlDAS.g:112:1: between : ( 'between' | 'BETWEEN' ) ;
    def between(self, ):

        retval = self.between_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:112:10: ( ( 'between' | 'BETWEEN' ) )
                # SqlDAS.g:112:11: ( 'between' | 'BETWEEN' )
                pass 
                if (43 <= self.input.LA(1) <= 44):
                    self.input.consume()
                    self._state.errorRecovery = False

                else:
                    mse = MismatchedSetException(None, self.input)
                    raise mse





                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass
        return retval

    # $ANTLR end "between"


    # $ANTLR start "lb"
    # SqlDAS.g:113:1: lb : ( '(' ) ;
    def lb(self, ):

        try:
            try:
                # SqlDAS.g:113:5: ( ( '(' ) )
                # SqlDAS.g:113:7: ( '(' )
                pass 
                # SqlDAS.g:113:7: ( '(' )
                # SqlDAS.g:113:8: '('
                pass 
                self.match(self.input, 15, self.FOLLOW_15_in_lb1081)







                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "lb"


    # $ANTLR start "rb"
    # SqlDAS.g:114:1: rb : ( ')' ) ;
    def rb(self, ):

        try:
            try:
                # SqlDAS.g:114:5: ( ( ')' ) )
                # SqlDAS.g:114:7: ( ')' )
                pass 
                # SqlDAS.g:114:7: ( ')' )
                # SqlDAS.g:114:8: ')'
                pass 
                self.match(self.input, 16, self.FOLLOW_16_in_rb1091)







                        
            except:
            	raise
        finally:

            pass
        return 

    # $ANTLR end "rb"


    # Delegated rules


    # lookup tables for DFA #3

    DFA3_eot = DFA.unpack(
        u"\6\uffff"
        )

    DFA3_eof = DFA.unpack(
        u"\1\uffff\1\3\3\uffff\1\3"
        )

    DFA3_min = DFA.unpack(
        u"\1\4\1\5\1\4\2\uffff\1\5"
        )

    DFA3_max = DFA.unpack(
        u"\1\4\1\52\1\4\2\uffff\1\52"
        )

    DFA3_accept = DFA.unpack(
        u"\3\uffff\1\1\1\2\1\uffff"
        )

    DFA3_special = DFA.unpack(
        u"\6\uffff"
        )

            
    DFA3_transition = [
        DFA.unpack(u"\1\1"),
        DFA.unpack(u"\1\2\41\uffff\4\4"),
        DFA.unpack(u"\1\5"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\2\41\uffff\4\4")
    ]

    # class definition for DFA #3

    class DFA3(DFA):
        pass


    # lookup tables for DFA #16

    DFA16_eot = DFA.unpack(
        u"\15\uffff"
        )

    DFA16_eof = DFA.unpack(
        u"\15\uffff"
        )

    DFA16_min = DFA.unpack(
        u"\1\6\3\4\11\uffff"
        )

    DFA16_max = DFA.unpack(
        u"\1\11\2\10\1\6\11\uffff"
        )

    DFA16_accept = DFA.unpack(
        u"\4\uffff\1\4\1\5\1\6\1\1\1\7\1\11\1\2\1\3\1\10"
        )

    DFA16_special = DFA.unpack(
        u"\15\uffff"
        )

            
    DFA16_transition = [
        DFA.unpack(u"\1\1\1\2\1\3\1\4"),
        DFA.unpack(u"\1\7\2\uffff\1\6\1\5"),
        DFA.unpack(u"\1\12\1\uffff\1\10\1\uffff\1\11"),
        DFA.unpack(u"\1\13\1\uffff\1\14"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"")
    ]

    # class definition for DFA #16

    class DFA16(DFA):
        pass


 

    FOLLOW_select_in_stmt42 = frozenset([4, 13, 14])
    FOLLOW_selectList_in_stmt44 = frozenset([1, 17, 18, 25, 26])
    FOLLOW_where_in_stmt47 = frozenset([4, 15])
    FOLLOW_constraintList_in_stmt49 = frozenset([1, 25, 26])
    FOLLOW_order_in_stmt54 = frozenset([27, 28])
    FOLLOW_by_in_stmt56 = frozenset([4])
    FOLLOW_orderList_in_stmt58 = frozenset([1])
    FOLLOW_orderList1_in_orderList68 = frozenset([1])
    FOLLOW_orderList1_in_orderList74 = frozenset([39, 40, 41, 42])
    FOLLOW_orderKw_in_orderList76 = frozenset([1])
    FOLLOW_KW_in_orderList186 = frozenset([1, 5])
    FOLLOW_COMMA_in_orderList1101 = frozenset([4])
    FOLLOW_KW_in_orderList1108 = frozenset([1, 5])
    FOLLOW_ordering_in_orderKw134 = frozenset([1])
    FOLLOW_asc_in_ordering146 = frozenset([1])
    FOLLOW_desc_in_ordering148 = frozenset([1])
    FOLLOW_gkw_in_selectList159 = frozenset([1, 5])
    FOLLOW_COMMA_in_selectList174 = frozenset([4, 13, 14])
    FOLLOW_gkw_in_selectList181 = frozenset([1, 5])
    FOLLOW_KW_in_gkw324 = frozenset([1])
    FOLLOW_set_in_gkw329 = frozenset([15])
    FOLLOW_15_in_gkw335 = frozenset([4])
    FOLLOW_KW_in_gkw338 = frozenset([16])
    FOLLOW_16_in_gkw341 = frozenset([1])
    FOLLOW_constraint1_in_constraintList353 = frozenset([1, 23, 24, 29, 30])
    FOLLOW_logicalOp_in_constraintList363 = frozenset([4, 15])
    FOLLOW_constraint1_in_constraintList372 = frozenset([1, 23, 24, 29, 30])
    FOLLOW_lb_in_lopen383 = frozenset([1, 15])
    FOLLOW_lb_in_lopen385 = frozenset([1, 15])
    FOLLOW_rb_in_ropen395 = frozenset([1, 16])
    FOLLOW_rb_in_ropen397 = frozenset([1, 16])
    FOLLOW_lopen_in_constraint1415 = frozenset([4, 15])
    FOLLOW_constraint_in_constraint1441 = frozenset([16, 23, 24, 29, 30])
    FOLLOW_logicalOp_in_constraint1453 = frozenset([4, 15])
    FOLLOW_constraint_in_constraint1473 = frozenset([16, 23, 24, 29, 30])
    FOLLOW_ropen_in_constraint1500 = frozenset([1])
    FOLLOW_constraint_in_constraint1514 = frozenset([1])
    FOLLOW_KW_in_constraint525 = frozenset([6, 7, 8, 9])
    FOLLOW_compOpt_in_constraint539 = frozenset([4])
    FOLLOW_spaceValue_in_constraint552 = frozenset([1])
    FOLLOW_KW_in_constraint567 = frozenset([31, 32])
    FOLLOW_inpython_in_constraint581 = frozenset([15])
    FOLLOW_15_in_constraint592 = frozenset([4])
    FOLLOW_valueList_in_constraint598 = frozenset([16])
    FOLLOW_16_in_constraint608 = frozenset([1])
    FOLLOW_KW_in_constraint634 = frozenset([33, 34, 35, 36])
    FOLLOW_like_in_constraint648 = frozenset([4])
    FOLLOW_KW_in_constraint659 = frozenset([1])
    FOLLOW_KW_in_constraint676 = frozenset([43, 44])
    FOLLOW_between_in_constraint690 = frozenset([4])
    FOLLOW_betValue_in_constraint700 = frozenset([1])
    FOLLOW_KW_in_spaceValue749 = frozenset([1, 4])
    FOLLOW_KW_in_spaceValue752 = frozenset([1, 4])
    FOLLOW_KW_in_valueList761 = frozenset([1, 5])
    FOLLOW_COMMA_in_valueList765 = frozenset([4])
    FOLLOW_KW_in_valueList767 = frozenset([1, 5])
    FOLLOW_KW_in_betValue777 = frozenset([23, 24])
    FOLLOW_andpython_in_betValue779 = frozenset([4])
    FOLLOW_KW_in_betValue781 = frozenset([1])
    FOLLOW_set_in_where787 = frozenset([1])
    FOLLOW_EQ_in_compOpt801 = frozenset([1])
    FOLLOW_LT_in_compOpt808 = frozenset([1])
    FOLLOW_GT_in_compOpt815 = frozenset([1])
    FOLLOW_NOT_in_compOpt822 = frozenset([6])
    FOLLOW_EQ_in_compOpt825 = frozenset([1])
    FOLLOW_EQ_in_compOpt832 = frozenset([8])
    FOLLOW_GT_in_compOpt835 = frozenset([1])
    FOLLOW_EQ_in_compOpt842 = frozenset([7])
    FOLLOW_LT_in_compOpt845 = frozenset([1])
    FOLLOW_LT_in_compOpt852 = frozenset([6])
    FOLLOW_EQ_in_compOpt855 = frozenset([1])
    FOLLOW_GT_in_compOpt862 = frozenset([6])
    FOLLOW_EQ_in_compOpt865 = frozenset([1])
    FOLLOW_LT_in_compOpt872 = frozenset([8])
    FOLLOW_GT_in_compOpt875 = frozenset([1])
    FOLLOW_andpython_in_logicalOp884 = frozenset([1])
    FOLLOW_orpython_in_logicalOp886 = frozenset([1])
    FOLLOW_set_in_select894 = frozenset([1])
    FOLLOW_set_in_andpython914 = frozenset([1])
    FOLLOW_set_in_order927 = frozenset([1])
    FOLLOW_set_in_by940 = frozenset([1])
    FOLLOW_set_in_orpython952 = frozenset([1])
    FOLLOW_set_in_inpython964 = frozenset([1])
    FOLLOW_set_in_notpython976 = frozenset([1])
    FOLLOW_35_in_like990 = frozenset([1])
    FOLLOW_36_in_like994 = frozenset([1])
    FOLLOW_33_in_like998 = frozenset([35])
    FOLLOW_35_in_like1000 = frozenset([1])
    FOLLOW_34_in_like1004 = frozenset([36])
    FOLLOW_36_in_like1006 = frozenset([1])
    FOLLOW_set_in_count1014 = frozenset([1])
    FOLLOW_set_in_sum1027 = frozenset([1])
    FOLLOW_set_in_asc1040 = frozenset([1])
    FOLLOW_set_in_desc1053 = frozenset([1])
    FOLLOW_set_in_between1066 = frozenset([1])
    FOLLOW_15_in_lb1081 = frozenset([1])
    FOLLOW_16_in_rb1091 = frozenset([1])



def main(argv, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    from antlr3.main import ParserMain
    main = ParserMain("SqlDASLexer", SqlDASParser)
    main.stdin = stdin
    main.stdout = stdout
    main.stderr = stderr
    main.execute(argv)


if __name__ == '__main__':
    main(sys.argv)
