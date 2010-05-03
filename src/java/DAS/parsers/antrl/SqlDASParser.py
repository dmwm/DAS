# $ANTLR 3.1.2 SqlDAS.g 2009-03-05 10:47:43

import sys
from antlr3 import *
from antlr3.compat import set, frozenset
          
kws = []
okws = []
constraints = []
orderingkw = []




# for convenience in actions
HIDDEN = BaseRecognizer.HIDDEN

# token types
LT=9
T__29=29
T__28=28
T__27=27
T__26=26
T__25=25
T__24=24
T__23=23
T__22=22
T__21=21
T__20=20
NOT=11
EOF=-1
T__93=93
T__19=19
T__94=94
T__91=91
T__92=92
T__16=16
T__15=15
T__90=90
T__18=18
T__17=17
NL=13
EQ=8
T__99=99
T__98=98
T__97=97
T__96=96
T__95=95
T__80=80
T__81=81
T__82=82
T__83=83
VALUE=7
T__85=85
T__84=84
T__87=87
T__86=86
T__89=89
T__88=88
WS=14
T__71=71
T__72=72
T__70=70
GT=10
T__76=76
T__75=75
T__74=74
T__73=73
T__79=79
T__78=78
T__77=77
T__68=68
T__69=69
T__66=66
T__67=67
T__64=64
T__65=65
T__62=62
T__63=63
AMP=12
T__61=61
SPACE=4
T__60=60
T__55=55
T__56=56
T__57=57
T__58=58
T__51=51
T__52=52
T__53=53
T__54=54
T__107=107
COMMA=5
T__108=108
T__109=109
T__103=103
T__59=59
T__104=104
T__105=105
T__106=106
DOT=6
T__50=50
T__42=42
T__43=43
T__40=40
T__41=41
T__46=46
T__47=47
T__44=44
T__45=45
T__48=48
T__49=49
T__102=102
T__101=101
T__100=100
T__30=30
T__31=31
T__32=32
T__33=33
T__34=34
T__35=35
T__36=36
T__37=37
T__38=38
T__39=39

# token names
tokenNames = [
    "<invalid>", "<EOR>", "<DOWN>", "<UP>", 
    "SPACE", "COMMA", "DOT", "VALUE", "EQ", "LT", "GT", "NOT", "AMP", "NL", 
    "WS", "'('", "')'", "'WHERE'", "'where'", "'in'", "'ads'", "'config'", 
    "'dataset'", "'release'", "'tier'", "'site'", "'block'", "'file'", "'primds'", 
    "'procds'", "'run'", "'lumi'", "'dq'", "'ilumi'", "'phygrp'", "'group'", 
    "'pset'", "'algo'", "'datatype'", "'mcdesc'", "'trigdesc'", "'branch'", 
    "'createdate'", "'moddate'", "'starttime'", "'endtime'", "'createby'", 
    "'modby'", "'name'", "'version'", "'number'", "'startevnum'", "'endevnum'", 
    "'numevents'", "'numfiles'", "'numlss'", "'totlumi'", "'store'", "'size'", 
    "'count'", "'status'", "'type'", "'id'", "'parent'", "'child'", "'def'", 
    "'evnum'", "'era'", "'tag'", "'xsection'", "'hash'", "'content'", "'family'", 
    "'exe'", "'annotation'", "'checksum'", "'numruns()'", "'numfiles()'", 
    "'dataquality()'", "'latest()'", "'parentrelease()'", "'childrelease()'", 
    "'intluminosity()'", "'findevents()'", "'select'", "'SELECT'", "'find'", 
    "'FIND'", "'and'", "'AND'", "'order'", "'ORDER'", "'by'", "'BY'", "'or'", 
    "'OR'", "'IN'", "'not'", "'NOT'", "'like'", "'LIKE'", "'COUNT'", "'sum'", 
    "'SUM'", "'asc'", "'ASC'", "'desc'", "'DESC'", "'between'", "'BETWEEN'"
]




class SqlDASParser(Parser):
    grammarFileName = "SqlDAS.g"
    antlr_version = version_str_to_tuple("3.1.2")
    antlr_version_str = "3.1.2"
    tokenNames = tokenNames

    def __init__(self, input, state=None):
        if state is None:
            state = RecognizerSharedState()

        Parser.__init__(self, input, state)


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

        self.dfa4 = self.DFA4(
            self, 4,
            eot = self.DFA4_eot,
            eof = self.DFA4_eof,
            min = self.DFA4_min,
            max = self.DFA4_max,
            accept = self.DFA4_accept,
            special = self.DFA4_special,
            transition = self.DFA4_transition
            )

        self.dfa6 = self.DFA6(
            self, 6,
            eot = self.DFA6_eot,
            eof = self.DFA6_eof,
            min = self.DFA6_min,
            max = self.DFA6_max,
            accept = self.DFA6_accept,
            special = self.DFA6_special,
            transition = self.DFA6_transition
            )

        self.dfa8 = self.DFA8(
            self, 8,
            eot = self.DFA8_eot,
            eof = self.DFA8_eof,
            min = self.DFA8_min,
            max = self.DFA8_max,
            accept = self.DFA8_accept,
            special = self.DFA8_special,
            transition = self.DFA8_transition
            )

        self.dfa11 = self.DFA11(
            self, 11,
            eot = self.DFA11_eot,
            eof = self.DFA11_eof,
            min = self.DFA11_min,
            max = self.DFA11_max,
            accept = self.DFA11_accept,
            special = self.DFA11_special,
            transition = self.DFA11_transition
            )

        self.dfa12 = self.DFA12(
            self, 12,
            eot = self.DFA12_eot,
            eof = self.DFA12_eof,
            min = self.DFA12_min,
            max = self.DFA12_max,
            accept = self.DFA12_accept,
            special = self.DFA12_special,
            transition = self.DFA12_transition
            )

        self.dfa13 = self.DFA13(
            self, 13,
            eot = self.DFA13_eot,
            eof = self.DFA13_eof,
            min = self.DFA13_min,
            max = self.DFA13_max,
            accept = self.DFA13_accept,
            special = self.DFA13_special,
            transition = self.DFA13_transition
            )

        self.dfa14 = self.DFA14(
            self, 14,
            eot = self.DFA14_eot,
            eof = self.DFA14_eof,
            min = self.DFA14_min,
            max = self.DFA14_max,
            accept = self.DFA14_accept,
            special = self.DFA14_special,
            transition = self.DFA14_transition
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
    # SqlDAS.g:26:1: stmt : ( select spaces selectList spaces where spaces constraintList spaces ( order spaces by spaces orderList )* | select spaces selectList spaces | select spaces selectList spaces order spaces by spaces orderList );
    def stmt(self, ):

        try:
            try:
                # SqlDAS.g:26:6: ( select spaces selectList spaces where spaces constraintList spaces ( order spaces by spaces orderList )* | select spaces selectList spaces | select spaces selectList spaces order spaces by spaces orderList )
                alt2 = 3
                alt2 = self.dfa2.predict(self.input)
                if alt2 == 1:
                    # SqlDAS.g:26:8: select spaces selectList spaces where spaces constraintList spaces ( order spaces by spaces orderList )*
                    pass 
                    self._state.following.append(self.FOLLOW_select_in_stmt42)
                    self.select()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_stmt44)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_selectList_in_stmt46)
                    self.selectList()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_stmt48)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_where_in_stmt50)
                    self.where()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_stmt52)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_constraintList_in_stmt54)
                    self.constraintList()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_stmt56)
                    self.spaces()

                    self._state.following.pop()
                    # SqlDAS.g:26:75: ( order spaces by spaces orderList )*
                    while True: #loop1
                        alt1 = 2
                        LA1_0 = self.input.LA(1)

                        if ((90 <= LA1_0 <= 91)) :
                            alt1 = 1


                        if alt1 == 1:
                            # SqlDAS.g:26:76: order spaces by spaces orderList
                            pass 
                            self._state.following.append(self.FOLLOW_order_in_stmt59)
                            self.order()

                            self._state.following.pop()
                            self._state.following.append(self.FOLLOW_spaces_in_stmt61)
                            self.spaces()

                            self._state.following.pop()
                            self._state.following.append(self.FOLLOW_by_in_stmt63)
                            self.by()

                            self._state.following.pop()
                            self._state.following.append(self.FOLLOW_spaces_in_stmt65)
                            self.spaces()

                            self._state.following.pop()
                            self._state.following.append(self.FOLLOW_orderList_in_stmt67)
                            self.orderList()

                            self._state.following.pop()


                        else:
                            break #loop1




                elif alt2 == 2:
                    # SqlDAS.g:27:4: select spaces selectList spaces
                    pass 
                    self._state.following.append(self.FOLLOW_select_in_stmt74)
                    self.select()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_stmt76)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_selectList_in_stmt78)
                    self.selectList()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_stmt80)
                    self.spaces()

                    self._state.following.pop()


                elif alt2 == 3:
                    # SqlDAS.g:28:4: select spaces selectList spaces order spaces by spaces orderList
                    pass 
                    self._state.following.append(self.FOLLOW_select_in_stmt85)
                    self.select()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_stmt87)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_selectList_in_stmt89)
                    self.selectList()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_stmt91)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_order_in_stmt93)
                    self.order()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_stmt95)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_by_in_stmt97)
                    self.by()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_stmt99)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_orderList_in_stmt101)
                    self.orderList()

                    self._state.following.pop()



                        
            except:
            	raise
        finally:

            pass

        return 

    # $ANTLR end "stmt"


    # $ANTLR start "spaces"
    # SqlDAS.g:34:1: spaces : ( SPACE )* ;
    def spaces(self, ):

        try:
            try:
                # SqlDAS.g:34:8: ( ( SPACE )* )
                # SqlDAS.g:34:10: ( SPACE )*
                pass 
                # SqlDAS.g:34:10: ( SPACE )*
                while True: #loop3
                    alt3 = 2
                    LA3_0 = self.input.LA(1)

                    if (LA3_0 == SPACE) :
                        alt3 = 1


                    if alt3 == 1:
                        # SqlDAS.g:34:11: SPACE
                        pass 
                        self.match(self.input, SPACE, self.FOLLOW_SPACE_in_spaces116)


                    else:
                        break #loop3






                        
            except:
            	raise
        finally:

            pass

        return 

    # $ANTLR end "spaces"


    # $ANTLR start "orderList"
    # SqlDAS.g:36:1: orderList : okw= keyword ( spaces COMMA spaces okw= keyword )* spaces oing= ordering ;
    def orderList(self, ):

        okw = None

        oing = None


        try:
            try:
                # SqlDAS.g:36:11: (okw= keyword ( spaces COMMA spaces okw= keyword )* spaces oing= ordering )
                # SqlDAS.g:36:12: okw= keyword ( spaces COMMA spaces okw= keyword )* spaces oing= ordering
                pass 
                self._state.following.append(self.FOLLOW_keyword_in_orderList128)
                okw = self.keyword()

                self._state.following.pop()
                #action start
                okws.append(str(((okw is not None) and [self.input.toString(okw.start,okw.stop)] or [None])[0]))
                #action end
                # SqlDAS.g:37:4: ( spaces COMMA spaces okw= keyword )*
                while True: #loop4
                    alt4 = 2
                    alt4 = self.dfa4.predict(self.input)
                    if alt4 == 1:
                        # SqlDAS.g:38:3: spaces COMMA spaces okw= keyword
                        pass 
                        self._state.following.append(self.FOLLOW_spaces_in_orderList141)
                        self.spaces()

                        self._state.following.pop()
                        self.match(self.input, COMMA, self.FOLLOW_COMMA_in_orderList145)
                        self._state.following.append(self.FOLLOW_spaces_in_orderList149)
                        self.spaces()

                        self._state.following.pop()
                        self._state.following.append(self.FOLLOW_keyword_in_orderList156)
                        okw = self.keyword()

                        self._state.following.pop()
                        #action start
                        okws.append(str(((okw is not None) and [self.input.toString(okw.start,okw.stop)] or [None])[0]))
                        #action end


                    else:
                        break #loop4


                self._state.following.append(self.FOLLOW_spaces_in_orderList171)
                self.spaces()

                self._state.following.pop()
                self._state.following.append(self.FOLLOW_ordering_in_orderList179)
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

    # $ANTLR end "orderList"

    class ordering_return(ParserRuleReturnScope):
        def __init__(self):
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "ordering"
    # SqlDAS.g:47:1: ordering : ( asc | desc )? ;
    def ordering(self, ):

        retval = self.ordering_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:47:11: ( ( asc | desc )? )
                # SqlDAS.g:47:13: ( asc | desc )?
                pass 
                # SqlDAS.g:47:13: ( asc | desc )?
                alt5 = 3
                LA5_0 = self.input.LA(1)

                if ((104 <= LA5_0 <= 105)) :
                    alt5 = 1
                elif ((106 <= LA5_0 <= 107)) :
                    alt5 = 2
                if alt5 == 1:
                    # SqlDAS.g:47:14: asc
                    pass 
                    self._state.following.append(self.FOLLOW_asc_in_ordering196)
                    self.asc()

                    self._state.following.pop()


                elif alt5 == 2:
                    # SqlDAS.g:47:18: desc
                    pass 
                    self._state.following.append(self.FOLLOW_desc_in_ordering198)
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
    # SqlDAS.g:48:1: selectList : kw= keyword ( spaces COMMA spaces kw= keyword )* ;
    def selectList(self, ):

        kw = None


        try:
            try:
                # SqlDAS.g:48:12: (kw= keyword ( spaces COMMA spaces kw= keyword )* )
                # SqlDAS.g:48:13: kw= keyword ( spaces COMMA spaces kw= keyword )*
                pass 
                self._state.following.append(self.FOLLOW_keyword_in_selectList211)
                kw = self.keyword()

                self._state.following.pop()
                #action start
                kws.append(str(((kw is not None) and [self.input.toString(kw.start,kw.stop)] or [None])[0]))
                #action end
                # SqlDAS.g:49:4: ( spaces COMMA spaces kw= keyword )*
                while True: #loop6
                    alt6 = 2
                    alt6 = self.dfa6.predict(self.input)
                    if alt6 == 1:
                        # SqlDAS.g:50:3: spaces COMMA spaces kw= keyword
                        pass 
                        self._state.following.append(self.FOLLOW_spaces_in_selectList224)
                        self.spaces()

                        self._state.following.pop()
                        self.match(self.input, COMMA, self.FOLLOW_COMMA_in_selectList228)
                        self._state.following.append(self.FOLLOW_spaces_in_selectList232)
                        self.spaces()

                        self._state.following.pop()
                        self._state.following.append(self.FOLLOW_keyword_in_selectList239)
                        kw = self.keyword()

                        self._state.following.pop()
                        #action start
                        kws.append(str(((kw is not None) and [self.input.toString(kw.start,kw.stop)] or [None])[0]))
                        #action end


                    else:
                        break #loop6






                        
            except:
            	raise
        finally:

            pass

        return 

    # $ANTLR end "selectList"

    class keyword_return(ParserRuleReturnScope):
        def __init__(self):
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "keyword"
    # SqlDAS.g:56:1: keyword : ( entity | entity DOT attr | entity DOT funct | count spaces '(' spaces entity spaces ')' | sum spaces '(' spaces entity DOT attr spaces ')' );
    def keyword(self, ):

        retval = self.keyword_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:56:9: ( entity | entity DOT attr | entity DOT funct | count spaces '(' spaces entity spaces ')' | sum spaces '(' spaces entity DOT attr spaces ')' )
                alt7 = 5
                LA7 = self.input.LA(1)
                if LA7 == 20 or LA7 == 21 or LA7 == 22 or LA7 == 23 or LA7 == 24 or LA7 == 25 or LA7 == 26 or LA7 == 27 or LA7 == 28 or LA7 == 29 or LA7 == 30 or LA7 == 31 or LA7 == 32 or LA7 == 33 or LA7 == 34 or LA7 == 35 or LA7 == 36 or LA7 == 37 or LA7 == 38 or LA7 == 39 or LA7 == 40 or LA7 == 41:
                    LA7_1 = self.input.LA(2)

                    if (LA7_1 == DOT) :
                        LA7_4 = self.input.LA(3)

                        if ((76 <= LA7_4 <= 83)) :
                            alt7 = 3
                        elif ((22 <= LA7_4 <= 24) or (42 <= LA7_4 <= 75)) :
                            alt7 = 2
                        else:
                            nvae = NoViableAltException("", 7, 4, self.input)

                            raise nvae

                    elif (LA7_1 == EOF or (SPACE <= LA7_1 <= COMMA) or (EQ <= LA7_1 <= NOT) or (17 <= LA7_1 <= 19) or (90 <= LA7_1 <= 91) or (96 <= LA7_1 <= 100) or (104 <= LA7_1 <= 109)) :
                        alt7 = 1
                    else:
                        nvae = NoViableAltException("", 7, 1, self.input)

                        raise nvae

                elif LA7 == 59 or LA7 == 101:
                    alt7 = 4
                elif LA7 == 102 or LA7 == 103:
                    alt7 = 5
                else:
                    nvae = NoViableAltException("", 7, 0, self.input)

                    raise nvae

                if alt7 == 1:
                    # SqlDAS.g:56:11: entity
                    pass 
                    self._state.following.append(self.FOLLOW_entity_in_keyword264)
                    self.entity()

                    self._state.following.pop()


                elif alt7 == 2:
                    # SqlDAS.g:57:4: entity DOT attr
                    pass 
                    self._state.following.append(self.FOLLOW_entity_in_keyword270)
                    self.entity()

                    self._state.following.pop()
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_keyword272)
                    self._state.following.append(self.FOLLOW_attr_in_keyword274)
                    self.attr()

                    self._state.following.pop()


                elif alt7 == 3:
                    # SqlDAS.g:58:4: entity DOT funct
                    pass 
                    self._state.following.append(self.FOLLOW_entity_in_keyword279)
                    self.entity()

                    self._state.following.pop()
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_keyword281)
                    self._state.following.append(self.FOLLOW_funct_in_keyword283)
                    self.funct()

                    self._state.following.pop()


                elif alt7 == 4:
                    # SqlDAS.g:59:4: count spaces '(' spaces entity spaces ')'
                    pass 
                    self._state.following.append(self.FOLLOW_count_in_keyword288)
                    self.count()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_keyword290)
                    self.spaces()

                    self._state.following.pop()
                    self.match(self.input, 15, self.FOLLOW_15_in_keyword292)
                    self._state.following.append(self.FOLLOW_spaces_in_keyword294)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_entity_in_keyword296)
                    self.entity()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_keyword298)
                    self.spaces()

                    self._state.following.pop()
                    self.match(self.input, 16, self.FOLLOW_16_in_keyword300)


                elif alt7 == 5:
                    # SqlDAS.g:60:4: sum spaces '(' spaces entity DOT attr spaces ')'
                    pass 
                    self._state.following.append(self.FOLLOW_sum_in_keyword305)
                    self.sum()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_keyword307)
                    self.spaces()

                    self._state.following.pop()
                    self.match(self.input, 15, self.FOLLOW_15_in_keyword309)
                    self._state.following.append(self.FOLLOW_spaces_in_keyword311)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_entity_in_keyword313)
                    self.entity()

                    self._state.following.pop()
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_keyword315)
                    self._state.following.append(self.FOLLOW_attr_in_keyword317)
                    self.attr()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_spaces_in_keyword319)
                    self.spaces()

                    self._state.following.pop()
                    self.match(self.input, 16, self.FOLLOW_16_in_keyword321)


                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass

        return retval

    # $ANTLR end "keyword"


    # $ANTLR start "constraintList"
    # SqlDAS.g:62:1: constraintList : constraint1 ( spaces rel= logicalOp spaces constraint1 )* ;
    def constraintList(self, ):

        rel = None


        try:
            try:
                # SqlDAS.g:62:16: ( constraint1 ( spaces rel= logicalOp spaces constraint1 )* )
                # SqlDAS.g:62:18: constraint1 ( spaces rel= logicalOp spaces constraint1 )*
                pass 
                self._state.following.append(self.FOLLOW_constraint1_in_constraintList330)
                self.constraint1()

                self._state.following.pop()
                # SqlDAS.g:62:30: ( spaces rel= logicalOp spaces constraint1 )*
                while True: #loop8
                    alt8 = 2
                    alt8 = self.dfa8.predict(self.input)
                    if alt8 == 1:
                        # SqlDAS.g:62:32: spaces rel= logicalOp spaces constraint1
                        pass 
                        self._state.following.append(self.FOLLOW_spaces_in_constraintList334)
                        self.spaces()

                        self._state.following.pop()
                        self._state.following.append(self.FOLLOW_logicalOp_in_constraintList341)
                        rel = self.logicalOp()

                        self._state.following.pop()
                        #action start
                        constraints.append(str(((rel is not None) and [self.input.toString(rel.start,rel.stop)] or [None])[0]));
                        #action end
                        self._state.following.append(self.FOLLOW_spaces_in_constraintList349)
                        self.spaces()

                        self._state.following.pop()
                        self._state.following.append(self.FOLLOW_constraint1_in_constraintList351)
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
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "lopen"
    # SqlDAS.g:65:1: lopen : ( lb )* ;
    def lopen(self, ):

        retval = self.lopen_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:65:8: ( ( lb )* )
                # SqlDAS.g:65:10: ( lb )*
                pass 
                # SqlDAS.g:65:10: ( lb )*
                while True: #loop9
                    alt9 = 2
                    LA9_0 = self.input.LA(1)

                    if (LA9_0 == 15) :
                        alt9 = 1


                    if alt9 == 1:
                        # SqlDAS.g:65:11: lb
                        pass 
                        self._state.following.append(self.FOLLOW_lb_in_lopen362)
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
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "ropen"
    # SqlDAS.g:66:1: ropen : ( rb )* ;
    def ropen(self, ):

        retval = self.ropen_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:66:8: ( ( rb )* )
                # SqlDAS.g:66:10: ( rb )*
                pass 
                # SqlDAS.g:66:10: ( rb )*
                while True: #loop10
                    alt10 = 2
                    LA10_0 = self.input.LA(1)

                    if (LA10_0 == 16) :
                        alt10 = 1


                    if alt10 == 1:
                        # SqlDAS.g:66:11: rb
                        pass 
                        self._state.following.append(self.FOLLOW_rb_in_ropen373)
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
    # SqlDAS.g:67:1: constraint1 : kl= lopen constraint kr= ropen ;
    def constraint1(self, ):

        kl = None

        kr = None


        try:
            try:
                # SqlDAS.g:67:17: (kl= lopen constraint kr= ropen )
                # SqlDAS.g:67:19: kl= lopen constraint kr= ropen
                pass 
                self._state.following.append(self.FOLLOW_lopen_in_constraint1391)
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
                self._state.following.append(self.FOLLOW_constraint_in_constraint1417)
                self.constraint()

                self._state.following.pop()
                self._state.following.append(self.FOLLOW_ropen_in_constraint1443)
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




                        
            except:
            	raise
        finally:

            pass

        return 

    # $ANTLR end "constraint1"


    # $ANTLR start "constraint"
    # SqlDAS.g:71:1: constraint : (kw= keyword spaces op= compOpt spaces val= genValue | kw= keyword spaces op1= inpython spaces '(' spaces val1= valueList spaces ')' | kw= keyword spaces op2= like spaces val2= dotValue | kw= keyword spaces op3= between spaces val3= betValue );
    def constraint(self, ):

        kw = None

        op = None

        val = None

        op1 = None

        val1 = None

        op2 = None

        val2 = None

        op3 = None

        val3 = None


        try:
            try:
                # SqlDAS.g:71:12: (kw= keyword spaces op= compOpt spaces val= genValue | kw= keyword spaces op1= inpython spaces '(' spaces val1= valueList spaces ')' | kw= keyword spaces op2= like spaces val2= dotValue | kw= keyword spaces op3= between spaces val3= betValue )
                alt11 = 4
                alt11 = self.dfa11.predict(self.input)
                if alt11 == 1:
                    # SqlDAS.g:71:14: kw= keyword spaces op= compOpt spaces val= genValue
                    pass 
                    self._state.following.append(self.FOLLOW_keyword_in_constraint462)
                    kw = self.keyword()

                    self._state.following.pop()
                    #action start
                    c = {}
                    #action end
                    #action start
                    c['key'] = str(((kw is not None) and [self.input.toString(kw.start,kw.stop)] or [None])[0])
                    #action end
                    self._state.following.append(self.FOLLOW_spaces_in_constraint473)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_compOpt_in_constraint480)
                    op = self.compOpt()

                    self._state.following.pop()
                    #action start
                    c['op'] = str(((op is not None) and [self.input.toString(op.start,op.stop)] or [None])[0])
                    #action end
                    self._state.following.append(self.FOLLOW_spaces_in_constraint490)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_genValue_in_constraint497)
                    val = self.genValue()

                    self._state.following.pop()
                    #action start
                    c['value'] = str(((val is not None) and [self.input.toString(val.start,val.stop)] or [None])[0])
                    #action end
                    #action start
                    constraints.append(c)
                    #action end


                elif alt11 == 2:
                    # SqlDAS.g:77:2: kw= keyword spaces op1= inpython spaces '(' spaces val1= valueList spaces ')'
                    pass 
                    self._state.following.append(self.FOLLOW_keyword_in_constraint512)
                    kw = self.keyword()

                    self._state.following.pop()
                    #action start
                    c = {}
                    #action end
                    #action start
                    c['key'] = str(((kw is not None) and [self.input.toString(kw.start,kw.stop)] or [None])[0])
                    #action end
                    self._state.following.append(self.FOLLOW_spaces_in_constraint523)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_inpython_in_constraint530)
                    op1 = self.inpython()

                    self._state.following.pop()
                    #action start
                    c['op'] = str(((op1 is not None) and [self.input.toString(op1.start,op1.stop)] or [None])[0])
                    #action end
                    self._state.following.append(self.FOLLOW_spaces_in_constraint541)
                    self.spaces()

                    self._state.following.pop()
                    self.match(self.input, 15, self.FOLLOW_15_in_constraint543)
                    self._state.following.append(self.FOLLOW_spaces_in_constraint547)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_valueList_in_constraint553)
                    val1 = self.valueList()

                    self._state.following.pop()
                    #action start
                    c['value'] = str(((val1 is not None) and [self.input.toString(val1.start,val1.stop)] or [None])[0])
                    #action end
                    #action start
                    constraints.append(c);
                    #action end
                    self._state.following.append(self.FOLLOW_spaces_in_constraint563)
                    self.spaces()

                    self._state.following.pop()
                    self.match(self.input, 16, self.FOLLOW_16_in_constraint567)


                elif alt11 == 3:
                    # SqlDAS.g:86:2: kw= keyword spaces op2= like spaces val2= dotValue
                    pass 
                    self._state.following.append(self.FOLLOW_keyword_in_constraint593)
                    kw = self.keyword()

                    self._state.following.pop()
                    #action start
                    c = {}
                    #action end
                    #action start
                    c['key'] = str(((kw is not None) and [self.input.toString(kw.start,kw.stop)] or [None])[0])
                    #action end
                    self._state.following.append(self.FOLLOW_spaces_in_constraint604)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_like_in_constraint611)
                    op2 = self.like()

                    self._state.following.pop()
                    #action start
                    c['op'] = str(((op2 is not None) and [self.input.toString(op2.start,op2.stop)] or [None])[0])
                    #action end
                    self._state.following.append(self.FOLLOW_spaces_in_constraint620)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_dotValue_in_constraint627)
                    val2 = self.dotValue()

                    self._state.following.pop()
                    #action start
                    c['value'] = str(((val2 is not None) and [self.input.toString(val2.start,val2.stop)] or [None])[0])
                    #action end
                    #action start
                    constraints.append(c)
                    #action end


                elif alt11 == 4:
                    # SqlDAS.g:92:3: kw= keyword spaces op3= between spaces val3= betValue
                    pass 
                    self._state.following.append(self.FOLLOW_keyword_in_constraint644)
                    kw = self.keyword()

                    self._state.following.pop()
                    #action start
                    c = {}
                    #action end
                    #action start
                    c['key'] = str(((kw is not None) and [self.input.toString(kw.start,kw.stop)] or [None])[0])
                    #action end
                    self._state.following.append(self.FOLLOW_spaces_in_constraint655)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_between_in_constraint662)
                    op3 = self.between()

                    self._state.following.pop()
                    #action start
                    c['op'] = str(((op3 is not None) and [self.input.toString(op3.start,op3.stop)] or [None])[0])
                    #action end
                    self._state.following.append(self.FOLLOW_spaces_in_constraint670)
                    self.spaces()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_betValue_in_constraint677)
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


    # $ANTLR start "where"
    # SqlDAS.g:99:1: where : ( 'WHERE' | 'where' ) ;
    def where(self, ):

        try:
            try:
                # SqlDAS.g:99:7: ( ( 'WHERE' | 'where' ) )
                # SqlDAS.g:99:8: ( 'WHERE' | 'where' )
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

    class dotValue_return(ParserRuleReturnScope):
        def __init__(self):
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "dotValue"
    # SqlDAS.g:100:1: dotValue : ( VALUE | 'in' | VALUE DOT 'in' | VALUE DOT VALUE | VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE SPACE VALUE | VALUE SPACE VALUE SPACE VALUE );
    def dotValue(self, ):

        retval = self.dotValue_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:100:17: ( VALUE | 'in' | VALUE DOT 'in' | VALUE DOT VALUE | VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE | VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in' | VALUE SPACE VALUE | VALUE SPACE VALUE SPACE VALUE )
                alt12 = 23
                alt12 = self.dfa12.predict(self.input)
                if alt12 == 1:
                    # SqlDAS.g:100:19: VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue746)


                elif alt12 == 2:
                    # SqlDAS.g:101:5: 'in'
                    pass 
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue753)


                elif alt12 == 3:
                    # SqlDAS.g:102:5: VALUE DOT 'in'
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue759)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue761)
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue763)


                elif alt12 == 4:
                    # SqlDAS.g:103:5: VALUE DOT VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue769)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue771)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue773)


                elif alt12 == 5:
                    # SqlDAS.g:104:5: VALUE DOT VALUE DOT 'in'
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue779)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue781)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue783)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue785)
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue787)


                elif alt12 == 6:
                    # SqlDAS.g:105:5: VALUE DOT VALUE DOT VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue793)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue795)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue797)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue799)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue801)


                elif alt12 == 7:
                    # SqlDAS.g:106:5: VALUE DOT VALUE DOT VALUE DOT 'in'
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue807)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue809)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue811)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue813)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue815)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue817)
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue819)


                elif alt12 == 8:
                    # SqlDAS.g:107:5: VALUE DOT VALUE DOT VALUE DOT VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue825)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue827)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue829)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue831)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue833)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue835)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue837)


                elif alt12 == 9:
                    # SqlDAS.g:108:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue843)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue845)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue847)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue849)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue851)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue853)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue855)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue857)
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue859)


                elif alt12 == 10:
                    # SqlDAS.g:109:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue865)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue867)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue869)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue871)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue873)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue875)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue877)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue879)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue881)


                elif alt12 == 11:
                    # SqlDAS.g:110:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue887)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue889)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue891)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue893)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue895)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue897)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue899)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue901)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue903)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue905)
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue907)


                elif alt12 == 12:
                    # SqlDAS.g:111:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue913)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue915)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue917)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue919)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue921)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue923)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue925)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue927)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue929)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue931)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue933)


                elif alt12 == 13:
                    # SqlDAS.g:112:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue939)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue941)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue943)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue945)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue947)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue949)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue951)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue953)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue955)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue957)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue959)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue961)
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue963)


                elif alt12 == 14:
                    # SqlDAS.g:113:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue969)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue971)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue973)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue975)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue977)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue979)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue981)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue983)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue985)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue987)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue989)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue991)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue993)


                elif alt12 == 15:
                    # SqlDAS.g:114:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue999)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1001)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1003)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1005)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1007)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1009)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1011)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1013)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1015)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1017)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1019)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1021)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1023)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1025)
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue1027)


                elif alt12 == 16:
                    # SqlDAS.g:115:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1033)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1035)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1037)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1039)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1041)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1043)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1045)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1047)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1049)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1051)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1053)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1055)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1057)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1059)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1061)


                elif alt12 == 17:
                    # SqlDAS.g:116:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1067)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1069)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1071)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1073)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1075)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1077)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1079)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1081)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1083)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1085)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1087)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1089)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1091)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1093)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1095)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1097)
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue1099)


                elif alt12 == 18:
                    # SqlDAS.g:117:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1105)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1107)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1109)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1111)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1113)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1115)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1117)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1119)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1121)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1123)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1125)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1127)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1129)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1131)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1133)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1135)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1137)


                elif alt12 == 19:
                    # SqlDAS.g:118:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1144)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1146)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1148)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1150)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1152)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1154)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1156)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1158)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1160)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1162)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1164)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1166)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1168)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1170)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1172)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1174)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1176)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1178)
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue1180)


                elif alt12 == 20:
                    # SqlDAS.g:119:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1186)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1188)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1190)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1192)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1194)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1196)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1198)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1200)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1202)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1204)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1206)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1208)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1210)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1212)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1214)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1216)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1218)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1220)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1222)


                elif alt12 == 21:
                    # SqlDAS.g:120:5: VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1229)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1231)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1233)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1235)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1237)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1239)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1241)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1243)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1245)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1247)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1249)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1251)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1253)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1255)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1257)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1259)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1261)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1263)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1265)
                    self.match(self.input, DOT, self.FOLLOW_DOT_in_dotValue1267)
                    self.match(self.input, 19, self.FOLLOW_19_in_dotValue1269)


                elif alt12 == 22:
                    # SqlDAS.g:121:5: VALUE SPACE VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1275)
                    self.match(self.input, SPACE, self.FOLLOW_SPACE_in_dotValue1277)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1279)


                elif alt12 == 23:
                    # SqlDAS.g:122:5: VALUE SPACE VALUE SPACE VALUE
                    pass 
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1285)
                    self.match(self.input, SPACE, self.FOLLOW_SPACE_in_dotValue1287)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1289)
                    self.match(self.input, SPACE, self.FOLLOW_SPACE_in_dotValue1291)
                    self.match(self.input, VALUE, self.FOLLOW_VALUE_in_dotValue1293)


                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass

        return retval

    # $ANTLR end "dotValue"

    class valueList_return(ParserRuleReturnScope):
        def __init__(self):
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "valueList"
    # SqlDAS.g:126:1: valueList : dotValue ( spaces COMMA spaces dotValue )* ;
    def valueList(self, ):

        retval = self.valueList_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:126:11: ( dotValue ( spaces COMMA spaces dotValue )* )
                # SqlDAS.g:126:12: dotValue ( spaces COMMA spaces dotValue )*
                pass 
                self._state.following.append(self.FOLLOW_dotValue_in_valueList1302)
                self.dotValue()

                self._state.following.pop()
                # SqlDAS.g:126:21: ( spaces COMMA spaces dotValue )*
                while True: #loop13
                    alt13 = 2
                    alt13 = self.dfa13.predict(self.input)
                    if alt13 == 1:
                        # SqlDAS.g:126:23: spaces COMMA spaces dotValue
                        pass 
                        self._state.following.append(self.FOLLOW_spaces_in_valueList1306)
                        self.spaces()

                        self._state.following.pop()
                        self.match(self.input, COMMA, self.FOLLOW_COMMA_in_valueList1308)
                        self._state.following.append(self.FOLLOW_spaces_in_valueList1310)
                        self.spaces()

                        self._state.following.pop()
                        self._state.following.append(self.FOLLOW_dotValue_in_valueList1312)
                        self.dotValue()

                        self._state.following.pop()


                    else:
                        break #loop13





                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass

        return retval

    # $ANTLR end "valueList"

    class compOpt_return(ParserRuleReturnScope):
        def __init__(self):
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "compOpt"
    # SqlDAS.g:128:1: compOpt : ( ( EQ ) | ( LT ) | ( GT ) | ( NOT ) ( EQ ) | ( EQ ) ( GT ) | ( EQ ) ( LT ) | ( LT ) ( EQ ) | ( GT ) ( EQ ) | ( LT ) ( GT ) );
    def compOpt(self, ):

        retval = self.compOpt_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:128:10: ( ( EQ ) | ( LT ) | ( GT ) | ( NOT ) ( EQ ) | ( EQ ) ( GT ) | ( EQ ) ( LT ) | ( LT ) ( EQ ) | ( GT ) ( EQ ) | ( LT ) ( GT ) )
                alt14 = 9
                alt14 = self.dfa14.predict(self.input)
                if alt14 == 1:
                    # SqlDAS.g:128:11: ( EQ )
                    pass 
                    # SqlDAS.g:128:11: ( EQ )
                    # SqlDAS.g:128:12: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt1324)





                elif alt14 == 2:
                    # SqlDAS.g:129:4: ( LT )
                    pass 
                    # SqlDAS.g:129:4: ( LT )
                    # SqlDAS.g:129:5: LT
                    pass 
                    self.match(self.input, LT, self.FOLLOW_LT_in_compOpt1331)





                elif alt14 == 3:
                    # SqlDAS.g:130:4: ( GT )
                    pass 
                    # SqlDAS.g:130:4: ( GT )
                    # SqlDAS.g:130:5: GT
                    pass 
                    self.match(self.input, GT, self.FOLLOW_GT_in_compOpt1338)





                elif alt14 == 4:
                    # SqlDAS.g:131:4: ( NOT ) ( EQ )
                    pass 
                    # SqlDAS.g:131:4: ( NOT )
                    # SqlDAS.g:131:5: NOT
                    pass 
                    self.match(self.input, NOT, self.FOLLOW_NOT_in_compOpt1345)



                    # SqlDAS.g:131:9: ( EQ )
                    # SqlDAS.g:131:10: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt1348)





                elif alt14 == 5:
                    # SqlDAS.g:132:4: ( EQ ) ( GT )
                    pass 
                    # SqlDAS.g:132:4: ( EQ )
                    # SqlDAS.g:132:5: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt1355)



                    # SqlDAS.g:132:8: ( GT )
                    # SqlDAS.g:132:9: GT
                    pass 
                    self.match(self.input, GT, self.FOLLOW_GT_in_compOpt1358)





                elif alt14 == 6:
                    # SqlDAS.g:133:4: ( EQ ) ( LT )
                    pass 
                    # SqlDAS.g:133:4: ( EQ )
                    # SqlDAS.g:133:5: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt1365)



                    # SqlDAS.g:133:8: ( LT )
                    # SqlDAS.g:133:9: LT
                    pass 
                    self.match(self.input, LT, self.FOLLOW_LT_in_compOpt1368)





                elif alt14 == 7:
                    # SqlDAS.g:134:4: ( LT ) ( EQ )
                    pass 
                    # SqlDAS.g:134:4: ( LT )
                    # SqlDAS.g:134:5: LT
                    pass 
                    self.match(self.input, LT, self.FOLLOW_LT_in_compOpt1375)



                    # SqlDAS.g:134:8: ( EQ )
                    # SqlDAS.g:134:9: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt1378)





                elif alt14 == 8:
                    # SqlDAS.g:135:4: ( GT ) ( EQ )
                    pass 
                    # SqlDAS.g:135:4: ( GT )
                    # SqlDAS.g:135:5: GT
                    pass 
                    self.match(self.input, GT, self.FOLLOW_GT_in_compOpt1385)



                    # SqlDAS.g:135:8: ( EQ )
                    # SqlDAS.g:135:9: EQ
                    pass 
                    self.match(self.input, EQ, self.FOLLOW_EQ_in_compOpt1388)





                elif alt14 == 9:
                    # SqlDAS.g:136:4: ( LT ) ( GT )
                    pass 
                    # SqlDAS.g:136:4: ( LT )
                    # SqlDAS.g:136:5: LT
                    pass 
                    self.match(self.input, LT, self.FOLLOW_LT_in_compOpt1395)



                    # SqlDAS.g:136:8: ( GT )
                    # SqlDAS.g:136:9: GT
                    pass 
                    self.match(self.input, GT, self.FOLLOW_GT_in_compOpt1398)





                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass

        return retval

    # $ANTLR end "compOpt"

    class genValue_return(ParserRuleReturnScope):
        def __init__(self):
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "genValue"
    # SqlDAS.g:138:1: genValue : ( dotValue | dotValue compOpt dotValue ( AMP dotValue compOpt dotValue )* );
    def genValue(self, ):

        retval = self.genValue_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:138:10: ( dotValue | dotValue compOpt dotValue ( AMP dotValue compOpt dotValue )* )
                alt16 = 2
                alt16 = self.dfa16.predict(self.input)
                if alt16 == 1:
                    # SqlDAS.g:138:11: dotValue
                    pass 
                    self._state.following.append(self.FOLLOW_dotValue_in_genValue1406)
                    self.dotValue()

                    self._state.following.pop()


                elif alt16 == 2:
                    # SqlDAS.g:139:4: dotValue compOpt dotValue ( AMP dotValue compOpt dotValue )*
                    pass 
                    self._state.following.append(self.FOLLOW_dotValue_in_genValue1411)
                    self.dotValue()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_compOpt_in_genValue1413)
                    self.compOpt()

                    self._state.following.pop()
                    self._state.following.append(self.FOLLOW_dotValue_in_genValue1415)
                    self.dotValue()

                    self._state.following.pop()
                    # SqlDAS.g:139:30: ( AMP dotValue compOpt dotValue )*
                    while True: #loop15
                        alt15 = 2
                        LA15_0 = self.input.LA(1)

                        if (LA15_0 == AMP) :
                            alt15 = 1


                        if alt15 == 1:
                            # SqlDAS.g:139:31: AMP dotValue compOpt dotValue
                            pass 
                            self.match(self.input, AMP, self.FOLLOW_AMP_in_genValue1418)
                            self._state.following.append(self.FOLLOW_dotValue_in_genValue1420)
                            self.dotValue()

                            self._state.following.pop()
                            self._state.following.append(self.FOLLOW_compOpt_in_genValue1422)
                            self.compOpt()

                            self._state.following.pop()
                            self._state.following.append(self.FOLLOW_dotValue_in_genValue1424)
                            self.dotValue()

                            self._state.following.pop()


                        else:
                            break #loop15




                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass

        return retval

    # $ANTLR end "genValue"

    class betValue_return(ParserRuleReturnScope):
        def __init__(self):
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "betValue"
    # SqlDAS.g:140:1: betValue : dotValue spaces andpython spaces dotValue ;
    def betValue(self, ):

        retval = self.betValue_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:140:10: ( dotValue spaces andpython spaces dotValue )
                # SqlDAS.g:140:11: dotValue spaces andpython spaces dotValue
                pass 
                self._state.following.append(self.FOLLOW_dotValue_in_betValue1432)
                self.dotValue()

                self._state.following.pop()
                self._state.following.append(self.FOLLOW_spaces_in_betValue1434)
                self.spaces()

                self._state.following.pop()
                self._state.following.append(self.FOLLOW_andpython_in_betValue1436)
                self.andpython()

                self._state.following.pop()
                self._state.following.append(self.FOLLOW_spaces_in_betValue1438)
                self.spaces()

                self._state.following.pop()
                self._state.following.append(self.FOLLOW_dotValue_in_betValue1440)
                self.dotValue()

                self._state.following.pop()



                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass

        return retval

    # $ANTLR end "betValue"

    class logicalOp_return(ParserRuleReturnScope):
        def __init__(self):
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "logicalOp"
    # SqlDAS.g:146:1: logicalOp : ( andpython | orpython ) ;
    def logicalOp(self, ):

        retval = self.logicalOp_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:146:11: ( ( andpython | orpython ) )
                # SqlDAS.g:146:12: ( andpython | orpython )
                pass 
                # SqlDAS.g:146:12: ( andpython | orpython )
                alt17 = 2
                LA17_0 = self.input.LA(1)

                if ((88 <= LA17_0 <= 89)) :
                    alt17 = 1
                elif ((94 <= LA17_0 <= 95)) :
                    alt17 = 2
                else:
                    nvae = NoViableAltException("", 17, 0, self.input)

                    raise nvae

                if alt17 == 1:
                    # SqlDAS.g:146:13: andpython
                    pass 
                    self._state.following.append(self.FOLLOW_andpython_in_logicalOp1452)
                    self.andpython()

                    self._state.following.pop()


                elif alt17 == 2:
                    # SqlDAS.g:146:23: orpython
                    pass 
                    self._state.following.append(self.FOLLOW_orpython_in_logicalOp1454)
                    self.orpython()

                    self._state.following.pop()






                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass

        return retval

    # $ANTLR end "logicalOp"


    # $ANTLR start "entity"
    # SqlDAS.g:147:1: entity : ( 'ads' | 'config' | 'dataset' | 'release' | 'tier' | 'site' | 'block' | 'file' | 'primds' | 'procds' | 'run' | 'lumi' | 'dq' | 'ilumi' | 'phygrp' | 'group' | 'pset' | 'algo' | 'datatype' | 'mcdesc' | 'trigdesc' | 'branch' ) ;
    def entity(self, ):

        try:
            try:
                # SqlDAS.g:147:9: ( ( 'ads' | 'config' | 'dataset' | 'release' | 'tier' | 'site' | 'block' | 'file' | 'primds' | 'procds' | 'run' | 'lumi' | 'dq' | 'ilumi' | 'phygrp' | 'group' | 'pset' | 'algo' | 'datatype' | 'mcdesc' | 'trigdesc' | 'branch' ) )
                # SqlDAS.g:147:11: ( 'ads' | 'config' | 'dataset' | 'release' | 'tier' | 'site' | 'block' | 'file' | 'primds' | 'procds' | 'run' | 'lumi' | 'dq' | 'ilumi' | 'phygrp' | 'group' | 'pset' | 'algo' | 'datatype' | 'mcdesc' | 'trigdesc' | 'branch' )
                pass 
                if (20 <= self.input.LA(1) <= 41):
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

    # $ANTLR end "entity"


    # $ANTLR start "attr"
    # SqlDAS.g:148:1: attr : ( 'createdate' | 'moddate' | 'starttime' | 'endtime' | 'createby' | 'modby' | 'name' | 'dataset' | 'version' | 'number' | 'startevnum' | 'endevnum' | 'numevents' | 'numfiles' | 'numlss' | 'totlumi' | 'store' | 'size' | 'release' | 'count' | 'status' | 'type' | 'id' | 'parent' | 'child' | 'tier' | 'def' | 'evnum' | 'era' | 'tag' | 'xsection' | 'hash' | 'content' | 'family' | 'exe' | 'annotation' | 'checksum' ) ;
    def attr(self, ):

        try:
            try:
                # SqlDAS.g:148:7: ( ( 'createdate' | 'moddate' | 'starttime' | 'endtime' | 'createby' | 'modby' | 'name' | 'dataset' | 'version' | 'number' | 'startevnum' | 'endevnum' | 'numevents' | 'numfiles' | 'numlss' | 'totlumi' | 'store' | 'size' | 'release' | 'count' | 'status' | 'type' | 'id' | 'parent' | 'child' | 'tier' | 'def' | 'evnum' | 'era' | 'tag' | 'xsection' | 'hash' | 'content' | 'family' | 'exe' | 'annotation' | 'checksum' ) )
                # SqlDAS.g:148:8: ( 'createdate' | 'moddate' | 'starttime' | 'endtime' | 'createby' | 'modby' | 'name' | 'dataset' | 'version' | 'number' | 'startevnum' | 'endevnum' | 'numevents' | 'numfiles' | 'numlss' | 'totlumi' | 'store' | 'size' | 'release' | 'count' | 'status' | 'type' | 'id' | 'parent' | 'child' | 'tier' | 'def' | 'evnum' | 'era' | 'tag' | 'xsection' | 'hash' | 'content' | 'family' | 'exe' | 'annotation' | 'checksum' )
                pass 
                if (22 <= self.input.LA(1) <= 24) or (42 <= self.input.LA(1) <= 75):
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

    # $ANTLR end "attr"


    # $ANTLR start "funct"
    # SqlDAS.g:149:1: funct : ( 'numruns()' | 'numfiles()' | 'dataquality()' | 'latest()' | 'parentrelease()' | 'childrelease()' | 'intluminosity()' | 'findevents()' ) ;
    def funct(self, ):

        try:
            try:
                # SqlDAS.g:149:8: ( ( 'numruns()' | 'numfiles()' | 'dataquality()' | 'latest()' | 'parentrelease()' | 'childrelease()' | 'intluminosity()' | 'findevents()' ) )
                # SqlDAS.g:149:9: ( 'numruns()' | 'numfiles()' | 'dataquality()' | 'latest()' | 'parentrelease()' | 'childrelease()' | 'intluminosity()' | 'findevents()' )
                pass 
                if (76 <= self.input.LA(1) <= 83):
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

    # $ANTLR end "funct"


    # $ANTLR start "select"
    # SqlDAS.g:150:1: select : ( 'select' | 'SELECT' | 'find' | 'FIND' ) ;
    def select(self, ):

        try:
            try:
                # SqlDAS.g:150:9: ( ( 'select' | 'SELECT' | 'find' | 'FIND' ) )
                # SqlDAS.g:150:10: ( 'select' | 'SELECT' | 'find' | 'FIND' )
                pass 
                if (84 <= self.input.LA(1) <= 87):
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
    # SqlDAS.g:151:1: andpython : ( 'and' | 'AND' ) ;
    def andpython(self, ):

        try:
            try:
                # SqlDAS.g:151:11: ( ( 'and' | 'AND' ) )
                # SqlDAS.g:151:12: ( 'and' | 'AND' )
                pass 
                if (88 <= self.input.LA(1) <= 89):
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
    # SqlDAS.g:152:1: order : ( 'order' | 'ORDER' ) ;
    def order(self, ):

        try:
            try:
                # SqlDAS.g:152:8: ( ( 'order' | 'ORDER' ) )
                # SqlDAS.g:152:9: ( 'order' | 'ORDER' )
                pass 
                if (90 <= self.input.LA(1) <= 91):
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
    # SqlDAS.g:153:1: by : ( 'by' | 'BY' ) ;
    def by(self, ):

        try:
            try:
                # SqlDAS.g:153:5: ( ( 'by' | 'BY' ) )
                # SqlDAS.g:153:6: ( 'by' | 'BY' )
                pass 
                if (92 <= self.input.LA(1) <= 93):
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
    # SqlDAS.g:154:1: orpython : ( 'or' | 'OR' ) ;
    def orpython(self, ):

        try:
            try:
                # SqlDAS.g:154:10: ( ( 'or' | 'OR' ) )
                # SqlDAS.g:154:11: ( 'or' | 'OR' )
                pass 
                if (94 <= self.input.LA(1) <= 95):
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
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "inpython"
    # SqlDAS.g:155:1: inpython : ( 'in' | 'IN' ) ;
    def inpython(self, ):

        retval = self.inpython_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:155:10: ( ( 'in' | 'IN' ) )
                # SqlDAS.g:155:11: ( 'in' | 'IN' )
                pass 
                if self.input.LA(1) == 19 or self.input.LA(1) == 96:
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
    # SqlDAS.g:156:1: notpython : ( 'not' | 'NOT' ) ;
    def notpython(self, ):

        try:
            try:
                # SqlDAS.g:156:11: ( ( 'not' | 'NOT' ) )
                # SqlDAS.g:156:12: ( 'not' | 'NOT' )
                pass 
                if (97 <= self.input.LA(1) <= 98):
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
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "like"
    # SqlDAS.g:157:1: like : ( 'like' | 'LIKE' | 'not' spaces 'like' | 'NOT' spaces 'LIKE' ) ;
    def like(self, ):

        retval = self.like_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:157:7: ( ( 'like' | 'LIKE' | 'not' spaces 'like' | 'NOT' spaces 'LIKE' ) )
                # SqlDAS.g:157:8: ( 'like' | 'LIKE' | 'not' spaces 'like' | 'NOT' spaces 'LIKE' )
                pass 
                # SqlDAS.g:157:8: ( 'like' | 'LIKE' | 'not' spaces 'like' | 'NOT' spaces 'LIKE' )
                alt18 = 4
                LA18 = self.input.LA(1)
                if LA18 == 99:
                    alt18 = 1
                elif LA18 == 100:
                    alt18 = 2
                elif LA18 == 97:
                    alt18 = 3
                elif LA18 == 98:
                    alt18 = 4
                else:
                    nvae = NoViableAltException("", 18, 0, self.input)

                    raise nvae

                if alt18 == 1:
                    # SqlDAS.g:157:9: 'like'
                    pass 
                    self.match(self.input, 99, self.FOLLOW_99_in_like1841)


                elif alt18 == 2:
                    # SqlDAS.g:157:18: 'LIKE'
                    pass 
                    self.match(self.input, 100, self.FOLLOW_100_in_like1845)


                elif alt18 == 3:
                    # SqlDAS.g:157:27: 'not' spaces 'like'
                    pass 
                    self.match(self.input, 97, self.FOLLOW_97_in_like1849)
                    self._state.following.append(self.FOLLOW_spaces_in_like1851)
                    self.spaces()

                    self._state.following.pop()
                    self.match(self.input, 99, self.FOLLOW_99_in_like1853)


                elif alt18 == 4:
                    # SqlDAS.g:157:49: 'NOT' spaces 'LIKE'
                    pass 
                    self.match(self.input, 98, self.FOLLOW_98_in_like1857)
                    self._state.following.append(self.FOLLOW_spaces_in_like1859)
                    self.spaces()

                    self._state.following.pop()
                    self.match(self.input, 100, self.FOLLOW_100_in_like1861)






                retval.stop = self.input.LT(-1)


                        
            except:
            	raise
        finally:

            pass

        return retval

    # $ANTLR end "like"


    # $ANTLR start "count"
    # SqlDAS.g:159:1: count : ( 'count' | 'COUNT' ) ;
    def count(self, ):

        try:
            try:
                # SqlDAS.g:159:8: ( ( 'count' | 'COUNT' ) )
                # SqlDAS.g:159:9: ( 'count' | 'COUNT' )
                pass 
                if self.input.LA(1) == 59 or self.input.LA(1) == 101:
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
    # SqlDAS.g:160:1: sum : ( 'sum' | 'SUM' ) ;
    def sum(self, ):

        try:
            try:
                # SqlDAS.g:160:6: ( ( 'sum' | 'SUM' ) )
                # SqlDAS.g:160:7: ( 'sum' | 'SUM' )
                pass 
                if (102 <= self.input.LA(1) <= 103):
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
    # SqlDAS.g:161:1: asc : ( 'asc' | 'ASC' ) ;
    def asc(self, ):

        try:
            try:
                # SqlDAS.g:161:6: ( ( 'asc' | 'ASC' ) )
                # SqlDAS.g:161:7: ( 'asc' | 'ASC' )
                pass 
                if (104 <= self.input.LA(1) <= 105):
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
    # SqlDAS.g:162:1: desc : ( 'desc' | 'DESC' ) ;
    def desc(self, ):

        try:
            try:
                # SqlDAS.g:162:7: ( ( 'desc' | 'DESC' ) )
                # SqlDAS.g:162:8: ( 'desc' | 'DESC' )
                pass 
                if (106 <= self.input.LA(1) <= 107):
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
            ParserRuleReturnScope.__init__(self)





    # $ANTLR start "between"
    # SqlDAS.g:163:1: between : ( 'between' | 'BETWEEN' ) ;
    def between(self, ):

        retval = self.between_return()
        retval.start = self.input.LT(1)

        try:
            try:
                # SqlDAS.g:163:10: ( ( 'between' | 'BETWEEN' ) )
                # SqlDAS.g:163:11: ( 'between' | 'BETWEEN' )
                pass 
                if (108 <= self.input.LA(1) <= 109):
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
    # SqlDAS.g:164:1: lb : ( '(' ) ;
    def lb(self, ):

        try:
            try:
                # SqlDAS.g:164:5: ( ( '(' ) )
                # SqlDAS.g:164:7: ( '(' )
                pass 
                # SqlDAS.g:164:7: ( '(' )
                # SqlDAS.g:164:8: '('
                pass 
                self.match(self.input, 15, self.FOLLOW_15_in_lb1937)







                        
            except:
            	raise
        finally:

            pass

        return 

    # $ANTLR end "lb"


    # $ANTLR start "rb"
    # SqlDAS.g:165:1: rb : ( ')' ) ;
    def rb(self, ):

        try:
            try:
                # SqlDAS.g:165:5: ( ( ')' ) )
                # SqlDAS.g:165:7: ( ')' )
                pass 
                # SqlDAS.g:165:7: ( ')' )
                # SqlDAS.g:165:8: ')'
                pass 
                self.match(self.input, 16, self.FOLLOW_16_in_rb1947)







                        
            except:
            	raise
        finally:

            pass

        return 

    # $ANTLR end "rb"


    # Delegated rules


    # lookup tables for DFA #2

    DFA2_eot = DFA.unpack(
        u"\61\uffff"
        )

    DFA2_eof = DFA.unpack(
        u"\3\uffff\1\11\3\uffff\1\11\10\uffff\2\11\1\uffff\1\11\14\uffff"
        u"\1\11\1\uffff\2\11\6\uffff\1\11\2\uffff\1\11\2\uffff\1\11"
        )

    DFA2_min = DFA.unpack(
        u"\1\124\5\4\1\26\2\4\3\uffff\15\4\1\6\1\26\6\4\1\26\5\4\1\6\3\4"
        u"\1\26\5\4"
        )

    DFA2_max = DFA.unpack(
        u"\1\127\2\147\1\133\2\17\1\123\1\133\1\147\3\uffff\1\17\1\51\1\17"
        u"\1\51\2\133\1\147\1\133\2\17\1\51\1\20\1\51\1\6\1\123\1\17\1\51"
        u"\1\17\1\51\1\20\1\133\1\113\2\133\1\51\1\20\1\51\1\6\2\20\1\133"
        u"\1\113\1\20\1\133\2\20\1\133"
        )

    DFA2_accept = DFA.unpack(
        u"\11\uffff\1\2\1\1\1\3\45\uffff"
        )

    DFA2_special = DFA.unpack(
        u"\61\uffff"
        )

            
    DFA2_transition = [
        DFA.unpack(u"\4\1"),
        DFA.unpack(u"\1\2\17\uffff\26\3\21\uffff\1\4\51\uffff\1\4\2\5"),
        DFA.unpack(u"\1\2\17\uffff\26\3\21\uffff\1\4\51\uffff\1\4\2\5"),
        DFA.unpack(u"\1\7\1\10\1\6\12\uffff\2\12\107\uffff\2\13"),
        DFA.unpack(u"\1\14\12\uffff\1\15"),
        DFA.unpack(u"\1\16\12\uffff\1\17"),
        DFA.unpack(u"\3\20\21\uffff\42\20\10\21"),
        DFA.unpack(u"\1\7\1\10\13\uffff\2\12\107\uffff\2\13"),
        DFA.unpack(u"\1\22\17\uffff\26\23\21\uffff\1\24\51\uffff\1\24\2"
        u"\25"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\14\12\uffff\1\15"),
        DFA.unpack(u"\1\26\17\uffff\26\27"),
        DFA.unpack(u"\1\16\12\uffff\1\17"),
        DFA.unpack(u"\1\30\17\uffff\26\31"),
        DFA.unpack(u"\1\7\1\10\13\uffff\2\12\107\uffff\2\13"),
        DFA.unpack(u"\1\7\1\10\13\uffff\2\12\107\uffff\2\13"),
        DFA.unpack(u"\1\22\17\uffff\26\23\21\uffff\1\24\51\uffff\1\24\2"
        u"\25"),
        DFA.unpack(u"\1\7\1\10\1\32\12\uffff\2\12\107\uffff\2\13"),
        DFA.unpack(u"\1\33\12\uffff\1\34"),
        DFA.unpack(u"\1\35\12\uffff\1\36"),
        DFA.unpack(u"\1\26\17\uffff\26\27"),
        DFA.unpack(u"\1\37\13\uffff\1\40"),
        DFA.unpack(u"\1\30\17\uffff\26\31"),
        DFA.unpack(u"\1\41"),
        DFA.unpack(u"\3\43\21\uffff\42\43\10\42"),
        DFA.unpack(u"\1\33\12\uffff\1\34"),
        DFA.unpack(u"\1\44\17\uffff\26\45"),
        DFA.unpack(u"\1\35\12\uffff\1\36"),
        DFA.unpack(u"\1\46\17\uffff\26\47"),
        DFA.unpack(u"\1\37\13\uffff\1\40"),
        DFA.unpack(u"\1\7\1\10\13\uffff\2\12\107\uffff\2\13"),
        DFA.unpack(u"\3\50\21\uffff\42\50"),
        DFA.unpack(u"\1\7\1\10\13\uffff\2\12\107\uffff\2\13"),
        DFA.unpack(u"\1\7\1\10\13\uffff\2\12\107\uffff\2\13"),
        DFA.unpack(u"\1\44\17\uffff\26\45"),
        DFA.unpack(u"\1\51\13\uffff\1\52"),
        DFA.unpack(u"\1\46\17\uffff\26\47"),
        DFA.unpack(u"\1\53"),
        DFA.unpack(u"\1\54\13\uffff\1\55"),
        DFA.unpack(u"\1\51\13\uffff\1\52"),
        DFA.unpack(u"\1\7\1\10\13\uffff\2\12\107\uffff\2\13"),
        DFA.unpack(u"\3\56\21\uffff\42\56"),
        DFA.unpack(u"\1\54\13\uffff\1\55"),
        DFA.unpack(u"\1\7\1\10\13\uffff\2\12\107\uffff\2\13"),
        DFA.unpack(u"\1\57\13\uffff\1\60"),
        DFA.unpack(u"\1\57\13\uffff\1\60"),
        DFA.unpack(u"\1\7\1\10\13\uffff\2\12\107\uffff\2\13")
    ]

    # class definition for DFA #2

    DFA2 = DFA
    # lookup tables for DFA #4

    DFA4_eot = DFA.unpack(
        u"\4\uffff"
        )

    DFA4_eof = DFA.unpack(
        u"\2\2\2\uffff"
        )

    DFA4_min = DFA.unpack(
        u"\2\4\2\uffff"
        )

    DFA4_max = DFA.unpack(
        u"\2\153\2\uffff"
        )

    DFA4_accept = DFA.unpack(
        u"\2\uffff\1\2\1\1"
        )

    DFA4_special = DFA.unpack(
        u"\4\uffff"
        )

            
    DFA4_transition = [
        DFA.unpack(u"\1\1\1\3\124\uffff\2\2\14\uffff\4\2"),
        DFA.unpack(u"\1\1\1\3\124\uffff\2\2\14\uffff\4\2"),
        DFA.unpack(u""),
        DFA.unpack(u"")
    ]

    # class definition for DFA #4

    DFA4 = DFA
    # lookup tables for DFA #6

    DFA6_eot = DFA.unpack(
        u"\4\uffff"
        )

    DFA6_eof = DFA.unpack(
        u"\2\2\2\uffff"
        )

    DFA6_min = DFA.unpack(
        u"\2\4\2\uffff"
        )

    DFA6_max = DFA.unpack(
        u"\2\133\2\uffff"
        )

    DFA6_accept = DFA.unpack(
        u"\2\uffff\1\2\1\1"
        )

    DFA6_special = DFA.unpack(
        u"\4\uffff"
        )

            
    DFA6_transition = [
        DFA.unpack(u"\1\1\1\3\13\uffff\2\2\107\uffff\2\2"),
        DFA.unpack(u"\1\1\1\3\13\uffff\2\2\107\uffff\2\2"),
        DFA.unpack(u""),
        DFA.unpack(u"")
    ]

    # class definition for DFA #6

    DFA6 = DFA
    # lookup tables for DFA #8

    DFA8_eot = DFA.unpack(
        u"\4\uffff"
        )

    DFA8_eof = DFA.unpack(
        u"\2\2\2\uffff"
        )

    DFA8_min = DFA.unpack(
        u"\2\4\2\uffff"
        )

    DFA8_max = DFA.unpack(
        u"\2\137\2\uffff"
        )

    DFA8_accept = DFA.unpack(
        u"\2\uffff\1\2\1\1"
        )

    DFA8_special = DFA.unpack(
        u"\4\uffff"
        )

            
    DFA8_transition = [
        DFA.unpack(u"\1\1\123\uffff\2\3\2\2\2\uffff\2\3"),
        DFA.unpack(u"\1\1\123\uffff\2\3\2\2\2\uffff\2\3"),
        DFA.unpack(u""),
        DFA.unpack(u"")
    ]

    # class definition for DFA #8

    DFA8 = DFA
    # lookup tables for DFA #11

    DFA11_eot = DFA.unpack(
        u"\32\uffff"
        )

    DFA11_eof = DFA.unpack(
        u"\32\uffff"
        )

    DFA11_min = DFA.unpack(
        u"\1\24\4\4\1\uffff\1\26\3\uffff\11\4\1\6\2\4\1\26\3\4"
        )

    DFA11_max = DFA.unpack(
        u"\1\147\1\155\2\17\1\155\1\uffff\1\123\3\uffff\1\17\1\51\1\17\1"
        u"\51\2\155\1\51\1\20\1\51\1\6\1\20\1\155\1\113\2\20\1\155"
        )

    DFA11_accept = DFA.unpack(
        u"\5\uffff\1\1\1\uffff\1\3\1\2\1\4\20\uffff"
        )

    DFA11_special = DFA.unpack(
        u"\32\uffff"
        )

            
    DFA11_transition = [
        DFA.unpack(u"\26\1\21\uffff\1\2\51\uffff\1\2\2\3"),
        DFA.unpack(u"\1\4\1\uffff\1\6\1\uffff\4\5\7\uffff\1\10\114\uffff"
        u"\1\10\4\7\7\uffff\2\11"),
        DFA.unpack(u"\1\12\12\uffff\1\13"),
        DFA.unpack(u"\1\14\12\uffff\1\15"),
        DFA.unpack(u"\1\4\3\uffff\4\5\7\uffff\1\10\114\uffff\1\10\4\7\7"
        u"\uffff\2\11"),
        DFA.unpack(u""),
        DFA.unpack(u"\3\17\21\uffff\42\17\10\16"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\12\12\uffff\1\13"),
        DFA.unpack(u"\1\20\17\uffff\26\21"),
        DFA.unpack(u"\1\14\12\uffff\1\15"),
        DFA.unpack(u"\1\22\17\uffff\26\23"),
        DFA.unpack(u"\1\4\3\uffff\4\5\7\uffff\1\10\114\uffff\1\10\4\7\7"
        u"\uffff\2\11"),
        DFA.unpack(u"\1\4\3\uffff\4\5\7\uffff\1\10\114\uffff\1\10\4\7\7"
        u"\uffff\2\11"),
        DFA.unpack(u"\1\20\17\uffff\26\21"),
        DFA.unpack(u"\1\24\13\uffff\1\25"),
        DFA.unpack(u"\1\22\17\uffff\26\23"),
        DFA.unpack(u"\1\26"),
        DFA.unpack(u"\1\24\13\uffff\1\25"),
        DFA.unpack(u"\1\4\3\uffff\4\5\7\uffff\1\10\114\uffff\1\10\4\7\7"
        u"\uffff\2\11"),
        DFA.unpack(u"\3\27\21\uffff\42\27"),
        DFA.unpack(u"\1\30\13\uffff\1\31"),
        DFA.unpack(u"\1\30\13\uffff\1\31"),
        DFA.unpack(u"\1\4\3\uffff\4\5\7\uffff\1\10\114\uffff\1\10\4\7\7"
        u"\uffff\2\11")
    ]

    # class definition for DFA #11

    DFA11 = DFA
    # lookup tables for DFA #12

    DFA12_eot = DFA.unpack(
        u"\56\uffff"
        )

    DFA12_eof = DFA.unpack(
        u"\1\uffff\1\5\2\uffff\1\5\2\uffff\1\12\1\14\2\uffff\1\14\2\uffff"
        u"\1\21\4\uffff\1\25\3\uffff\1\31\3\uffff\1\35\3\uffff\1\41\3\uffff"
        u"\1\45\3\uffff\1\51\3\uffff\1\55\2\uffff"
        )

    DFA12_min = DFA.unpack(
        u"\1\7\1\4\1\uffff\1\7\1\4\2\uffff\2\4\1\7\1\uffff\1\4\2\uffff\1"
        u"\4\1\uffff\1\7\2\uffff\1\4\1\7\2\uffff\1\4\1\7\2\uffff\1\4\1\7"
        u"\2\uffff\1\4\1\7\2\uffff\1\4\1\7\2\uffff\1\4\1\7\2\uffff\1\4\2"
        u"\uffff"
        )

    DFA12_max = DFA.unpack(
        u"\1\23\1\137\1\uffff\1\23\1\137\2\uffff\2\137\1\23\1\uffff\1\137"
        u"\2\uffff\1\137\1\uffff\1\23\2\uffff\1\137\1\23\2\uffff\1\137\1"
        u"\23\2\uffff\1\137\1\23\2\uffff\1\137\1\23\2\uffff\1\137\1\23\2"
        u"\uffff\1\137\1\23\2\uffff\1\137\2\uffff"
        )

    DFA12_accept = DFA.unpack(
        u"\2\uffff\1\2\2\uffff\1\1\1\3\3\uffff\1\4\1\uffff\1\26\1\5\1\uffff"
        u"\1\27\1\uffff\1\6\1\7\2\uffff\1\10\1\11\2\uffff\1\12\1\13\2\uffff"
        u"\1\14\1\15\2\uffff\1\16\1\17\2\uffff\1\20\1\21\2\uffff\1\22\1\23"
        u"\1\uffff\1\25\1\24"
        )

    DFA12_special = DFA.unpack(
        u"\56\uffff"
        )

            
    DFA12_transition = [
        DFA.unpack(u"\1\1\13\uffff\1\2"),
        DFA.unpack(u"\1\4\1\5\1\3\1\uffff\5\5\3\uffff\1\5\107\uffff\4\5"
        u"\2\uffff\2\5"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\7\13\uffff\1\6"),
        DFA.unpack(u"\2\5\1\uffff\1\10\10\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\12\1\11\1\uffff\5\12\3\uffff\1\12\107\uffff\4\12"
        u"\2\uffff\2\12"),
        DFA.unpack(u"\1\13\1\14\2\uffff\5\14\3\uffff\1\14\107\uffff\4\14"
        u"\2\uffff\2\14"),
        DFA.unpack(u"\1\16\13\uffff\1\15"),
        DFA.unpack(u""),
        DFA.unpack(u"\2\14\1\uffff\1\17\10\uffff\1\14\107\uffff\4\14\2\uffff"
        u"\2\14"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\21\1\20\1\uffff\5\21\3\uffff\1\21\107\uffff\4\21"
        u"\2\uffff\2\21"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\23\13\uffff\1\22"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\25\1\24\1\uffff\5\25\3\uffff\1\25\107\uffff\4\25"
        u"\2\uffff\2\25"),
        DFA.unpack(u"\1\27\13\uffff\1\26"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\31\1\30\1\uffff\5\31\3\uffff\1\31\107\uffff\4\31"
        u"\2\uffff\2\31"),
        DFA.unpack(u"\1\33\13\uffff\1\32"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\35\1\34\1\uffff\5\35\3\uffff\1\35\107\uffff\4\35"
        u"\2\uffff\2\35"),
        DFA.unpack(u"\1\37\13\uffff\1\36"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\41\1\40\1\uffff\5\41\3\uffff\1\41\107\uffff\4\41"
        u"\2\uffff\2\41"),
        DFA.unpack(u"\1\43\13\uffff\1\42"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\45\1\44\1\uffff\5\45\3\uffff\1\45\107\uffff\4\45"
        u"\2\uffff\2\45"),
        DFA.unpack(u"\1\47\13\uffff\1\46"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\51\1\50\1\uffff\5\51\3\uffff\1\51\107\uffff\4\51"
        u"\2\uffff\2\51"),
        DFA.unpack(u"\1\53\13\uffff\1\52"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\55\1\54\1\uffff\5\55\3\uffff\1\55\107\uffff\4\55"
        u"\2\uffff\2\55"),
        DFA.unpack(u""),
        DFA.unpack(u"")
    ]

    # class definition for DFA #12

    DFA12 = DFA
    # lookup tables for DFA #13

    DFA13_eot = DFA.unpack(
        u"\4\uffff"
        )

    DFA13_eof = DFA.unpack(
        u"\4\uffff"
        )

    DFA13_min = DFA.unpack(
        u"\2\4\2\uffff"
        )

    DFA13_max = DFA.unpack(
        u"\2\20\2\uffff"
        )

    DFA13_accept = DFA.unpack(
        u"\2\uffff\1\2\1\1"
        )

    DFA13_special = DFA.unpack(
        u"\4\uffff"
        )

            
    DFA13_transition = [
        DFA.unpack(u"\1\1\1\3\12\uffff\1\2"),
        DFA.unpack(u"\1\1\1\3\12\uffff\1\2"),
        DFA.unpack(u""),
        DFA.unpack(u"")
    ]

    # class definition for DFA #13

    DFA13 = DFA
    # lookup tables for DFA #14

    DFA14_eot = DFA.unpack(
        u"\15\uffff"
        )

    DFA14_eof = DFA.unpack(
        u"\15\uffff"
        )

    DFA14_min = DFA.unpack(
        u"\1\10\3\4\11\uffff"
        )

    DFA14_max = DFA.unpack(
        u"\1\13\3\23\11\uffff"
        )

    DFA14_accept = DFA.unpack(
        u"\4\uffff\1\4\1\6\1\5\1\1\1\11\1\2\1\7\1\10\1\3"
        )

    DFA14_special = DFA.unpack(
        u"\15\uffff"
        )

            
    DFA14_transition = [
        DFA.unpack(u"\1\1\1\2\1\3\1\4"),
        DFA.unpack(u"\1\7\2\uffff\1\7\1\uffff\1\5\1\6\10\uffff\1\7"),
        DFA.unpack(u"\1\11\2\uffff\1\11\1\12\1\uffff\1\10\10\uffff\1\11"),
        DFA.unpack(u"\1\14\2\uffff\1\14\1\13\12\uffff\1\14"),
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

    # class definition for DFA #14

    DFA14 = DFA
    # lookup tables for DFA #16

    DFA16_eot = DFA.unpack(
        u"\46\uffff"
        )

    DFA16_eof = DFA.unpack(
        u"\1\uffff\2\5\1\uffff\1\5\2\uffff\3\5\1\uffff\4\5\1\uffff\2\5\1"
        u"\uffff\2\5\1\uffff\2\5\1\uffff\2\5\1\uffff\2\5\1\uffff\2\5\1\uffff"
        u"\2\5\1\uffff\1\5"
        )

    DFA16_min = DFA.unpack(
        u"\1\7\2\4\1\7\1\4\2\uffff\3\4\1\7\4\4\1\7\2\4\1\7\2\4\1\7\2\4\1"
        u"\7\2\4\1\7\2\4\1\7\2\4\1\7\2\4\1\23\1\4"
        )

    DFA16_max = DFA.unpack(
        u"\1\23\2\137\1\23\1\137\2\uffff\3\137\1\23\4\137\1\23\2\137\1\23"
        u"\2\137\1\23\2\137\1\23\2\137\1\23\2\137\1\23\2\137\1\23\2\137\1"
        u"\23\1\137"
        )

    DFA16_accept = DFA.unpack(
        u"\5\uffff\1\1\1\2\37\uffff"
        )

    DFA16_special = DFA.unpack(
        u"\46\uffff"
        )

            
    DFA16_transition = [
        DFA.unpack(u"\1\1\13\uffff\1\2"),
        DFA.unpack(u"\1\4\1\uffff\1\3\1\uffff\4\6\4\uffff\1\5\107\uffff"
        u"\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\10\13\uffff\1\7"),
        DFA.unpack(u"\1\5\2\uffff\1\11\120\uffff\4\5\2\uffff\2\5"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\5\1\uffff\1\12\1\uffff\4\6\4\uffff\1\5\107\uffff"
        u"\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\13\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\15\13\uffff\1\14"),
        DFA.unpack(u"\1\5\2\uffff\1\16\120\uffff\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\5\1\uffff\1\17\1\uffff\4\6\4\uffff\1\5\107\uffff"
        u"\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\21\13\uffff\1\20"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\5\1\uffff\1\22\1\uffff\4\6\4\uffff\1\5\107\uffff"
        u"\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\24\13\uffff\1\23"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\5\1\uffff\1\25\1\uffff\4\6\4\uffff\1\5\107\uffff"
        u"\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\27\13\uffff\1\26"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\5\1\uffff\1\30\1\uffff\4\6\4\uffff\1\5\107\uffff"
        u"\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\32\13\uffff\1\31"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\5\1\uffff\1\33\1\uffff\4\6\4\uffff\1\5\107\uffff"
        u"\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\35\13\uffff\1\34"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\5\1\uffff\1\36\1\uffff\4\6\4\uffff\1\5\107\uffff"
        u"\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\40\13\uffff\1\37"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\5\1\uffff\1\41\1\uffff\4\6\4\uffff\1\5\107\uffff"
        u"\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\43\13\uffff\1\42"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5"),
        DFA.unpack(u"\1\5\1\uffff\1\44\1\uffff\4\6\4\uffff\1\5\107\uffff"
        u"\4\5\2\uffff\2\5"),
        DFA.unpack(u"\1\45"),
        DFA.unpack(u"\1\5\3\uffff\4\6\4\uffff\1\5\107\uffff\4\5\2\uffff"
        u"\2\5")
    ]

    # class definition for DFA #16

    DFA16 = DFA
 

    FOLLOW_select_in_stmt42 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_spaces_in_stmt44 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_selectList_in_stmt46 = frozenset([4, 17, 18])
    FOLLOW_spaces_in_stmt48 = frozenset([4, 17, 18])
    FOLLOW_where_in_stmt50 = frozenset([4, 15, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_spaces_in_stmt52 = frozenset([4, 15, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_constraintList_in_stmt54 = frozenset([4, 90, 91])
    FOLLOW_spaces_in_stmt56 = frozenset([1, 90, 91])
    FOLLOW_order_in_stmt59 = frozenset([4, 92, 93])
    FOLLOW_spaces_in_stmt61 = frozenset([4, 92, 93])
    FOLLOW_by_in_stmt63 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_spaces_in_stmt65 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_orderList_in_stmt67 = frozenset([1, 90, 91])
    FOLLOW_select_in_stmt74 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_spaces_in_stmt76 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_selectList_in_stmt78 = frozenset([4])
    FOLLOW_spaces_in_stmt80 = frozenset([1])
    FOLLOW_select_in_stmt85 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_spaces_in_stmt87 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_selectList_in_stmt89 = frozenset([4, 90, 91])
    FOLLOW_spaces_in_stmt91 = frozenset([90, 91])
    FOLLOW_order_in_stmt93 = frozenset([4, 92, 93])
    FOLLOW_spaces_in_stmt95 = frozenset([4, 92, 93])
    FOLLOW_by_in_stmt97 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_spaces_in_stmt99 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_orderList_in_stmt101 = frozenset([1])
    FOLLOW_SPACE_in_spaces116 = frozenset([1, 4])
    FOLLOW_keyword_in_orderList128 = frozenset([4, 5, 104, 105, 106, 107])
    FOLLOW_spaces_in_orderList141 = frozenset([5])
    FOLLOW_COMMA_in_orderList145 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_spaces_in_orderList149 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_keyword_in_orderList156 = frozenset([4, 5, 104, 105, 106, 107])
    FOLLOW_spaces_in_orderList171 = frozenset([104, 105, 106, 107])
    FOLLOW_ordering_in_orderList179 = frozenset([1])
    FOLLOW_asc_in_ordering196 = frozenset([1])
    FOLLOW_desc_in_ordering198 = frozenset([1])
    FOLLOW_keyword_in_selectList211 = frozenset([1, 4, 5])
    FOLLOW_spaces_in_selectList224 = frozenset([5])
    FOLLOW_COMMA_in_selectList228 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_spaces_in_selectList232 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_keyword_in_selectList239 = frozenset([1, 4, 5])
    FOLLOW_entity_in_keyword264 = frozenset([1])
    FOLLOW_entity_in_keyword270 = frozenset([6])
    FOLLOW_DOT_in_keyword272 = frozenset([22, 23, 24, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75])
    FOLLOW_attr_in_keyword274 = frozenset([1])
    FOLLOW_entity_in_keyword279 = frozenset([6])
    FOLLOW_DOT_in_keyword281 = frozenset([76, 77, 78, 79, 80, 81, 82, 83])
    FOLLOW_funct_in_keyword283 = frozenset([1])
    FOLLOW_count_in_keyword288 = frozenset([4, 15])
    FOLLOW_spaces_in_keyword290 = frozenset([15])
    FOLLOW_15_in_keyword292 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41])
    FOLLOW_spaces_in_keyword294 = frozenset([20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41])
    FOLLOW_entity_in_keyword296 = frozenset([4, 16])
    FOLLOW_spaces_in_keyword298 = frozenset([16])
    FOLLOW_16_in_keyword300 = frozenset([1])
    FOLLOW_sum_in_keyword305 = frozenset([4, 15])
    FOLLOW_spaces_in_keyword307 = frozenset([15])
    FOLLOW_15_in_keyword309 = frozenset([4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41])
    FOLLOW_spaces_in_keyword311 = frozenset([20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41])
    FOLLOW_entity_in_keyword313 = frozenset([6])
    FOLLOW_DOT_in_keyword315 = frozenset([22, 23, 24, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75])
    FOLLOW_attr_in_keyword317 = frozenset([4, 16])
    FOLLOW_spaces_in_keyword319 = frozenset([16])
    FOLLOW_16_in_keyword321 = frozenset([1])
    FOLLOW_constraint1_in_constraintList330 = frozenset([1, 4, 88, 89, 94, 95])
    FOLLOW_spaces_in_constraintList334 = frozenset([4, 88, 89, 94, 95])
    FOLLOW_logicalOp_in_constraintList341 = frozenset([4, 15, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_spaces_in_constraintList349 = frozenset([4, 15, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_constraint1_in_constraintList351 = frozenset([1, 4, 88, 89, 94, 95])
    FOLLOW_lb_in_lopen362 = frozenset([1, 15])
    FOLLOW_rb_in_ropen373 = frozenset([1, 16])
    FOLLOW_lopen_in_constraint1391 = frozenset([4, 15, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 59, 101, 102, 103])
    FOLLOW_constraint_in_constraint1417 = frozenset([16])
    FOLLOW_ropen_in_constraint1443 = frozenset([1])
    FOLLOW_keyword_in_constraint462 = frozenset([4, 8, 9, 10, 11])
    FOLLOW_spaces_in_constraint473 = frozenset([4, 8, 9, 10, 11])
    FOLLOW_compOpt_in_constraint480 = frozenset([4, 7, 19])
    FOLLOW_spaces_in_constraint490 = frozenset([4, 7, 19])
    FOLLOW_genValue_in_constraint497 = frozenset([1])
    FOLLOW_keyword_in_constraint512 = frozenset([4, 19, 96])
    FOLLOW_spaces_in_constraint523 = frozenset([4, 19, 96])
    FOLLOW_inpython_in_constraint530 = frozenset([4, 15])
    FOLLOW_spaces_in_constraint541 = frozenset([15])
    FOLLOW_15_in_constraint543 = frozenset([4, 7, 19])
    FOLLOW_spaces_in_constraint547 = frozenset([4, 7, 19])
    FOLLOW_valueList_in_constraint553 = frozenset([4, 16])
    FOLLOW_spaces_in_constraint563 = frozenset([16])
    FOLLOW_16_in_constraint567 = frozenset([1])
    FOLLOW_keyword_in_constraint593 = frozenset([4, 97, 98, 99, 100])
    FOLLOW_spaces_in_constraint604 = frozenset([4, 97, 98, 99, 100])
    FOLLOW_like_in_constraint611 = frozenset([4, 7, 19])
    FOLLOW_spaces_in_constraint620 = frozenset([7, 19])
    FOLLOW_dotValue_in_constraint627 = frozenset([1])
    FOLLOW_keyword_in_constraint644 = frozenset([4, 108, 109])
    FOLLOW_spaces_in_constraint655 = frozenset([4, 108, 109])
    FOLLOW_between_in_constraint662 = frozenset([4, 7, 19])
    FOLLOW_spaces_in_constraint670 = frozenset([4, 7, 19])
    FOLLOW_betValue_in_constraint677 = frozenset([1])
    FOLLOW_set_in_where726 = frozenset([1])
    FOLLOW_VALUE_in_dotValue746 = frozenset([1])
    FOLLOW_19_in_dotValue753 = frozenset([1])
    FOLLOW_VALUE_in_dotValue759 = frozenset([6])
    FOLLOW_DOT_in_dotValue761 = frozenset([19])
    FOLLOW_19_in_dotValue763 = frozenset([1])
    FOLLOW_VALUE_in_dotValue769 = frozenset([6])
    FOLLOW_DOT_in_dotValue771 = frozenset([7])
    FOLLOW_VALUE_in_dotValue773 = frozenset([1])
    FOLLOW_VALUE_in_dotValue779 = frozenset([6])
    FOLLOW_DOT_in_dotValue781 = frozenset([7])
    FOLLOW_VALUE_in_dotValue783 = frozenset([6])
    FOLLOW_DOT_in_dotValue785 = frozenset([19])
    FOLLOW_19_in_dotValue787 = frozenset([1])
    FOLLOW_VALUE_in_dotValue793 = frozenset([6])
    FOLLOW_DOT_in_dotValue795 = frozenset([7])
    FOLLOW_VALUE_in_dotValue797 = frozenset([6])
    FOLLOW_DOT_in_dotValue799 = frozenset([7])
    FOLLOW_VALUE_in_dotValue801 = frozenset([1])
    FOLLOW_VALUE_in_dotValue807 = frozenset([6])
    FOLLOW_DOT_in_dotValue809 = frozenset([7])
    FOLLOW_VALUE_in_dotValue811 = frozenset([6])
    FOLLOW_DOT_in_dotValue813 = frozenset([7])
    FOLLOW_VALUE_in_dotValue815 = frozenset([6])
    FOLLOW_DOT_in_dotValue817 = frozenset([19])
    FOLLOW_19_in_dotValue819 = frozenset([1])
    FOLLOW_VALUE_in_dotValue825 = frozenset([6])
    FOLLOW_DOT_in_dotValue827 = frozenset([7])
    FOLLOW_VALUE_in_dotValue829 = frozenset([6])
    FOLLOW_DOT_in_dotValue831 = frozenset([7])
    FOLLOW_VALUE_in_dotValue833 = frozenset([6])
    FOLLOW_DOT_in_dotValue835 = frozenset([7])
    FOLLOW_VALUE_in_dotValue837 = frozenset([1])
    FOLLOW_VALUE_in_dotValue843 = frozenset([6])
    FOLLOW_DOT_in_dotValue845 = frozenset([7])
    FOLLOW_VALUE_in_dotValue847 = frozenset([6])
    FOLLOW_DOT_in_dotValue849 = frozenset([7])
    FOLLOW_VALUE_in_dotValue851 = frozenset([6])
    FOLLOW_DOT_in_dotValue853 = frozenset([7])
    FOLLOW_VALUE_in_dotValue855 = frozenset([6])
    FOLLOW_DOT_in_dotValue857 = frozenset([19])
    FOLLOW_19_in_dotValue859 = frozenset([1])
    FOLLOW_VALUE_in_dotValue865 = frozenset([6])
    FOLLOW_DOT_in_dotValue867 = frozenset([7])
    FOLLOW_VALUE_in_dotValue869 = frozenset([6])
    FOLLOW_DOT_in_dotValue871 = frozenset([7])
    FOLLOW_VALUE_in_dotValue873 = frozenset([6])
    FOLLOW_DOT_in_dotValue875 = frozenset([7])
    FOLLOW_VALUE_in_dotValue877 = frozenset([6])
    FOLLOW_DOT_in_dotValue879 = frozenset([7])
    FOLLOW_VALUE_in_dotValue881 = frozenset([1])
    FOLLOW_VALUE_in_dotValue887 = frozenset([6])
    FOLLOW_DOT_in_dotValue889 = frozenset([7])
    FOLLOW_VALUE_in_dotValue891 = frozenset([6])
    FOLLOW_DOT_in_dotValue893 = frozenset([7])
    FOLLOW_VALUE_in_dotValue895 = frozenset([6])
    FOLLOW_DOT_in_dotValue897 = frozenset([7])
    FOLLOW_VALUE_in_dotValue899 = frozenset([6])
    FOLLOW_DOT_in_dotValue901 = frozenset([7])
    FOLLOW_VALUE_in_dotValue903 = frozenset([6])
    FOLLOW_DOT_in_dotValue905 = frozenset([19])
    FOLLOW_19_in_dotValue907 = frozenset([1])
    FOLLOW_VALUE_in_dotValue913 = frozenset([6])
    FOLLOW_DOT_in_dotValue915 = frozenset([7])
    FOLLOW_VALUE_in_dotValue917 = frozenset([6])
    FOLLOW_DOT_in_dotValue919 = frozenset([7])
    FOLLOW_VALUE_in_dotValue921 = frozenset([6])
    FOLLOW_DOT_in_dotValue923 = frozenset([7])
    FOLLOW_VALUE_in_dotValue925 = frozenset([6])
    FOLLOW_DOT_in_dotValue927 = frozenset([7])
    FOLLOW_VALUE_in_dotValue929 = frozenset([6])
    FOLLOW_DOT_in_dotValue931 = frozenset([7])
    FOLLOW_VALUE_in_dotValue933 = frozenset([1])
    FOLLOW_VALUE_in_dotValue939 = frozenset([6])
    FOLLOW_DOT_in_dotValue941 = frozenset([7])
    FOLLOW_VALUE_in_dotValue943 = frozenset([6])
    FOLLOW_DOT_in_dotValue945 = frozenset([7])
    FOLLOW_VALUE_in_dotValue947 = frozenset([6])
    FOLLOW_DOT_in_dotValue949 = frozenset([7])
    FOLLOW_VALUE_in_dotValue951 = frozenset([6])
    FOLLOW_DOT_in_dotValue953 = frozenset([7])
    FOLLOW_VALUE_in_dotValue955 = frozenset([6])
    FOLLOW_DOT_in_dotValue957 = frozenset([7])
    FOLLOW_VALUE_in_dotValue959 = frozenset([6])
    FOLLOW_DOT_in_dotValue961 = frozenset([19])
    FOLLOW_19_in_dotValue963 = frozenset([1])
    FOLLOW_VALUE_in_dotValue969 = frozenset([6])
    FOLLOW_DOT_in_dotValue971 = frozenset([7])
    FOLLOW_VALUE_in_dotValue973 = frozenset([6])
    FOLLOW_DOT_in_dotValue975 = frozenset([7])
    FOLLOW_VALUE_in_dotValue977 = frozenset([6])
    FOLLOW_DOT_in_dotValue979 = frozenset([7])
    FOLLOW_VALUE_in_dotValue981 = frozenset([6])
    FOLLOW_DOT_in_dotValue983 = frozenset([7])
    FOLLOW_VALUE_in_dotValue985 = frozenset([6])
    FOLLOW_DOT_in_dotValue987 = frozenset([7])
    FOLLOW_VALUE_in_dotValue989 = frozenset([6])
    FOLLOW_DOT_in_dotValue991 = frozenset([7])
    FOLLOW_VALUE_in_dotValue993 = frozenset([1])
    FOLLOW_VALUE_in_dotValue999 = frozenset([6])
    FOLLOW_DOT_in_dotValue1001 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1003 = frozenset([6])
    FOLLOW_DOT_in_dotValue1005 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1007 = frozenset([6])
    FOLLOW_DOT_in_dotValue1009 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1011 = frozenset([6])
    FOLLOW_DOT_in_dotValue1013 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1015 = frozenset([6])
    FOLLOW_DOT_in_dotValue1017 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1019 = frozenset([6])
    FOLLOW_DOT_in_dotValue1021 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1023 = frozenset([6])
    FOLLOW_DOT_in_dotValue1025 = frozenset([19])
    FOLLOW_19_in_dotValue1027 = frozenset([1])
    FOLLOW_VALUE_in_dotValue1033 = frozenset([6])
    FOLLOW_DOT_in_dotValue1035 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1037 = frozenset([6])
    FOLLOW_DOT_in_dotValue1039 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1041 = frozenset([6])
    FOLLOW_DOT_in_dotValue1043 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1045 = frozenset([6])
    FOLLOW_DOT_in_dotValue1047 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1049 = frozenset([6])
    FOLLOW_DOT_in_dotValue1051 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1053 = frozenset([6])
    FOLLOW_DOT_in_dotValue1055 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1057 = frozenset([6])
    FOLLOW_DOT_in_dotValue1059 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1061 = frozenset([1])
    FOLLOW_VALUE_in_dotValue1067 = frozenset([6])
    FOLLOW_DOT_in_dotValue1069 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1071 = frozenset([6])
    FOLLOW_DOT_in_dotValue1073 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1075 = frozenset([6])
    FOLLOW_DOT_in_dotValue1077 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1079 = frozenset([6])
    FOLLOW_DOT_in_dotValue1081 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1083 = frozenset([6])
    FOLLOW_DOT_in_dotValue1085 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1087 = frozenset([6])
    FOLLOW_DOT_in_dotValue1089 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1091 = frozenset([6])
    FOLLOW_DOT_in_dotValue1093 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1095 = frozenset([6])
    FOLLOW_DOT_in_dotValue1097 = frozenset([19])
    FOLLOW_19_in_dotValue1099 = frozenset([1])
    FOLLOW_VALUE_in_dotValue1105 = frozenset([6])
    FOLLOW_DOT_in_dotValue1107 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1109 = frozenset([6])
    FOLLOW_DOT_in_dotValue1111 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1113 = frozenset([6])
    FOLLOW_DOT_in_dotValue1115 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1117 = frozenset([6])
    FOLLOW_DOT_in_dotValue1119 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1121 = frozenset([6])
    FOLLOW_DOT_in_dotValue1123 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1125 = frozenset([6])
    FOLLOW_DOT_in_dotValue1127 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1129 = frozenset([6])
    FOLLOW_DOT_in_dotValue1131 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1133 = frozenset([6])
    FOLLOW_DOT_in_dotValue1135 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1137 = frozenset([1])
    FOLLOW_VALUE_in_dotValue1144 = frozenset([6])
    FOLLOW_DOT_in_dotValue1146 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1148 = frozenset([6])
    FOLLOW_DOT_in_dotValue1150 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1152 = frozenset([6])
    FOLLOW_DOT_in_dotValue1154 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1156 = frozenset([6])
    FOLLOW_DOT_in_dotValue1158 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1160 = frozenset([6])
    FOLLOW_DOT_in_dotValue1162 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1164 = frozenset([6])
    FOLLOW_DOT_in_dotValue1166 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1168 = frozenset([6])
    FOLLOW_DOT_in_dotValue1170 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1172 = frozenset([6])
    FOLLOW_DOT_in_dotValue1174 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1176 = frozenset([6])
    FOLLOW_DOT_in_dotValue1178 = frozenset([19])
    FOLLOW_19_in_dotValue1180 = frozenset([1])
    FOLLOW_VALUE_in_dotValue1186 = frozenset([6])
    FOLLOW_DOT_in_dotValue1188 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1190 = frozenset([6])
    FOLLOW_DOT_in_dotValue1192 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1194 = frozenset([6])
    FOLLOW_DOT_in_dotValue1196 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1198 = frozenset([6])
    FOLLOW_DOT_in_dotValue1200 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1202 = frozenset([6])
    FOLLOW_DOT_in_dotValue1204 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1206 = frozenset([6])
    FOLLOW_DOT_in_dotValue1208 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1210 = frozenset([6])
    FOLLOW_DOT_in_dotValue1212 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1214 = frozenset([6])
    FOLLOW_DOT_in_dotValue1216 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1218 = frozenset([6])
    FOLLOW_DOT_in_dotValue1220 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1222 = frozenset([1])
    FOLLOW_VALUE_in_dotValue1229 = frozenset([6])
    FOLLOW_DOT_in_dotValue1231 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1233 = frozenset([6])
    FOLLOW_DOT_in_dotValue1235 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1237 = frozenset([6])
    FOLLOW_DOT_in_dotValue1239 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1241 = frozenset([6])
    FOLLOW_DOT_in_dotValue1243 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1245 = frozenset([6])
    FOLLOW_DOT_in_dotValue1247 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1249 = frozenset([6])
    FOLLOW_DOT_in_dotValue1251 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1253 = frozenset([6])
    FOLLOW_DOT_in_dotValue1255 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1257 = frozenset([6])
    FOLLOW_DOT_in_dotValue1259 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1261 = frozenset([6])
    FOLLOW_DOT_in_dotValue1263 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1265 = frozenset([6])
    FOLLOW_DOT_in_dotValue1267 = frozenset([19])
    FOLLOW_19_in_dotValue1269 = frozenset([1])
    FOLLOW_VALUE_in_dotValue1275 = frozenset([4])
    FOLLOW_SPACE_in_dotValue1277 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1279 = frozenset([1])
    FOLLOW_VALUE_in_dotValue1285 = frozenset([4])
    FOLLOW_SPACE_in_dotValue1287 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1289 = frozenset([4])
    FOLLOW_SPACE_in_dotValue1291 = frozenset([7])
    FOLLOW_VALUE_in_dotValue1293 = frozenset([1])
    FOLLOW_dotValue_in_valueList1302 = frozenset([1, 4, 5])
    FOLLOW_spaces_in_valueList1306 = frozenset([5])
    FOLLOW_COMMA_in_valueList1308 = frozenset([4, 7, 19])
    FOLLOW_spaces_in_valueList1310 = frozenset([7, 19])
    FOLLOW_dotValue_in_valueList1312 = frozenset([1, 4, 5])
    FOLLOW_EQ_in_compOpt1324 = frozenset([1])
    FOLLOW_LT_in_compOpt1331 = frozenset([1])
    FOLLOW_GT_in_compOpt1338 = frozenset([1])
    FOLLOW_NOT_in_compOpt1345 = frozenset([8])
    FOLLOW_EQ_in_compOpt1348 = frozenset([1])
    FOLLOW_EQ_in_compOpt1355 = frozenset([10])
    FOLLOW_GT_in_compOpt1358 = frozenset([1])
    FOLLOW_EQ_in_compOpt1365 = frozenset([9])
    FOLLOW_LT_in_compOpt1368 = frozenset([1])
    FOLLOW_LT_in_compOpt1375 = frozenset([8])
    FOLLOW_EQ_in_compOpt1378 = frozenset([1])
    FOLLOW_GT_in_compOpt1385 = frozenset([8])
    FOLLOW_EQ_in_compOpt1388 = frozenset([1])
    FOLLOW_LT_in_compOpt1395 = frozenset([10])
    FOLLOW_GT_in_compOpt1398 = frozenset([1])
    FOLLOW_dotValue_in_genValue1406 = frozenset([1])
    FOLLOW_dotValue_in_genValue1411 = frozenset([4, 8, 9, 10, 11])
    FOLLOW_compOpt_in_genValue1413 = frozenset([7, 19])
    FOLLOW_dotValue_in_genValue1415 = frozenset([1, 12])
    FOLLOW_AMP_in_genValue1418 = frozenset([7, 19])
    FOLLOW_dotValue_in_genValue1420 = frozenset([4, 8, 9, 10, 11])
    FOLLOW_compOpt_in_genValue1422 = frozenset([7, 19])
    FOLLOW_dotValue_in_genValue1424 = frozenset([1, 12])
    FOLLOW_dotValue_in_betValue1432 = frozenset([4, 88, 89])
    FOLLOW_spaces_in_betValue1434 = frozenset([88, 89])
    FOLLOW_andpython_in_betValue1436 = frozenset([4, 7, 19])
    FOLLOW_spaces_in_betValue1438 = frozenset([7, 19])
    FOLLOW_dotValue_in_betValue1440 = frozenset([1])
    FOLLOW_andpython_in_logicalOp1452 = frozenset([1])
    FOLLOW_orpython_in_logicalOp1454 = frozenset([1])
    FOLLOW_set_in_entity1463 = frozenset([1])
    FOLLOW_set_in_attr1554 = frozenset([1])
    FOLLOW_set_in_funct1707 = frozenset([1])
    FOLLOW_set_in_select1745 = frozenset([1])
    FOLLOW_set_in_andpython1765 = frozenset([1])
    FOLLOW_set_in_order1778 = frozenset([1])
    FOLLOW_set_in_by1791 = frozenset([1])
    FOLLOW_set_in_orpython1803 = frozenset([1])
    FOLLOW_set_in_inpython1815 = frozenset([1])
    FOLLOW_set_in_notpython1827 = frozenset([1])
    FOLLOW_99_in_like1841 = frozenset([1])
    FOLLOW_100_in_like1845 = frozenset([1])
    FOLLOW_97_in_like1849 = frozenset([4, 99])
    FOLLOW_spaces_in_like1851 = frozenset([99])
    FOLLOW_99_in_like1853 = frozenset([1])
    FOLLOW_98_in_like1857 = frozenset([4, 100])
    FOLLOW_spaces_in_like1859 = frozenset([100])
    FOLLOW_100_in_like1861 = frozenset([1])
    FOLLOW_set_in_count1870 = frozenset([1])
    FOLLOW_set_in_sum1883 = frozenset([1])
    FOLLOW_set_in_asc1896 = frozenset([1])
    FOLLOW_set_in_desc1909 = frozenset([1])
    FOLLOW_set_in_between1922 = frozenset([1])
    FOLLOW_15_in_lb1937 = frozenset([1])
    FOLLOW_16_in_rb1947 = frozenset([1])



def main(argv, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    from antlr3.main import ParserMain
    main = ParserMain("SqlDASLexer", SqlDASParser)
    main.stdin = stdin
    main.stdout = stdout
    main.stderr = stderr
    main.execute(argv)


if __name__ == '__main__':
    main(sys.argv)
