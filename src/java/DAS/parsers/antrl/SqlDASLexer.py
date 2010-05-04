# $ANTLR 3.1.2 SqlDAS.g 2009-03-05 10:47:44

import sys
from antlr3 import *
from antlr3.compat import set, frozenset


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
T__90=90
T__15=15
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


class SqlDASLexer(Lexer):

    grammarFileName = "SqlDAS.g"
    antlr_version = version_str_to_tuple("3.1.2")
    antlr_version_str = "3.1.2"

    def __init__(self, input=None, state=None):
        if state is None:
            state = RecognizerSharedState()
        Lexer.__init__(self, input, state)

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






    # $ANTLR start "T__15"
    def mT__15(self, ):

        try:
            _type = T__15
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:7:7: ( '(' )
            # SqlDAS.g:7:9: '('
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

            # SqlDAS.g:8:7: ( ')' )
            # SqlDAS.g:8:9: ')'
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

            # SqlDAS.g:9:7: ( 'WHERE' )
            # SqlDAS.g:9:9: 'WHERE'
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

            # SqlDAS.g:10:7: ( 'where' )
            # SqlDAS.g:10:9: 'where'
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

            # SqlDAS.g:11:7: ( 'in' )
            # SqlDAS.g:11:9: 'in'
            pass 
            self.match("in")



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

            # SqlDAS.g:12:7: ( 'ads' )
            # SqlDAS.g:12:9: 'ads'
            pass 
            self.match("ads")



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

            # SqlDAS.g:13:7: ( 'config' )
            # SqlDAS.g:13:9: 'config'
            pass 
            self.match("config")



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

            # SqlDAS.g:14:7: ( 'dataset' )
            # SqlDAS.g:14:9: 'dataset'
            pass 
            self.match("dataset")



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

            # SqlDAS.g:15:7: ( 'release' )
            # SqlDAS.g:15:9: 'release'
            pass 
            self.match("release")



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

            # SqlDAS.g:16:7: ( 'tier' )
            # SqlDAS.g:16:9: 'tier'
            pass 
            self.match("tier")



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

            # SqlDAS.g:17:7: ( 'site' )
            # SqlDAS.g:17:9: 'site'
            pass 
            self.match("site")



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

            # SqlDAS.g:18:7: ( 'block' )
            # SqlDAS.g:18:9: 'block'
            pass 
            self.match("block")



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

            # SqlDAS.g:19:7: ( 'file' )
            # SqlDAS.g:19:9: 'file'
            pass 
            self.match("file")



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

            # SqlDAS.g:20:7: ( 'primds' )
            # SqlDAS.g:20:9: 'primds'
            pass 
            self.match("primds")



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

            # SqlDAS.g:21:7: ( 'procds' )
            # SqlDAS.g:21:9: 'procds'
            pass 
            self.match("procds")



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

            # SqlDAS.g:22:7: ( 'run' )
            # SqlDAS.g:22:9: 'run'
            pass 
            self.match("run")



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

            # SqlDAS.g:23:7: ( 'lumi' )
            # SqlDAS.g:23:9: 'lumi'
            pass 
            self.match("lumi")



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

            # SqlDAS.g:24:7: ( 'dq' )
            # SqlDAS.g:24:9: 'dq'
            pass 
            self.match("dq")



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

            # SqlDAS.g:25:7: ( 'ilumi' )
            # SqlDAS.g:25:9: 'ilumi'
            pass 
            self.match("ilumi")



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

            # SqlDAS.g:26:7: ( 'phygrp' )
            # SqlDAS.g:26:9: 'phygrp'
            pass 
            self.match("phygrp")



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

            # SqlDAS.g:27:7: ( 'group' )
            # SqlDAS.g:27:9: 'group'
            pass 
            self.match("group")



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

            # SqlDAS.g:28:7: ( 'pset' )
            # SqlDAS.g:28:9: 'pset'
            pass 
            self.match("pset")



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

            # SqlDAS.g:29:7: ( 'algo' )
            # SqlDAS.g:29:9: 'algo'
            pass 
            self.match("algo")



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

            # SqlDAS.g:30:7: ( 'datatype' )
            # SqlDAS.g:30:9: 'datatype'
            pass 
            self.match("datatype")



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

            # SqlDAS.g:31:7: ( 'mcdesc' )
            # SqlDAS.g:31:9: 'mcdesc'
            pass 
            self.match("mcdesc")



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

            # SqlDAS.g:32:7: ( 'trigdesc' )
            # SqlDAS.g:32:9: 'trigdesc'
            pass 
            self.match("trigdesc")



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

            # SqlDAS.g:33:7: ( 'branch' )
            # SqlDAS.g:33:9: 'branch'
            pass 
            self.match("branch")



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

            # SqlDAS.g:34:7: ( 'createdate' )
            # SqlDAS.g:34:9: 'createdate'
            pass 
            self.match("createdate")



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

            # SqlDAS.g:35:7: ( 'moddate' )
            # SqlDAS.g:35:9: 'moddate'
            pass 
            self.match("moddate")



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

            # SqlDAS.g:36:7: ( 'starttime' )
            # SqlDAS.g:36:9: 'starttime'
            pass 
            self.match("starttime")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__44"



    # $ANTLR start "T__45"
    def mT__45(self, ):

        try:
            _type = T__45
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:37:7: ( 'endtime' )
            # SqlDAS.g:37:9: 'endtime'
            pass 
            self.match("endtime")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__45"



    # $ANTLR start "T__46"
    def mT__46(self, ):

        try:
            _type = T__46
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:38:7: ( 'createby' )
            # SqlDAS.g:38:9: 'createby'
            pass 
            self.match("createby")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__46"



    # $ANTLR start "T__47"
    def mT__47(self, ):

        try:
            _type = T__47
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:39:7: ( 'modby' )
            # SqlDAS.g:39:9: 'modby'
            pass 
            self.match("modby")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__47"



    # $ANTLR start "T__48"
    def mT__48(self, ):

        try:
            _type = T__48
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:40:7: ( 'name' )
            # SqlDAS.g:40:9: 'name'
            pass 
            self.match("name")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__48"



    # $ANTLR start "T__49"
    def mT__49(self, ):

        try:
            _type = T__49
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:41:7: ( 'version' )
            # SqlDAS.g:41:9: 'version'
            pass 
            self.match("version")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__49"



    # $ANTLR start "T__50"
    def mT__50(self, ):

        try:
            _type = T__50
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:42:7: ( 'number' )
            # SqlDAS.g:42:9: 'number'
            pass 
            self.match("number")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__50"



    # $ANTLR start "T__51"
    def mT__51(self, ):

        try:
            _type = T__51
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:43:7: ( 'startevnum' )
            # SqlDAS.g:43:9: 'startevnum'
            pass 
            self.match("startevnum")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__51"



    # $ANTLR start "T__52"
    def mT__52(self, ):

        try:
            _type = T__52
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:44:7: ( 'endevnum' )
            # SqlDAS.g:44:9: 'endevnum'
            pass 
            self.match("endevnum")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__52"



    # $ANTLR start "T__53"
    def mT__53(self, ):

        try:
            _type = T__53
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:45:7: ( 'numevents' )
            # SqlDAS.g:45:9: 'numevents'
            pass 
            self.match("numevents")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__53"



    # $ANTLR start "T__54"
    def mT__54(self, ):

        try:
            _type = T__54
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:46:7: ( 'numfiles' )
            # SqlDAS.g:46:9: 'numfiles'
            pass 
            self.match("numfiles")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__54"



    # $ANTLR start "T__55"
    def mT__55(self, ):

        try:
            _type = T__55
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:47:7: ( 'numlss' )
            # SqlDAS.g:47:9: 'numlss'
            pass 
            self.match("numlss")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__55"



    # $ANTLR start "T__56"
    def mT__56(self, ):

        try:
            _type = T__56
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:48:7: ( 'totlumi' )
            # SqlDAS.g:48:9: 'totlumi'
            pass 
            self.match("totlumi")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__56"



    # $ANTLR start "T__57"
    def mT__57(self, ):

        try:
            _type = T__57
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:49:7: ( 'store' )
            # SqlDAS.g:49:9: 'store'
            pass 
            self.match("store")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__57"



    # $ANTLR start "T__58"
    def mT__58(self, ):

        try:
            _type = T__58
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:50:7: ( 'size' )
            # SqlDAS.g:50:9: 'size'
            pass 
            self.match("size")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__58"



    # $ANTLR start "T__59"
    def mT__59(self, ):

        try:
            _type = T__59
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:51:7: ( 'count' )
            # SqlDAS.g:51:9: 'count'
            pass 
            self.match("count")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__59"



    # $ANTLR start "T__60"
    def mT__60(self, ):

        try:
            _type = T__60
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:52:7: ( 'status' )
            # SqlDAS.g:52:9: 'status'
            pass 
            self.match("status")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__60"



    # $ANTLR start "T__61"
    def mT__61(self, ):

        try:
            _type = T__61
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:53:7: ( 'type' )
            # SqlDAS.g:53:9: 'type'
            pass 
            self.match("type")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__61"



    # $ANTLR start "T__62"
    def mT__62(self, ):

        try:
            _type = T__62
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:54:7: ( 'id' )
            # SqlDAS.g:54:9: 'id'
            pass 
            self.match("id")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__62"



    # $ANTLR start "T__63"
    def mT__63(self, ):

        try:
            _type = T__63
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:55:7: ( 'parent' )
            # SqlDAS.g:55:9: 'parent'
            pass 
            self.match("parent")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__63"



    # $ANTLR start "T__64"
    def mT__64(self, ):

        try:
            _type = T__64
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:56:7: ( 'child' )
            # SqlDAS.g:56:9: 'child'
            pass 
            self.match("child")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__64"



    # $ANTLR start "T__65"
    def mT__65(self, ):

        try:
            _type = T__65
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:57:7: ( 'def' )
            # SqlDAS.g:57:9: 'def'
            pass 
            self.match("def")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__65"



    # $ANTLR start "T__66"
    def mT__66(self, ):

        try:
            _type = T__66
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:58:7: ( 'evnum' )
            # SqlDAS.g:58:9: 'evnum'
            pass 
            self.match("evnum")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__66"



    # $ANTLR start "T__67"
    def mT__67(self, ):

        try:
            _type = T__67
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:59:7: ( 'era' )
            # SqlDAS.g:59:9: 'era'
            pass 
            self.match("era")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__67"



    # $ANTLR start "T__68"
    def mT__68(self, ):

        try:
            _type = T__68
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:60:7: ( 'tag' )
            # SqlDAS.g:60:9: 'tag'
            pass 
            self.match("tag")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__68"



    # $ANTLR start "T__69"
    def mT__69(self, ):

        try:
            _type = T__69
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:61:7: ( 'xsection' )
            # SqlDAS.g:61:9: 'xsection'
            pass 
            self.match("xsection")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__69"



    # $ANTLR start "T__70"
    def mT__70(self, ):

        try:
            _type = T__70
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:62:7: ( 'hash' )
            # SqlDAS.g:62:9: 'hash'
            pass 
            self.match("hash")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__70"



    # $ANTLR start "T__71"
    def mT__71(self, ):

        try:
            _type = T__71
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:63:7: ( 'content' )
            # SqlDAS.g:63:9: 'content'
            pass 
            self.match("content")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__71"



    # $ANTLR start "T__72"
    def mT__72(self, ):

        try:
            _type = T__72
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:64:7: ( 'family' )
            # SqlDAS.g:64:9: 'family'
            pass 
            self.match("family")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__72"



    # $ANTLR start "T__73"
    def mT__73(self, ):

        try:
            _type = T__73
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:65:7: ( 'exe' )
            # SqlDAS.g:65:9: 'exe'
            pass 
            self.match("exe")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__73"



    # $ANTLR start "T__74"
    def mT__74(self, ):

        try:
            _type = T__74
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:66:7: ( 'annotation' )
            # SqlDAS.g:66:9: 'annotation'
            pass 
            self.match("annotation")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__74"



    # $ANTLR start "T__75"
    def mT__75(self, ):

        try:
            _type = T__75
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:67:7: ( 'checksum' )
            # SqlDAS.g:67:9: 'checksum'
            pass 
            self.match("checksum")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__75"



    # $ANTLR start "T__76"
    def mT__76(self, ):

        try:
            _type = T__76
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:68:7: ( 'numruns()' )
            # SqlDAS.g:68:9: 'numruns()'
            pass 
            self.match("numruns()")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__76"



    # $ANTLR start "T__77"
    def mT__77(self, ):

        try:
            _type = T__77
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:69:7: ( 'numfiles()' )
            # SqlDAS.g:69:9: 'numfiles()'
            pass 
            self.match("numfiles()")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__77"



    # $ANTLR start "T__78"
    def mT__78(self, ):

        try:
            _type = T__78
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:70:7: ( 'dataquality()' )
            # SqlDAS.g:70:9: 'dataquality()'
            pass 
            self.match("dataquality()")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__78"



    # $ANTLR start "T__79"
    def mT__79(self, ):

        try:
            _type = T__79
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:71:7: ( 'latest()' )
            # SqlDAS.g:71:9: 'latest()'
            pass 
            self.match("latest()")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__79"



    # $ANTLR start "T__80"
    def mT__80(self, ):

        try:
            _type = T__80
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:72:7: ( 'parentrelease()' )
            # SqlDAS.g:72:9: 'parentrelease()'
            pass 
            self.match("parentrelease()")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__80"



    # $ANTLR start "T__81"
    def mT__81(self, ):

        try:
            _type = T__81
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:73:7: ( 'childrelease()' )
            # SqlDAS.g:73:9: 'childrelease()'
            pass 
            self.match("childrelease()")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__81"



    # $ANTLR start "T__82"
    def mT__82(self, ):

        try:
            _type = T__82
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:74:7: ( 'intluminosity()' )
            # SqlDAS.g:74:9: 'intluminosity()'
            pass 
            self.match("intluminosity()")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__82"



    # $ANTLR start "T__83"
    def mT__83(self, ):

        try:
            _type = T__83
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:75:7: ( 'findevents()' )
            # SqlDAS.g:75:9: 'findevents()'
            pass 
            self.match("findevents()")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__83"



    # $ANTLR start "T__84"
    def mT__84(self, ):

        try:
            _type = T__84
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:76:7: ( 'select' )
            # SqlDAS.g:76:9: 'select'
            pass 
            self.match("select")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__84"



    # $ANTLR start "T__85"
    def mT__85(self, ):

        try:
            _type = T__85
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:77:7: ( 'SELECT' )
            # SqlDAS.g:77:9: 'SELECT'
            pass 
            self.match("SELECT")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__85"



    # $ANTLR start "T__86"
    def mT__86(self, ):

        try:
            _type = T__86
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:78:7: ( 'find' )
            # SqlDAS.g:78:9: 'find'
            pass 
            self.match("find")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__86"



    # $ANTLR start "T__87"
    def mT__87(self, ):

        try:
            _type = T__87
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:79:7: ( 'FIND' )
            # SqlDAS.g:79:9: 'FIND'
            pass 
            self.match("FIND")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__87"



    # $ANTLR start "T__88"
    def mT__88(self, ):

        try:
            _type = T__88
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:80:7: ( 'and' )
            # SqlDAS.g:80:9: 'and'
            pass 
            self.match("and")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__88"



    # $ANTLR start "T__89"
    def mT__89(self, ):

        try:
            _type = T__89
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:81:7: ( 'AND' )
            # SqlDAS.g:81:9: 'AND'
            pass 
            self.match("AND")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__89"



    # $ANTLR start "T__90"
    def mT__90(self, ):

        try:
            _type = T__90
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:82:7: ( 'order' )
            # SqlDAS.g:82:9: 'order'
            pass 
            self.match("order")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__90"



    # $ANTLR start "T__91"
    def mT__91(self, ):

        try:
            _type = T__91
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:83:7: ( 'ORDER' )
            # SqlDAS.g:83:9: 'ORDER'
            pass 
            self.match("ORDER")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__91"



    # $ANTLR start "T__92"
    def mT__92(self, ):

        try:
            _type = T__92
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:84:7: ( 'by' )
            # SqlDAS.g:84:9: 'by'
            pass 
            self.match("by")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__92"



    # $ANTLR start "T__93"
    def mT__93(self, ):

        try:
            _type = T__93
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:85:7: ( 'BY' )
            # SqlDAS.g:85:9: 'BY'
            pass 
            self.match("BY")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__93"



    # $ANTLR start "T__94"
    def mT__94(self, ):

        try:
            _type = T__94
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:86:7: ( 'or' )
            # SqlDAS.g:86:9: 'or'
            pass 
            self.match("or")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__94"



    # $ANTLR start "T__95"
    def mT__95(self, ):

        try:
            _type = T__95
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:87:7: ( 'OR' )
            # SqlDAS.g:87:9: 'OR'
            pass 
            self.match("OR")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__95"



    # $ANTLR start "T__96"
    def mT__96(self, ):

        try:
            _type = T__96
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:88:7: ( 'IN' )
            # SqlDAS.g:88:9: 'IN'
            pass 
            self.match("IN")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__96"



    # $ANTLR start "T__97"
    def mT__97(self, ):

        try:
            _type = T__97
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:89:7: ( 'not' )
            # SqlDAS.g:89:9: 'not'
            pass 
            self.match("not")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__97"



    # $ANTLR start "T__98"
    def mT__98(self, ):

        try:
            _type = T__98
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:90:7: ( 'NOT' )
            # SqlDAS.g:90:9: 'NOT'
            pass 
            self.match("NOT")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__98"



    # $ANTLR start "T__99"
    def mT__99(self, ):

        try:
            _type = T__99
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:91:7: ( 'like' )
            # SqlDAS.g:91:9: 'like'
            pass 
            self.match("like")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__99"



    # $ANTLR start "T__100"
    def mT__100(self, ):

        try:
            _type = T__100
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:92:8: ( 'LIKE' )
            # SqlDAS.g:92:10: 'LIKE'
            pass 
            self.match("LIKE")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__100"



    # $ANTLR start "T__101"
    def mT__101(self, ):

        try:
            _type = T__101
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:93:8: ( 'COUNT' )
            # SqlDAS.g:93:10: 'COUNT'
            pass 
            self.match("COUNT")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__101"



    # $ANTLR start "T__102"
    def mT__102(self, ):

        try:
            _type = T__102
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:94:8: ( 'sum' )
            # SqlDAS.g:94:10: 'sum'
            pass 
            self.match("sum")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__102"



    # $ANTLR start "T__103"
    def mT__103(self, ):

        try:
            _type = T__103
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:95:8: ( 'SUM' )
            # SqlDAS.g:95:10: 'SUM'
            pass 
            self.match("SUM")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__103"



    # $ANTLR start "T__104"
    def mT__104(self, ):

        try:
            _type = T__104
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:96:8: ( 'asc' )
            # SqlDAS.g:96:10: 'asc'
            pass 
            self.match("asc")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__104"



    # $ANTLR start "T__105"
    def mT__105(self, ):

        try:
            _type = T__105
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:97:8: ( 'ASC' )
            # SqlDAS.g:97:10: 'ASC'
            pass 
            self.match("ASC")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__105"



    # $ANTLR start "T__106"
    def mT__106(self, ):

        try:
            _type = T__106
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:98:8: ( 'desc' )
            # SqlDAS.g:98:10: 'desc'
            pass 
            self.match("desc")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__106"



    # $ANTLR start "T__107"
    def mT__107(self, ):

        try:
            _type = T__107
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:99:8: ( 'DESC' )
            # SqlDAS.g:99:10: 'DESC'
            pass 
            self.match("DESC")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__107"



    # $ANTLR start "T__108"
    def mT__108(self, ):

        try:
            _type = T__108
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:100:8: ( 'between' )
            # SqlDAS.g:100:10: 'between'
            pass 
            self.match("between")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__108"



    # $ANTLR start "T__109"
    def mT__109(self, ):

        try:
            _type = T__109
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:101:8: ( 'BETWEEN' )
            # SqlDAS.g:101:10: 'BETWEEN'
            pass 
            self.match("BETWEEN")



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "T__109"



    # $ANTLR start "VALUE"
    def mVALUE(self, ):

        try:
            _type = VALUE
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:170:8: ( ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '/' | '-' | '_' | ':' | '#' | '*' | '%' )+ )
            # SqlDAS.g:170:9: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '/' | '-' | '_' | ':' | '#' | '*' | '%' )+
            pass 
            # SqlDAS.g:170:9: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '/' | '-' | '_' | ':' | '#' | '*' | '%' )+
            cnt1 = 0
            while True: #loop1
                alt1 = 2
                LA1_0 = self.input.LA(1)

                if (LA1_0 == 35 or LA1_0 == 37 or LA1_0 == 42 or LA1_0 == 45 or (47 <= LA1_0 <= 58) or (65 <= LA1_0 <= 90) or LA1_0 == 95 or (97 <= LA1_0 <= 122)) :
                    alt1 = 1


                if alt1 == 1:
                    # SqlDAS.g:
                    pass 
                    if self.input.LA(1) == 35 or self.input.LA(1) == 37 or self.input.LA(1) == 42 or self.input.LA(1) == 45 or (47 <= self.input.LA(1) <= 58) or (65 <= self.input.LA(1) <= 90) or self.input.LA(1) == 95 or (97 <= self.input.LA(1) <= 122):
                        self.input.consume()
                    else:
                        mse = MismatchedSetException(None, self.input)
                        self.recover(mse)
                        raise mse



                else:
                    if cnt1 >= 1:
                        break #loop1

                    eee = EarlyExitException(1, self.input)
                    raise eee

                cnt1 += 1





            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "VALUE"



    # $ANTLR start "COMMA"
    def mCOMMA(self, ):

        try:
            _type = COMMA
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:176:8: ( ( ',' ) )
            # SqlDAS.g:176:9: ( ',' )
            pass 
            # SqlDAS.g:176:9: ( ',' )
            # SqlDAS.g:176:10: ','
            pass 
            self.match(44)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "COMMA"



    # $ANTLR start "SPACE"
    def mSPACE(self, ):

        try:
            _type = SPACE
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:177:8: ( ( ' ' ) )
            # SqlDAS.g:177:9: ( ' ' )
            pass 
            # SqlDAS.g:177:9: ( ' ' )
            # SqlDAS.g:177:10: ' '
            pass 
            self.match(32)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "SPACE"



    # $ANTLR start "DOT"
    def mDOT(self, ):

        try:
            _type = DOT
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:178:6: ( ( '.' ) )
            # SqlDAS.g:178:7: ( '.' )
            pass 
            # SqlDAS.g:178:7: ( '.' )
            # SqlDAS.g:178:8: '.'
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

            # SqlDAS.g:180:5: ( ( '>' ) )
            # SqlDAS.g:180:6: ( '>' )
            pass 
            # SqlDAS.g:180:6: ( '>' )
            # SqlDAS.g:180:7: '>'
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

            # SqlDAS.g:181:5: ( ( '<' ) )
            # SqlDAS.g:181:6: ( '<' )
            pass 
            # SqlDAS.g:181:6: ( '<' )
            # SqlDAS.g:181:7: '<'
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

            # SqlDAS.g:182:5: ( ( '=' ) )
            # SqlDAS.g:182:6: ( '=' )
            pass 
            # SqlDAS.g:182:6: ( '=' )
            # SqlDAS.g:182:7: '='
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

            # SqlDAS.g:183:6: ( ( '!' ) )
            # SqlDAS.g:183:7: ( '!' )
            pass 
            # SqlDAS.g:183:7: ( '!' )
            # SqlDAS.g:183:8: '!'
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

            # SqlDAS.g:184:6: ( ( '&' ) )
            # SqlDAS.g:184:7: ( '&' )
            pass 
            # SqlDAS.g:184:7: ( '&' )
            # SqlDAS.g:184:8: '&'
            pass 
            self.match(38)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "AMP"



    # $ANTLR start "NL"
    def mNL(self, ):

        try:
            _type = NL
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:186:5: ( ( '\\n' ) )
            # SqlDAS.g:186:6: ( '\\n' )
            pass 
            # SqlDAS.g:186:6: ( '\\n' )
            # SqlDAS.g:186:7: '\\n'
            pass 
            self.match(10)






            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "NL"



    # $ANTLR start "WS"
    def mWS(self, ):

        try:
            _type = WS
            _channel = DEFAULT_CHANNEL

            # SqlDAS.g:187:6: ( ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+ )
            # SqlDAS.g:187:8: ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+
            pass 
            # SqlDAS.g:187:8: ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+
            cnt2 = 0
            while True: #loop2
                alt2 = 2
                LA2_0 = self.input.LA(1)

                if ((9 <= LA2_0 <= 10) or (12 <= LA2_0 <= 13) or LA2_0 == 32) :
                    alt2 = 1


                if alt2 == 1:
                    # SqlDAS.g:
                    pass 
                    if (9 <= self.input.LA(1) <= 10) or (12 <= self.input.LA(1) <= 13) or self.input.LA(1) == 32:
                        self.input.consume()
                    else:
                        mse = MismatchedSetException(None, self.input)
                        self.recover(mse)
                        raise mse



                else:
                    if cnt2 >= 1:
                        break #loop2

                    eee = EarlyExitException(2, self.input)
                    raise eee

                cnt2 += 1


            #action start
            _channel = HIDDEN; 
            #action end



            self._state.type = _type
            self._state.channel = _channel

        finally:

            pass

    # $ANTLR end "WS"



    def mTokens(self):
        # SqlDAS.g:1:8: ( T__15 | T__16 | T__17 | T__18 | T__19 | T__20 | T__21 | T__22 | T__23 | T__24 | T__25 | T__26 | T__27 | T__28 | T__29 | T__30 | T__31 | T__32 | T__33 | T__34 | T__35 | T__36 | T__37 | T__38 | T__39 | T__40 | T__41 | T__42 | T__43 | T__44 | T__45 | T__46 | T__47 | T__48 | T__49 | T__50 | T__51 | T__52 | T__53 | T__54 | T__55 | T__56 | T__57 | T__58 | T__59 | T__60 | T__61 | T__62 | T__63 | T__64 | T__65 | T__66 | T__67 | T__68 | T__69 | T__70 | T__71 | T__72 | T__73 | T__74 | T__75 | T__76 | T__77 | T__78 | T__79 | T__80 | T__81 | T__82 | T__83 | T__84 | T__85 | T__86 | T__87 | T__88 | T__89 | T__90 | T__91 | T__92 | T__93 | T__94 | T__95 | T__96 | T__97 | T__98 | T__99 | T__100 | T__101 | T__102 | T__103 | T__104 | T__105 | T__106 | T__107 | T__108 | T__109 | VALUE | COMMA | SPACE | DOT | GT | LT | EQ | NOT | AMP | NL | WS )
        alt3 = 106
        alt3 = self.dfa3.predict(self.input)
        if alt3 == 1:
            # SqlDAS.g:1:10: T__15
            pass 
            self.mT__15()


        elif alt3 == 2:
            # SqlDAS.g:1:16: T__16
            pass 
            self.mT__16()


        elif alt3 == 3:
            # SqlDAS.g:1:22: T__17
            pass 
            self.mT__17()


        elif alt3 == 4:
            # SqlDAS.g:1:28: T__18
            pass 
            self.mT__18()


        elif alt3 == 5:
            # SqlDAS.g:1:34: T__19
            pass 
            self.mT__19()


        elif alt3 == 6:
            # SqlDAS.g:1:40: T__20
            pass 
            self.mT__20()


        elif alt3 == 7:
            # SqlDAS.g:1:46: T__21
            pass 
            self.mT__21()


        elif alt3 == 8:
            # SqlDAS.g:1:52: T__22
            pass 
            self.mT__22()


        elif alt3 == 9:
            # SqlDAS.g:1:58: T__23
            pass 
            self.mT__23()


        elif alt3 == 10:
            # SqlDAS.g:1:64: T__24
            pass 
            self.mT__24()


        elif alt3 == 11:
            # SqlDAS.g:1:70: T__25
            pass 
            self.mT__25()


        elif alt3 == 12:
            # SqlDAS.g:1:76: T__26
            pass 
            self.mT__26()


        elif alt3 == 13:
            # SqlDAS.g:1:82: T__27
            pass 
            self.mT__27()


        elif alt3 == 14:
            # SqlDAS.g:1:88: T__28
            pass 
            self.mT__28()


        elif alt3 == 15:
            # SqlDAS.g:1:94: T__29
            pass 
            self.mT__29()


        elif alt3 == 16:
            # SqlDAS.g:1:100: T__30
            pass 
            self.mT__30()


        elif alt3 == 17:
            # SqlDAS.g:1:106: T__31
            pass 
            self.mT__31()


        elif alt3 == 18:
            # SqlDAS.g:1:112: T__32
            pass 
            self.mT__32()


        elif alt3 == 19:
            # SqlDAS.g:1:118: T__33
            pass 
            self.mT__33()


        elif alt3 == 20:
            # SqlDAS.g:1:124: T__34
            pass 
            self.mT__34()


        elif alt3 == 21:
            # SqlDAS.g:1:130: T__35
            pass 
            self.mT__35()


        elif alt3 == 22:
            # SqlDAS.g:1:136: T__36
            pass 
            self.mT__36()


        elif alt3 == 23:
            # SqlDAS.g:1:142: T__37
            pass 
            self.mT__37()


        elif alt3 == 24:
            # SqlDAS.g:1:148: T__38
            pass 
            self.mT__38()


        elif alt3 == 25:
            # SqlDAS.g:1:154: T__39
            pass 
            self.mT__39()


        elif alt3 == 26:
            # SqlDAS.g:1:160: T__40
            pass 
            self.mT__40()


        elif alt3 == 27:
            # SqlDAS.g:1:166: T__41
            pass 
            self.mT__41()


        elif alt3 == 28:
            # SqlDAS.g:1:172: T__42
            pass 
            self.mT__42()


        elif alt3 == 29:
            # SqlDAS.g:1:178: T__43
            pass 
            self.mT__43()


        elif alt3 == 30:
            # SqlDAS.g:1:184: T__44
            pass 
            self.mT__44()


        elif alt3 == 31:
            # SqlDAS.g:1:190: T__45
            pass 
            self.mT__45()


        elif alt3 == 32:
            # SqlDAS.g:1:196: T__46
            pass 
            self.mT__46()


        elif alt3 == 33:
            # SqlDAS.g:1:202: T__47
            pass 
            self.mT__47()


        elif alt3 == 34:
            # SqlDAS.g:1:208: T__48
            pass 
            self.mT__48()


        elif alt3 == 35:
            # SqlDAS.g:1:214: T__49
            pass 
            self.mT__49()


        elif alt3 == 36:
            # SqlDAS.g:1:220: T__50
            pass 
            self.mT__50()


        elif alt3 == 37:
            # SqlDAS.g:1:226: T__51
            pass 
            self.mT__51()


        elif alt3 == 38:
            # SqlDAS.g:1:232: T__52
            pass 
            self.mT__52()


        elif alt3 == 39:
            # SqlDAS.g:1:238: T__53
            pass 
            self.mT__53()


        elif alt3 == 40:
            # SqlDAS.g:1:244: T__54
            pass 
            self.mT__54()


        elif alt3 == 41:
            # SqlDAS.g:1:250: T__55
            pass 
            self.mT__55()


        elif alt3 == 42:
            # SqlDAS.g:1:256: T__56
            pass 
            self.mT__56()


        elif alt3 == 43:
            # SqlDAS.g:1:262: T__57
            pass 
            self.mT__57()


        elif alt3 == 44:
            # SqlDAS.g:1:268: T__58
            pass 
            self.mT__58()


        elif alt3 == 45:
            # SqlDAS.g:1:274: T__59
            pass 
            self.mT__59()


        elif alt3 == 46:
            # SqlDAS.g:1:280: T__60
            pass 
            self.mT__60()


        elif alt3 == 47:
            # SqlDAS.g:1:286: T__61
            pass 
            self.mT__61()


        elif alt3 == 48:
            # SqlDAS.g:1:292: T__62
            pass 
            self.mT__62()


        elif alt3 == 49:
            # SqlDAS.g:1:298: T__63
            pass 
            self.mT__63()


        elif alt3 == 50:
            # SqlDAS.g:1:304: T__64
            pass 
            self.mT__64()


        elif alt3 == 51:
            # SqlDAS.g:1:310: T__65
            pass 
            self.mT__65()


        elif alt3 == 52:
            # SqlDAS.g:1:316: T__66
            pass 
            self.mT__66()


        elif alt3 == 53:
            # SqlDAS.g:1:322: T__67
            pass 
            self.mT__67()


        elif alt3 == 54:
            # SqlDAS.g:1:328: T__68
            pass 
            self.mT__68()


        elif alt3 == 55:
            # SqlDAS.g:1:334: T__69
            pass 
            self.mT__69()


        elif alt3 == 56:
            # SqlDAS.g:1:340: T__70
            pass 
            self.mT__70()


        elif alt3 == 57:
            # SqlDAS.g:1:346: T__71
            pass 
            self.mT__71()


        elif alt3 == 58:
            # SqlDAS.g:1:352: T__72
            pass 
            self.mT__72()


        elif alt3 == 59:
            # SqlDAS.g:1:358: T__73
            pass 
            self.mT__73()


        elif alt3 == 60:
            # SqlDAS.g:1:364: T__74
            pass 
            self.mT__74()


        elif alt3 == 61:
            # SqlDAS.g:1:370: T__75
            pass 
            self.mT__75()


        elif alt3 == 62:
            # SqlDAS.g:1:376: T__76
            pass 
            self.mT__76()


        elif alt3 == 63:
            # SqlDAS.g:1:382: T__77
            pass 
            self.mT__77()


        elif alt3 == 64:
            # SqlDAS.g:1:388: T__78
            pass 
            self.mT__78()


        elif alt3 == 65:
            # SqlDAS.g:1:394: T__79
            pass 
            self.mT__79()


        elif alt3 == 66:
            # SqlDAS.g:1:400: T__80
            pass 
            self.mT__80()


        elif alt3 == 67:
            # SqlDAS.g:1:406: T__81
            pass 
            self.mT__81()


        elif alt3 == 68:
            # SqlDAS.g:1:412: T__82
            pass 
            self.mT__82()


        elif alt3 == 69:
            # SqlDAS.g:1:418: T__83
            pass 
            self.mT__83()


        elif alt3 == 70:
            # SqlDAS.g:1:424: T__84
            pass 
            self.mT__84()


        elif alt3 == 71:
            # SqlDAS.g:1:430: T__85
            pass 
            self.mT__85()


        elif alt3 == 72:
            # SqlDAS.g:1:436: T__86
            pass 
            self.mT__86()


        elif alt3 == 73:
            # SqlDAS.g:1:442: T__87
            pass 
            self.mT__87()


        elif alt3 == 74:
            # SqlDAS.g:1:448: T__88
            pass 
            self.mT__88()


        elif alt3 == 75:
            # SqlDAS.g:1:454: T__89
            pass 
            self.mT__89()


        elif alt3 == 76:
            # SqlDAS.g:1:460: T__90
            pass 
            self.mT__90()


        elif alt3 == 77:
            # SqlDAS.g:1:466: T__91
            pass 
            self.mT__91()


        elif alt3 == 78:
            # SqlDAS.g:1:472: T__92
            pass 
            self.mT__92()


        elif alt3 == 79:
            # SqlDAS.g:1:478: T__93
            pass 
            self.mT__93()


        elif alt3 == 80:
            # SqlDAS.g:1:484: T__94
            pass 
            self.mT__94()


        elif alt3 == 81:
            # SqlDAS.g:1:490: T__95
            pass 
            self.mT__95()


        elif alt3 == 82:
            # SqlDAS.g:1:496: T__96
            pass 
            self.mT__96()


        elif alt3 == 83:
            # SqlDAS.g:1:502: T__97
            pass 
            self.mT__97()


        elif alt3 == 84:
            # SqlDAS.g:1:508: T__98
            pass 
            self.mT__98()


        elif alt3 == 85:
            # SqlDAS.g:1:514: T__99
            pass 
            self.mT__99()


        elif alt3 == 86:
            # SqlDAS.g:1:520: T__100
            pass 
            self.mT__100()


        elif alt3 == 87:
            # SqlDAS.g:1:527: T__101
            pass 
            self.mT__101()


        elif alt3 == 88:
            # SqlDAS.g:1:534: T__102
            pass 
            self.mT__102()


        elif alt3 == 89:
            # SqlDAS.g:1:541: T__103
            pass 
            self.mT__103()


        elif alt3 == 90:
            # SqlDAS.g:1:548: T__104
            pass 
            self.mT__104()


        elif alt3 == 91:
            # SqlDAS.g:1:555: T__105
            pass 
            self.mT__105()


        elif alt3 == 92:
            # SqlDAS.g:1:562: T__106
            pass 
            self.mT__106()


        elif alt3 == 93:
            # SqlDAS.g:1:569: T__107
            pass 
            self.mT__107()


        elif alt3 == 94:
            # SqlDAS.g:1:576: T__108
            pass 
            self.mT__108()


        elif alt3 == 95:
            # SqlDAS.g:1:583: T__109
            pass 
            self.mT__109()


        elif alt3 == 96:
            # SqlDAS.g:1:590: VALUE
            pass 
            self.mVALUE()


        elif alt3 == 97:
            # SqlDAS.g:1:596: COMMA
            pass 
            self.mCOMMA()


        elif alt3 == 98:
            # SqlDAS.g:1:602: SPACE
            pass 
            self.mSPACE()


        elif alt3 == 99:
            # SqlDAS.g:1:608: DOT
            pass 
            self.mDOT()


        elif alt3 == 100:
            # SqlDAS.g:1:612: GT
            pass 
            self.mGT()


        elif alt3 == 101:
            # SqlDAS.g:1:615: LT
            pass 
            self.mLT()


        elif alt3 == 102:
            # SqlDAS.g:1:618: EQ
            pass 
            self.mEQ()


        elif alt3 == 103:
            # SqlDAS.g:1:621: NOT
            pass 
            self.mNOT()


        elif alt3 == 104:
            # SqlDAS.g:1:625: AMP
            pass 
            self.mAMP()


        elif alt3 == 105:
            # SqlDAS.g:1:629: NL
            pass 
            self.mNL()


        elif alt3 == 106:
            # SqlDAS.g:1:632: WS
            pass 
            self.mWS()







    # lookup tables for DFA #3

    DFA3_eot = DFA.unpack(
        u"\3\uffff\37\42\2\uffff\1\157\6\uffff\1\160\1\uffff\2\42\1\164\1"
        u"\42\1\166\10\42\1\u0082\16\42\1\u0094\34\42\1\u00b4\1\u00b6\1\u00b7"
        u"\1\42\1\u00b9\4\42\2\uffff\3\42\1\uffff\1\42\1\uffff\1\u00c2\2"
        u"\42\1\u00c5\1\u00c6\6\42\1\uffff\1\u00ce\2\42\1\u00d1\4\42\1\u00d6"
        u"\5\42\1\u00dd\2\42\1\uffff\21\42\1\u00f3\1\u00f4\2\42\1\u00fb\4"
        u"\42\1\u0100\1\42\1\u0102\1\u0103\1\42\1\uffff\1\42\2\uffff\1\42"
        u"\1\uffff\1\u0107\7\42\1\uffff\1\u010f\1\42\2\uffff\7\42\1\uffff"
        u"\1\u011a\1\42\1\uffff\1\u011c\2\42\1\u011f\1\uffff\1\u0120\1\u0121"
        u"\4\42\1\uffff\3\42\1\u0129\1\u012b\4\42\1\u0130\1\42\1\u0132\1"
        u"\42\1\u0134\7\42\2\uffff\1\u013c\5\42\1\uffff\2\42\1\u0144\1\42"
        u"\1\uffff\1\u0146\2\uffff\3\42\1\uffff\1\u014a\1\42\1\u014c\1\u014d"
        u"\1\u014e\1\42\1\u0150\1\uffff\3\42\1\u0154\1\42\1\u0157\4\42\1"
        u"\uffff\1\42\1\uffff\2\42\3\uffff\2\42\1\u0162\1\42\1\u0164\2\42"
        u"\1\uffff\1\42\1\uffff\4\42\1\uffff\1\42\1\uffff\1\42\1\uffff\1"
        u"\u016e\2\42\1\u0171\2\42\1\u0174\1\uffff\7\42\1\uffff\1\42\1\uffff"
        u"\1\u017d\1\u017e\1\42\1\uffff\1\u0180\3\uffff\1\42\1\uffff\1\42"
        u"\1\u0183\1\42\1\uffff\2\42\1\uffff\11\42\1\u0191\1\uffff\1\u0192"
        u"\1\uffff\1\u0193\2\42\1\u0196\1\u0197\1\u0198\1\u0199\1\u019b\1"
        u"\42\1\uffff\1\u019d\1\42\1\uffff\2\42\1\uffff\1\u01a1\2\42\1\u01a4"
        u"\3\42\1\u01a8\2\uffff\1\42\1\uffff\2\42\1\uffff\1\u01ac\4\42\1"
        u"\u01b1\2\42\1\u01b4\1\42\1\u01b6\2\42\3\uffff\1\u01b9\1\42\4\uffff"
        u"\1\42\3\uffff\1\u01bc\1\u01bd\1\42\1\uffff\2\42\1\uffff\1\42\1"
        u"\u01c2\1\42\1\uffff\1\u01c4\2\42\1\uffff\1\42\1\u01c8\1\42\1\u01ca"
        u"\1\uffff\1\u01cb\1\42\1\uffff\1\u01cd\1\uffff\2\42\1\uffff\2\42"
        u"\2\uffff\1\u01d2\1\42\1\u01d5\2\uffff\1\u01d6\1\uffff\3\42\1\uffff"
        u"\1\42\2\uffff\1\42\1\uffff\1\u01dc\3\42\1\uffff\1\u01e0\3\uffff"
        u"\1\42\1\u01e2\1\u01e3\2\42\1\uffff\1\u01e6\2\42\1\uffff\1\42\2"
        u"\uffff\2\42\2\uffff\3\42\1\uffff\2\42\1\uffff\1\42\2\uffff"
        )

    DFA3_eof = DFA.unpack(
        u"\u01f2\uffff"
        )

    DFA3_min = DFA.unpack(
        u"\1\11\2\uffff\1\110\1\150\2\144\1\150\1\141\1\145\1\141\2\145\3"
        u"\141\1\162\1\143\1\156\1\141\1\145\1\163\1\141\1\105\1\111\1\116"
        u"\1\162\1\122\1\105\1\116\1\117\1\111\1\117\1\105\2\uffff\1\11\6"
        u"\uffff\1\11\1\uffff\1\105\1\145\1\43\1\165\1\43\1\163\1\147\1\144"
        u"\1\143\1\156\2\145\1\164\1\43\1\146\1\154\1\156\1\145\1\151\1\164"
        u"\1\160\1\147\1\164\1\141\1\154\1\155\1\157\1\141\1\43\1\164\1\154"
        u"\1\155\1\151\1\171\1\145\1\162\1\155\1\164\1\153\1\157\3\144\1"
        u"\156\1\141\1\145\2\155\1\164\1\162\1\145\1\163\1\114\1\115\1\116"
        u"\1\104\1\103\3\43\1\124\1\43\1\124\1\113\1\125\1\123\2\uffff\1"
        u"\122\1\162\1\154\1\uffff\1\155\1\uffff\1\43\2\157\2\43\1\146\1"
        u"\156\1\141\1\154\1\143\1\141\1\uffff\1\43\1\143\1\145\1\43\1\162"
        u"\1\147\1\154\1\145\1\43\2\145\2\162\1\145\1\43\1\143\1\156\1\uffff"
        u"\1\167\1\145\1\144\1\151\1\155\1\143\1\147\1\164\1\145\1\151\2"
        u"\145\1\165\1\145\1\142\1\145\1\165\2\43\1\145\1\142\1\43\1\163"
        u"\1\143\1\150\1\105\1\43\1\104\2\43\1\145\1\uffff\1\105\2\uffff"
        u"\1\127\1\uffff\1\43\1\105\1\116\1\103\1\105\1\145\1\165\1\151\1"
        u"\uffff\1\43\1\164\2\uffff\1\151\1\145\2\164\1\144\1\153\1\161\1"
        u"\uffff\1\43\1\141\1\uffff\1\43\1\144\1\165\1\43\1\uffff\2\43\1"
        u"\164\1\165\1\145\1\143\1\uffff\1\153\1\143\1\145\2\43\1\154\2\144"
        u"\1\162\1\43\1\156\1\43\1\163\1\43\1\160\1\163\1\141\1\171\1\151"
        u"\1\166\1\155\2\uffff\1\43\1\145\1\166\1\151\1\163\1\165\1\uffff"
        u"\1\151\1\164\1\43\1\103\1\uffff\1\43\2\uffff\1\162\1\122\1\105"
        u"\1\uffff\1\43\1\124\3\43\1\155\1\43\1\uffff\1\141\1\147\1\156\1"
        u"\43\1\145\1\43\1\163\1\145\1\171\1\165\1\uffff\1\163\1\uffff\1"
        u"\145\1\155\3\uffff\1\145\1\163\1\43\1\164\1\43\1\150\1\145\1\uffff"
        u"\1\166\1\uffff\1\171\2\163\1\160\1\uffff\1\164\1\uffff\1\164\1"
        u"\uffff\1\43\1\143\1\164\1\43\1\155\1\156\1\43\1\uffff\1\162\1\145"
        u"\1\154\1\163\1\156\1\157\1\151\1\uffff\1\124\1\uffff\2\43\1\105"
        u"\1\uffff\1\43\3\uffff\1\151\1\uffff\1\164\1\43\1\164\1\uffff\1"
        u"\142\1\145\1\uffff\1\165\1\164\1\160\1\141\1\145\1\163\2\151\1"
        u"\166\1\43\1\uffff\1\43\1\uffff\1\43\1\156\1\145\5\43\1\50\1\uffff"
        u"\1\43\1\145\1\uffff\1\145\1\165\1\uffff\1\43\1\156\1\145\1\43\1"
        u"\163\1\156\1\157\1\43\2\uffff\1\116\1\uffff\1\156\1\151\1\uffff"
        u"\1\43\1\141\1\171\1\154\1\155\1\43\1\145\1\154\1\43\1\143\1\43"
        u"\1\155\1\156\3\uffff\1\43\1\156\4\uffff\1\145\3\uffff\2\43\1\155"
        u"\1\uffff\1\164\1\163\1\uffff\1\50\1\43\1\156\1\uffff\1\43\2\157"
        u"\1\uffff\1\164\1\43\1\145\1\43\1\uffff\1\43\1\151\1\uffff\1\43"
        u"\1\uffff\1\145\1\165\1\uffff\1\164\1\154\2\uffff\1\43\1\163\1\43"
        u"\2\uffff\1\43\1\uffff\1\163\1\156\1\145\1\uffff\1\141\2\uffff\1"
        u"\164\1\uffff\1\43\1\155\1\163\1\145\1\uffff\1\43\3\uffff\1\151"
        u"\2\43\1\163\1\171\1\uffff\1\43\1\50\1\141\1\uffff\1\164\2\uffff"
        u"\1\145\1\50\2\uffff\1\163\1\171\1\50\1\uffff\1\145\1\50\1\uffff"
        u"\1\50\2\uffff"
        )

    DFA3_max = DFA.unpack(
        u"\1\172\2\uffff\1\110\1\150\1\156\1\163\1\162\1\161\1\165\1\171"
        u"\1\165\1\171\1\151\1\163\1\165\1\162\1\157\1\170\1\165\1\145\1"
        u"\163\1\141\1\125\1\111\1\123\1\162\1\122\1\131\1\116\1\117\1\111"
        u"\1\117\1\105\2\uffff\1\40\6\uffff\1\40\1\uffff\1\105\1\145\1\172"
        u"\1\165\1\172\1\163\1\147\1\156\1\143\1\165\1\145\1\151\1\164\1"
        u"\172\1\163\1\154\1\156\1\145\1\151\1\164\1\160\1\147\1\172\1\157"
        u"\1\154\1\155\1\157\1\141\1\172\1\164\1\156\1\155\1\157\1\171\1"
        u"\145\1\162\1\155\1\164\1\153\1\157\3\144\1\156\1\141\1\145\2\155"
        u"\1\164\1\162\1\145\1\163\1\114\1\115\1\116\1\104\1\103\3\172\1"
        u"\124\1\172\1\124\1\113\1\125\1\123\2\uffff\1\122\1\162\1\154\1"
        u"\uffff\1\155\1\uffff\1\172\2\157\2\172\1\164\1\156\1\141\1\154"
        u"\1\143\1\141\1\uffff\1\172\1\143\1\145\1\172\1\162\1\147\1\154"
        u"\1\145\1\172\2\145\1\164\1\162\1\145\1\172\1\143\1\156\1\uffff"
        u"\1\167\1\145\1\144\1\151\1\155\1\143\1\147\1\164\1\145\1\151\2"
        u"\145\1\165\1\145\1\144\1\164\1\165\2\172\1\145\1\162\1\172\1\163"
        u"\1\143\1\150\1\105\1\172\1\104\2\172\1\145\1\uffff\1\105\2\uffff"
        u"\1\127\1\uffff\1\172\1\105\1\116\1\103\1\105\1\145\1\165\1\151"
        u"\1\uffff\1\172\1\164\2\uffff\1\151\1\145\2\164\1\144\1\153\1\164"
        u"\1\uffff\1\172\1\141\1\uffff\1\172\1\144\1\165\1\172\1\uffff\2"
        u"\172\1\164\1\165\1\145\1\143\1\uffff\1\153\1\143\1\145\2\172\1"
        u"\154\2\144\1\162\1\172\1\156\1\172\1\163\1\172\1\160\1\163\1\141"
        u"\1\171\1\151\1\166\1\155\2\uffff\1\172\1\145\1\166\1\151\1\163"
        u"\1\165\1\uffff\1\151\1\164\1\172\1\103\1\uffff\1\172\2\uffff\1"
        u"\162\1\122\1\105\1\uffff\1\172\1\124\3\172\1\155\1\172\1\uffff"
        u"\1\141\1\147\1\156\1\172\1\145\1\172\1\163\1\145\1\171\1\165\1"
        u"\uffff\1\163\1\uffff\1\145\1\155\3\uffff\1\164\1\163\1\172\1\164"
        u"\1\172\1\150\1\145\1\uffff\1\166\1\uffff\1\171\2\163\1\160\1\uffff"
        u"\1\164\1\uffff\1\164\1\uffff\1\172\1\143\1\164\1\172\1\155\1\156"
        u"\1\172\1\uffff\1\162\1\145\1\154\1\163\1\156\1\157\1\151\1\uffff"
        u"\1\124\1\uffff\2\172\1\105\1\uffff\1\172\3\uffff\1\151\1\uffff"
        u"\1\164\1\172\1\164\1\uffff\1\144\1\145\1\uffff\1\165\1\164\1\160"
        u"\1\141\1\145\1\163\2\151\1\166\1\172\1\uffff\1\172\1\uffff\1\172"
        u"\1\156\1\145\5\172\1\50\1\uffff\1\172\1\145\1\uffff\1\145\1\165"
        u"\1\uffff\1\172\1\156\1\145\1\172\1\163\1\156\1\157\1\172\2\uffff"
        u"\1\116\1\uffff\1\156\1\151\1\uffff\1\172\1\141\1\171\1\154\1\155"
        u"\1\172\1\145\1\154\1\172\1\143\1\172\1\155\1\156\3\uffff\1\172"
        u"\1\156\4\uffff\1\145\3\uffff\2\172\1\155\1\uffff\1\164\1\163\1"
        u"\uffff\1\50\1\172\1\156\1\uffff\1\172\2\157\1\uffff\1\164\1\172"
        u"\1\145\1\172\1\uffff\1\172\1\151\1\uffff\1\172\1\uffff\1\145\1"
        u"\165\1\uffff\1\164\1\154\2\uffff\1\172\1\163\1\172\2\uffff\1\172"
        u"\1\uffff\1\163\1\156\1\145\1\uffff\1\141\2\uffff\1\164\1\uffff"
        u"\1\172\1\155\1\163\1\145\1\uffff\1\172\3\uffff\1\151\2\172\1\163"
        u"\1\171\1\uffff\1\172\1\50\1\141\1\uffff\1\164\2\uffff\1\145\1\50"
        u"\2\uffff\1\163\1\171\1\50\1\uffff\1\145\1\50\1\uffff\1\50\2\uffff"
        )

    DFA3_accept = DFA.unpack(
        u"\1\uffff\1\1\1\2\37\uffff\1\140\1\141\1\uffff\1\143\1\144\1\145"
        u"\1\146\1\147\1\150\1\uffff\1\152\102\uffff\1\142\1\151\3\uffff"
        u"\1\5\1\uffff\1\60\13\uffff\1\22\21\uffff\1\116\37\uffff\1\120\1"
        u"\uffff\1\121\1\117\1\uffff\1\122\10\uffff\1\6\2\uffff\1\112\1\132"
        u"\7\uffff\1\63\2\uffff\1\20\4\uffff\1\66\6\uffff\1\130\25\uffff"
        u"\1\65\1\73\6\uffff\1\123\4\uffff\1\131\1\uffff\1\113\1\133\3\uffff"
        u"\1\124\7\uffff\1\27\12\uffff\1\134\1\uffff\1\12\2\uffff\1\57\1"
        u"\13\1\54\7\uffff\1\15\1\uffff\1\110\4\uffff\1\26\1\uffff\1\21\1"
        u"\uffff\1\125\7\uffff\1\42\7\uffff\1\70\1\uffff\1\111\3\uffff\1"
        u"\126\1\uffff\1\135\1\3\1\4\1\uffff\1\23\3\uffff\1\55\2\uffff\1"
        u"\62\12\uffff\1\53\1\uffff\1\14\11\uffff\1\25\2\uffff\1\41\2\uffff"
        u"\1\64\10\uffff\1\114\1\115\1\uffff\1\127\2\uffff\1\7\15\uffff\1"
        u"\56\1\106\1\33\2\uffff\1\72\1\16\1\17\1\24\1\uffff\1\61\1\101\1"
        u"\31\3\uffff\1\44\2\uffff\1\51\3\uffff\1\107\3\uffff\1\71\4\uffff"
        u"\1\10\2\uffff\1\11\1\uffff\1\52\2\uffff\1\136\2\uffff\1\35\1\37"
        u"\3\uffff\1\76\1\43\1\uffff\1\137\3\uffff\1\40\1\uffff\1\75\1\30"
        u"\1\uffff\1\32\4\uffff\1\46\1\uffff\1\77\1\50\1\67\5\uffff\1\36"
        u"\3\uffff\1\47\1\uffff\1\74\1\34\2\uffff\1\45\1\105\3\uffff\1\100"
        u"\2\uffff\1\103\1\uffff\1\104\1\102"
        )

    DFA3_special = DFA.unpack(
        u"\u01f2\uffff"
        )

            
    DFA3_transition = [
        DFA.unpack(u"\1\54\1\53\1\uffff\2\54\22\uffff\1\44\1\51\1\uffff\1"
        u"\42\1\uffff\1\42\1\52\1\uffff\1\1\1\2\1\42\1\uffff\1\43\1\42\1"
        u"\45\14\42\1\uffff\1\47\1\50\1\46\2\uffff\1\31\1\34\1\40\1\41\1"
        u"\42\1\30\2\42\1\35\2\42\1\37\1\42\1\36\1\33\3\42\1\27\3\42\1\3"
        u"\3\42\4\uffff\1\42\1\uffff\1\6\1\14\1\7\1\10\1\22\1\15\1\20\1\26"
        u"\1\5\2\42\1\17\1\21\1\23\1\32\1\16\1\42\1\11\1\13\1\12\1\42\1\24"
        u"\1\4\1\25\2\42"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\55"),
        DFA.unpack(u"\1\56"),
        DFA.unpack(u"\1\61\7\uffff\1\60\1\uffff\1\57"),
        DFA.unpack(u"\1\62\7\uffff\1\63\1\uffff\1\64\4\uffff\1\65"),
        DFA.unpack(u"\1\70\6\uffff\1\66\2\uffff\1\67"),
        DFA.unpack(u"\1\71\3\uffff\1\73\13\uffff\1\72"),
        DFA.unpack(u"\1\74\17\uffff\1\75"),
        DFA.unpack(u"\1\102\7\uffff\1\76\5\uffff\1\100\2\uffff\1\77\6\uffff"
        u"\1\101"),
        DFA.unpack(u"\1\105\3\uffff\1\103\12\uffff\1\104\1\106"),
        DFA.unpack(u"\1\112\6\uffff\1\107\5\uffff\1\110\6\uffff\1\111"),
        DFA.unpack(u"\1\114\7\uffff\1\113"),
        DFA.unpack(u"\1\120\6\uffff\1\116\11\uffff\1\115\1\117"),
        DFA.unpack(u"\1\122\7\uffff\1\123\13\uffff\1\121"),
        DFA.unpack(u"\1\124"),
        DFA.unpack(u"\1\125\13\uffff\1\126"),
        DFA.unpack(u"\1\127\3\uffff\1\131\3\uffff\1\130\1\uffff\1\132"),
        DFA.unpack(u"\1\133\15\uffff\1\135\5\uffff\1\134"),
        DFA.unpack(u"\1\136"),
        DFA.unpack(u"\1\137"),
        DFA.unpack(u"\1\140"),
        DFA.unpack(u"\1\141\17\uffff\1\142"),
        DFA.unpack(u"\1\143"),
        DFA.unpack(u"\1\144\4\uffff\1\145"),
        DFA.unpack(u"\1\146"),
        DFA.unpack(u"\1\147"),
        DFA.unpack(u"\1\151\23\uffff\1\150"),
        DFA.unpack(u"\1\152"),
        DFA.unpack(u"\1\153"),
        DFA.unpack(u"\1\154"),
        DFA.unpack(u"\1\155"),
        DFA.unpack(u"\1\156"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\54\1\uffff\2\54\22\uffff\1\54"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\2\54\1\uffff\2\54\22\uffff\1\54"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\161"),
        DFA.unpack(u"\1\162"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\23\42\1\163\6\42"),
        DFA.unpack(u"\1\165"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\167"),
        DFA.unpack(u"\1\170"),
        DFA.unpack(u"\1\172\11\uffff\1\171"),
        DFA.unpack(u"\1\173"),
        DFA.unpack(u"\1\174\6\uffff\1\175"),
        DFA.unpack(u"\1\176"),
        DFA.unpack(u"\1\u0080\3\uffff\1\177"),
        DFA.unpack(u"\1\u0081"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0083\14\uffff\1\u0084"),
        DFA.unpack(u"\1\u0085"),
        DFA.unpack(u"\1\u0086"),
        DFA.unpack(u"\1\u0087"),
        DFA.unpack(u"\1\u0088"),
        DFA.unpack(u"\1\u0089"),
        DFA.unpack(u"\1\u008a"),
        DFA.unpack(u"\1\u008b"),
        DFA.unpack(u"\1\u008c\5\uffff\1\u008d"),
        DFA.unpack(u"\1\u008e\15\uffff\1\u008f"),
        DFA.unpack(u"\1\u0090"),
        DFA.unpack(u"\1\u0091"),
        DFA.unpack(u"\1\u0092"),
        DFA.unpack(u"\1\u0093"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0095"),
        DFA.unpack(u"\1\u0096\1\uffff\1\u0097"),
        DFA.unpack(u"\1\u0098"),
        DFA.unpack(u"\1\u0099\5\uffff\1\u009a"),
        DFA.unpack(u"\1\u009b"),
        DFA.unpack(u"\1\u009c"),
        DFA.unpack(u"\1\u009d"),
        DFA.unpack(u"\1\u009e"),
        DFA.unpack(u"\1\u009f"),
        DFA.unpack(u"\1\u00a0"),
        DFA.unpack(u"\1\u00a1"),
        DFA.unpack(u"\1\u00a2"),
        DFA.unpack(u"\1\u00a3"),
        DFA.unpack(u"\1\u00a4"),
        DFA.unpack(u"\1\u00a5"),
        DFA.unpack(u"\1\u00a6"),
        DFA.unpack(u"\1\u00a7"),
        DFA.unpack(u"\1\u00a8"),
        DFA.unpack(u"\1\u00a9"),
        DFA.unpack(u"\1\u00aa"),
        DFA.unpack(u"\1\u00ab"),
        DFA.unpack(u"\1\u00ac"),
        DFA.unpack(u"\1\u00ad"),
        DFA.unpack(u"\1\u00ae"),
        DFA.unpack(u"\1\u00af"),
        DFA.unpack(u"\1\u00b0"),
        DFA.unpack(u"\1\u00b1"),
        DFA.unpack(u"\1\u00b2"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\3\42\1\u00b3\26\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\3\42\1\u00b5\26\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u00b8"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u00ba"),
        DFA.unpack(u"\1\u00bb"),
        DFA.unpack(u"\1\u00bc"),
        DFA.unpack(u"\1\u00bd"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u00be"),
        DFA.unpack(u"\1\u00bf"),
        DFA.unpack(u"\1\u00c0"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u00c1"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u00c3"),
        DFA.unpack(u"\1\u00c4"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u00c7\15\uffff\1\u00c8"),
        DFA.unpack(u"\1\u00c9"),
        DFA.unpack(u"\1\u00ca"),
        DFA.unpack(u"\1\u00cb"),
        DFA.unpack(u"\1\u00cc"),
        DFA.unpack(u"\1\u00cd"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u00cf"),
        DFA.unpack(u"\1\u00d0"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u00d2"),
        DFA.unpack(u"\1\u00d3"),
        DFA.unpack(u"\1\u00d4"),
        DFA.unpack(u"\1\u00d5"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u00d7"),
        DFA.unpack(u"\1\u00d8"),
        DFA.unpack(u"\1\u00d9\1\uffff\1\u00da"),
        DFA.unpack(u"\1\u00db"),
        DFA.unpack(u"\1\u00dc"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u00de"),
        DFA.unpack(u"\1\u00df"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u00e0"),
        DFA.unpack(u"\1\u00e1"),
        DFA.unpack(u"\1\u00e2"),
        DFA.unpack(u"\1\u00e3"),
        DFA.unpack(u"\1\u00e4"),
        DFA.unpack(u"\1\u00e5"),
        DFA.unpack(u"\1\u00e6"),
        DFA.unpack(u"\1\u00e7"),
        DFA.unpack(u"\1\u00e8"),
        DFA.unpack(u"\1\u00e9"),
        DFA.unpack(u"\1\u00ea"),
        DFA.unpack(u"\1\u00eb"),
        DFA.unpack(u"\1\u00ec"),
        DFA.unpack(u"\1\u00ed"),
        DFA.unpack(u"\1\u00ef\1\uffff\1\u00ee"),
        DFA.unpack(u"\1\u00f1\16\uffff\1\u00f0"),
        DFA.unpack(u"\1\u00f2"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u00f5"),
        DFA.unpack(u"\1\u00f6\2\uffff\1\u00f7\1\u00f8\5\uffff\1\u00f9\5"
        u"\uffff\1\u00fa"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u00fc"),
        DFA.unpack(u"\1\u00fd"),
        DFA.unpack(u"\1\u00fe"),
        DFA.unpack(u"\1\u00ff"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0101"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0104"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0105"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0106"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0108"),
        DFA.unpack(u"\1\u0109"),
        DFA.unpack(u"\1\u010a"),
        DFA.unpack(u"\1\u010b"),
        DFA.unpack(u"\1\u010c"),
        DFA.unpack(u"\1\u010d"),
        DFA.unpack(u"\1\u010e"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0110"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0111"),
        DFA.unpack(u"\1\u0112"),
        DFA.unpack(u"\1\u0113"),
        DFA.unpack(u"\1\u0114"),
        DFA.unpack(u"\1\u0115"),
        DFA.unpack(u"\1\u0116"),
        DFA.unpack(u"\1\u0119\1\uffff\1\u0117\1\u0118"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u011b"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u011d"),
        DFA.unpack(u"\1\u011e"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0122"),
        DFA.unpack(u"\1\u0123"),
        DFA.unpack(u"\1\u0124"),
        DFA.unpack(u"\1\u0125"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0126"),
        DFA.unpack(u"\1\u0127"),
        DFA.unpack(u"\1\u0128"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\4\42\1\u012a\25\42"),
        DFA.unpack(u"\1\u012c"),
        DFA.unpack(u"\1\u012d"),
        DFA.unpack(u"\1\u012e"),
        DFA.unpack(u"\1\u012f"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0131"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0133"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0135"),
        DFA.unpack(u"\1\u0136"),
        DFA.unpack(u"\1\u0137"),
        DFA.unpack(u"\1\u0138"),
        DFA.unpack(u"\1\u0139"),
        DFA.unpack(u"\1\u013a"),
        DFA.unpack(u"\1\u013b"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u013d"),
        DFA.unpack(u"\1\u013e"),
        DFA.unpack(u"\1\u013f"),
        DFA.unpack(u"\1\u0140"),
        DFA.unpack(u"\1\u0141"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0142"),
        DFA.unpack(u"\1\u0143"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0145"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0147"),
        DFA.unpack(u"\1\u0148"),
        DFA.unpack(u"\1\u0149"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u014b"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u014f"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0151"),
        DFA.unpack(u"\1\u0152"),
        DFA.unpack(u"\1\u0153"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0155"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\21\42\1\u0156\10\42"),
        DFA.unpack(u"\1\u0158"),
        DFA.unpack(u"\1\u0159"),
        DFA.unpack(u"\1\u015a"),
        DFA.unpack(u"\1\u015b"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u015c"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u015d"),
        DFA.unpack(u"\1\u015e"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0160\16\uffff\1\u015f"),
        DFA.unpack(u"\1\u0161"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0163"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0165"),
        DFA.unpack(u"\1\u0166"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0167"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0168"),
        DFA.unpack(u"\1\u0169"),
        DFA.unpack(u"\1\u016a"),
        DFA.unpack(u"\1\u016b"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u016c"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u016d"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u016f"),
        DFA.unpack(u"\1\u0170"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0172"),
        DFA.unpack(u"\1\u0173"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0175"),
        DFA.unpack(u"\1\u0176"),
        DFA.unpack(u"\1\u0177"),
        DFA.unpack(u"\1\u0178"),
        DFA.unpack(u"\1\u0179"),
        DFA.unpack(u"\1\u017a"),
        DFA.unpack(u"\1\u017b"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u017c"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u017f"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0181"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0182"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0184"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0186\1\uffff\1\u0185"),
        DFA.unpack(u"\1\u0187"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u0188"),
        DFA.unpack(u"\1\u0189"),
        DFA.unpack(u"\1\u018a"),
        DFA.unpack(u"\1\u018b"),
        DFA.unpack(u"\1\u018c"),
        DFA.unpack(u"\1\u018d"),
        DFA.unpack(u"\1\u018e"),
        DFA.unpack(u"\1\u018f"),
        DFA.unpack(u"\1\u0190"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u0194"),
        DFA.unpack(u"\1\u0195"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\21\42\1\u019a\10\42"),
        DFA.unpack(u"\1\u019c"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u019e"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u019f"),
        DFA.unpack(u"\1\u01a0"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01a2"),
        DFA.unpack(u"\1\u01a3"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01a5"),
        DFA.unpack(u"\1\u01a6"),
        DFA.unpack(u"\1\u01a7"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01a9"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01aa"),
        DFA.unpack(u"\1\u01ab"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01ad"),
        DFA.unpack(u"\1\u01ae"),
        DFA.unpack(u"\1\u01af"),
        DFA.unpack(u"\1\u01b0"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01b2"),
        DFA.unpack(u"\1\u01b3"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01b5"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01b7"),
        DFA.unpack(u"\1\u01b8"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01ba"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01bb"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01be"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01bf"),
        DFA.unpack(u"\1\u01c0"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01c1"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01c3"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01c5"),
        DFA.unpack(u"\1\u01c6"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01c7"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01c9"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01cc"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01ce"),
        DFA.unpack(u"\1\u01cf"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01d0"),
        DFA.unpack(u"\1\u01d1"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01d3"),
        DFA.unpack(u"\1\42\1\uffff\1\42\2\uffff\1\u01d4\1\uffff\1\42\2\uffff"
        u"\1\42\1\uffff\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01d7"),
        DFA.unpack(u"\1\u01d8"),
        DFA.unpack(u"\1\u01d9"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01da"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01db"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01dd"),
        DFA.unpack(u"\1\u01de"),
        DFA.unpack(u"\1\u01df"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01e1"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01e4"),
        DFA.unpack(u"\1\u01e5"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\42\1\uffff\1\42\4\uffff\1\42\2\uffff\1\42\1\uffff"
        u"\14\42\6\uffff\32\42\4\uffff\1\42\1\uffff\32\42"),
        DFA.unpack(u"\1\u01e7"),
        DFA.unpack(u"\1\u01e8"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01e9"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01ea"),
        DFA.unpack(u"\1\u01eb"),
        DFA.unpack(u""),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01ec"),
        DFA.unpack(u"\1\u01ed"),
        DFA.unpack(u"\1\u01ee"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01ef"),
        DFA.unpack(u"\1\u01f0"),
        DFA.unpack(u""),
        DFA.unpack(u"\1\u01f1"),
        DFA.unpack(u""),
        DFA.unpack(u"")
    ]

    # class definition for DFA #3

    DFA3 = DFA
 



def main(argv, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    from antlr3.main import LexerMain
    main = LexerMain(SqlDASLexer)
    main.stdin = stdin
    main.stdout = stdout
    main.stderr = stderr
    main.execute(argv)


if __name__ == '__main__':
    main(sys.argv)
