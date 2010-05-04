grammar SqlDAS;

options {
	language=Python;
}


//@header "SqlDASParser.__init__" {
@header  {
	kws = []
	okws = []
	constraints = []
	orderingkw = []

}

@members {	

}

@rulecatch {
except:
	raise
}

stmt	: select spaces selectList spaces where spaces constraintList spaces (order spaces by spaces orderList)*
	| select spaces selectList spaces
	| select spaces selectList spaces order spaces by spaces orderList;
	//| select spaces selectList spaces where spaces constraintList spaces;
	//| select spaces selectList spaces where spaces constraintList spaces order spaces by spaces orderList;



spaces	: (SPACE)*;

orderList	:okw=	keyword 		{okws.append(str($okw.text))}
 		(
		spaces
		COMMA
		spaces
 	okw=	keyword  		{okws.append(str($okw.text))}
 		)*
		spaces
	oing = 	ordering {orderingkw.append(str($oing.text))} 
		; 

ordering 	: (asc|desc)?;	 
selectList	:kw=	keyword 		{kws.append(str($kw.text))}
 		(
		spaces
		COMMA
		spaces
 	kw=	keyword  		{kws.append(str($kw.text))}
 		)*;		 
 		
keyword	: entity 
	| entity DOT attr
	| entity DOT funct
	| count spaces '(' spaces entity spaces ')'
	| sum spaces '(' spaces entity DOT attr spaces ')';
	
constraintList	: constraint1 ( spaces 
	rel=	logicalOp 		{ constraints.append(str($rel.text));}
		spaces constraint1)*;
lopen		: (lb)*;
ropen		: (rb)*;
constraint1     : kl=   lopen   {c1 = {}} {c1['bracket'] = str($kl.text)} {if str($kl.text) != '': constraints.append(c1)}
                constraint 
                kr=     ropen   {c = {}} {c['bracket'] = str($kr.text)} {if str($kr.text) != '':constraints.append(c)};

constraint	: kw=	keyword 		{c = {}} {c['key'] = str($kw.text)} 
		spaces
	 op=	compOpt 	{c['op'] = str($op.text)}   
		spaces
	 val=	genValue 	{c['value'] = str($val.text)} {constraints.append(c)}
		|
	kw=	keyword 		{c = {}} {c['key'] = str($kw.text)} 
		spaces 
	op1=	inpython 	 	{c['op'] = str($op1.text)}  
		spaces '('
		spaces
	val1=	valueList 		{c['value'] = str($val1.text)} {constraints.append(c);}
		spaces
		')'               
		| 
	kw=	keyword 		{c = {}} {c['key'] = str($kw.text)} 
		spaces 
	op2=	like 		{c['op'] = str($op2.text)} 
		spaces 
	val2=	dotValue 		{c['value'] = str($val2.text)} {constraints.append(c)}
		|
 	kw=	keyword 		{c = {}} {c['key'] = str($kw.text)} 
		spaces 
	op3=	between		{c['op'] = str($op3.text)} 
		spaces 
	val3=	betValue 		{c['value'] = str($val3.text)} {constraints.append(c)};                  
                 

where	:('WHERE' | 'where');
dotValue        : VALUE 
		| 'in'
		| VALUE DOT 'in'
		| VALUE DOT VALUE
		| VALUE DOT VALUE DOT 'in'
		| VALUE DOT VALUE DOT VALUE
		| VALUE DOT VALUE DOT VALUE DOT 'in'
		| VALUE DOT VALUE DOT VALUE DOT VALUE
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE 
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE 
		| VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT VALUE DOT 'in'
		| VALUE SPACE VALUE
		| VALUE SPACE VALUE SPACE VALUE;
//dateValue	: (DIGIT);

//cfgValue        : VALUE DOT VALUE SPACE LT SPACE VALUE;
valueList	:dotValue ( spaces COMMA spaces dotValue )*;
//cfgList		:dotValue ( spaces AMP spaces dotValue )*;
compOpt		:(EQ)
		|(LT)
		|(GT)
		|(NOT)(EQ)
		|(EQ)(GT)
		|(EQ)(LT)
		|(LT)(EQ)
		|(GT)(EQ)
		|(LT)(GT);

genValue	:dotValue
		|dotValue compOpt dotValue (AMP dotValue compOpt dotValue)*;
betValue	:dotValue spaces andpython spaces dotValue;
//cfgValue	: genValue (spaces AMP spaces genValue)*;

//likeValue 	:(dotValue| STAR)+;
//likeValue 	:(dotValue)+;
//likes		:(not)? spaces like;
logicalOp	:(andpython|orpython);
entity		: ('ads' | 'config' | 'dataset' | 'release' | 'tier' | 'site' | 'block' | 'file' | 'primds' | 'procds' | 'run' | 'lumi' | 'dq' | 'ilumi' | 'phygrp' | 'group'| 'pset'| 'algo' | 'datatype' | 'mcdesc' | 'trigdesc' | 'branch');
attr		:('createdate' | 'moddate' | 'starttime' | 'endtime' | 'createby' | 'modby' | 'name' | 'dataset' | 'version' | 'number' | 'startevnum' | 'endevnum' | 'numevents' | 'numfiles' | 'numlss' | 'totlumi' | 'store' | 'size' | 'release' | 'count' | 'status' | 'type' | 'id' | 'parent' | 'child' | 'tier' | 'def' | 'evnum' | 'era' | 'tag' | 'xsection' | 'hash' | 'content' | 'family'| 'exe' | 'annotation' | 'checksum' );
funct		:('numruns()' | 'numfiles()' | 'dataquality()' | 'latest()' | 'parentrelease()' | 'childrelease()' | 'intluminosity()' | 'findevents()' );
select		:('select' | 'SELECT' | 'find' | 'FIND');
andpython	:('and' | 'AND');
order		:('order' | 'ORDER');
by		:('by' | 'BY');
orpython	:('or' | 'OR');
inpython	:('in' | 'IN');
notpython	:('not' | 'NOT');
like		:('like' | 'LIKE' | 'not' spaces 'like' | 'NOT' spaces 'LIKE');
//like		:('like' | 'LIKE');
count		:('count' | 'COUNT');
sum		:('sum' | 'SUM');
asc		:('asc' | 'ASC');
desc		:('desc' | 'DESC');
between		:('between' | 'BETWEEN');
lb		: ('(');
rb		: (')');
//likeLeft	:('LikeLeft');
//likeRight	:('LikeRight');
//likeCfg		:('<like>');
//pset		:('pset');
VALUE		:('a'..'z'|'A'..'Z'|'0'..'9'|'/'|'-'|'_'|':'|'#'|'*'|'%' )+ ;
//DVALUE		:('a'..'z'|'A'..'Z'|'0'..'9'|'/'|'-'|'_'|':'|'#'|'.' )+ ;
//DIGIT		:('0'..'9');
//DASH		:('-');
//COLON		:(':');
//CHAR		:('a'..'z'|'A'..'Z');
COMMA		:(',');
SPACE		:(' ');
DOT		:('.');
//QUOTE		:('"');
GT		:('>');
LT		:('<');
EQ		:('=');
NOT		:('!');
AMP		:('&');
//STAR		:('*'|'%');
NL		:('\n');
WS 		: ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ 	{ $channel = HIDDEN; } ;

