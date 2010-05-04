grammar SqlDAS;

options {
	language=Python;
}


//@header "SqlDASParser.__init__" {
@header  {
from ValidateKeyword import *


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

stmt	: select selectList (where constraintList)? (order by orderList)?;


orderList	:orderList1
		| orderList1 orderKw;

orderList1	:okw=	KW 		{okws.append(str($okw.text))} {validateKw($okw.text)}
 		(
		COMMA
 	okw=	KW  		{okws.append(str($okw.text))} {validateKw($okw.text)}
 		)*;

orderKw		:oing = 	ordering {orderingkw.append(str($oing.text))} ; 

ordering 	:asc|desc;	 
selectList	:kw=	gkw 		{kws.append(str($kw.text))} {validateKw($kw.text)}
 		(
		COMMA
 	kw=	gkw  		{kws.append(str($kw.text))} {validateKw($kw.text)}
 		)*;		 

KW	: ('a' .. 'z' | '0' .. '9' | 'A' .. 'Z'| '-' | '_' | ':' | '#' | '/' | '*' | '%' ) ( 'a' .. 'z' | '0' .. '9'| 'A' .. 'Z' | '.' | '-' | '_' | ':' | '#' | '/' | '*' | '%' | '&')*;
//gkw	: KW ( '('  KW  ')' )*; 
gkw	: KW | (('count'|'sum') '('  KW  ')') ; 
	
constraintList	: constraint1 (  
	rel=	logicalOp 		{ constraints.append(str($rel.text));}
		 constraint1)*;

lopen		: lb(lb)*;
ropen		: rb(rb)*;
constraint1     : kl=   lopen   {c1 = {}} {c1['bracket'] = str($kl.text)} {if str($kl.text) != '': constraints.append(c1)}
                constraint 
		(rel=    logicalOp               { constraints.append(str($rel.text));}
		constraint)*
                kr=     ropen   {c = {}} {c['bracket'] = str($kr.text)} {if str($kr.text) != '':constraints.append(c)}
		| constraint;

constraint	: kw=	KW		{c = {}} {c['key'] = str($kw.text)} {validateKw($kw.text)}
	 op=	compOpt 	{c['op'] = str($op.text)}   
	 val=	spaceValue 	{c['value'] = str($val.text)} {constraints.append(c)}
		|
	kw=	KW 		{c = {}} {c['key'] = str($kw.text)} {validateKw($kw.text)}
	op1=	inpython 	 	{c['op'] = str($op1.text)}  
		'('
	val1=	valueList 		{c['value'] = str($val1.text)} {constraints.append(c);}
		')'               
		| 
	kw=	KW 		{c = {}} {c['key'] = str($kw.text)} {validateKw($kw.text)}
	op2=	like 		{c['op'] = str($op2.text)} 
	val2=	KW 		{c['value'] = str($val2.text)} {constraints.append(c)}
		|
 	kw=	KW 		{c = {}} {c['key'] = str($kw.text)} {validateKw($kw.text)}
	op3=	between		{c['op'] = str($op3.text)} 
	val3=	betValue 		{c['value'] = str($val3.text)} {constraints.append(c)};                  
                 
spaceValue	: KW (KW)*;
valueList	: KW ( COMMA KW )*;
betValue	: KW andpython KW;
where	:('WHERE' | 'where');
compOpt		:(EQ)
		|(LT)
		|(GT)
		|(NOT)(EQ)
		|(EQ)(GT)
		|(EQ)(LT)
		|(LT)(EQ)
		|(GT)(EQ)
		|(LT)(GT);

logicalOp	:(andpython|orpython);
select		:('select' | 'SELECT' | 'find' | 'FIND');
andpython	:('and' | 'AND');
order		:('order' | 'ORDER');
by		:('by' | 'BY');
orpython	:('or' | 'OR');
inpython	:('in' | 'IN');
notpython	:('not' | 'NOT');
like		:('like' | 'LIKE' | 'not' 'like' | 'NOT' 'LIKE');
count		:('count' | 'COUNT');
sum		:('sum' | 'SUM');
asc		:('asc' | 'ASC');
desc		:('desc' | 'DESC');
between		:('between' | 'BETWEEN');
lb		: ('(');
rb		: (')');
COMMA		:(',');
DOT		:('.');
GT		:('>');
LT		:('<');
EQ		:('=');
NOT		:('!');
AMP		:('&');
WS		:(' '|'\r'|'\t'|'\n') {$channel=HIDDEN;};

