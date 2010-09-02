import ply

class DASPLY(object):
    tokens = [
		'PIPE',
        'AGGREGATOR',
        'FILTER',
        'MAPREDUCE',
        'COMMA',
        'LSQUARE',
		'RSQUARE',
		'LPAREN',
        'RPAREN',
        'OPERATOR',
		'OPERATORLIST',
		'WORD',
		'NUMBER',
		'DATE'
    ]

	t_PIPE = r'\|'
	t_FILTER = r'grep|unique'
	t_OPERATOR = r'=|last'
	t_OPERATORLIST = r'in|between'
    t_COMMA    = r'\,'
	t_AGGREGATOR = r'sum|min|max|avg|count'
	t_MAPREDUCE = r'NEVER MATCH ME'
	t_LSQUARE = r'\['
	t_RSQUARE = r'\]'
	t_LPAREN = r'\('
	t_RPAREN = r'\)'
    def t_NUMBER(self, t):
        r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?'
        t.value = float(t.value)
        return t

    def t_DATE(self, t):
        r'[0-2]0[0-9][0-9][0-1][0-9][0-3][0-9]'
        t.value = int(t.value)
        return t

    def t_WORD(self, t):
        r'''[a-zA-Z/*][a-zA-Z_0-9/*\-#]+|[0-9]+[dhm]|([0-9]{1,3}\.){3,3}[0-9]{1,3}|["'][a-zA-Z_0-9/*\-#]+\s[a-zA-Z_0-9/*\-#]+["']'''
        if t[0] in ('"',"'") and t[-1] in ('"',"'"):
			return t[1:-1]
		return t

	def p_overall(self, p):
		'''overall : keys (PIPE pipefunc)*'''
	def p_keys0(self, p):
		'''keys : WORD (keyop)*'''
	def p_keys1(self, p):
		'''keys : (keyop)*'''
	def p_keyop(self, p):
		'''keyop : WORD OPERATOR (WORD|NUMBER|DATE)
 				 | WORD OPERATORLIST array'''
	def p_array(self, p):
		'''array : LSQUARE (WORD|NUMBER|DATE) (COMMA WORD|NUMBER|DATE)* RSQUARE'''
	def p_arglist(self, p):
		'''arglist: WORD (COMMA WORD)*'''
	def p_pipefunc0(self, p):
		'''pipefunc : FILTER (arglist)?'''
	def p_oneagg(self, p):
		'''oneagg : AGGREGATOR LPAREN WORD RPAREN'''
	def p_pipefunc1(self, p):
		'''pipefunc : oneagg (COMMA oneagg)*'''
	

		

    
