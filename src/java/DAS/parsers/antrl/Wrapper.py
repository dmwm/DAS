from __future__ import absolute_import
import sys
import antlr3

from .SqlDASLexer import SqlDASLexer
from .SqlDASParser import SqlDASParser
from .SqlDASParser import kws, okws, constraints, orderingkw

class Wrapper:
	def parseQuery(self, query):
		try:
			l = SqlDASLexer(antlr3.ANTLRStringStream(query))
			tokens = antlr3.CommonTokenStream(l)
			p = SqlDASParser(tokens)
			p.stmt()
			toReturn = {}
			toReturn['FIND_KEYWORDS'] = kws
			toReturn['WHERE_CONSTRAINTS'] = constraints
			toReturn['ORDER_BY_KEYWORDS'] = okws
			toReturn['ORDERING'] = orderingkw
			return toReturn
		except antlr3.exceptions.NoViableAltException as expObj:
			#print 'error ',expObj
			t = expObj.token
			msg = "Invalid Token " + str(t.getText()) + " on line " + str(t.getLine()) + " at column " + str(t.getCharPositionInLine()) + "\n"
			msg += "QUERY    " + query + "\nPOSITION "
			pos = int(t.getCharPositionInLine())
			if  pos > 0:
				for i in range(pos): msg += " "
			msg += "^\n";
			#print msg
			raise msg

#query = "find dataset where dataset = abc and (file = xyz or run like 12*) order by file, dataset desc"
#query = sys.argv[1]
#parserObj = Wrapper()
#tokens = parserObj.parseQuery(query)
#print 'FIND_KEYWORDS ', tokens['FIND_KEYWORDS']
#print 'WHERE_CONSTRAINTS ', tokens['WHERE_CONSTRAINTS']
#print 'ORDER_BY_KEYWORDS ', tokens['ORDER_BY_KEYWORDS']
#print 'ORDERING', tokens['ORDERING']
