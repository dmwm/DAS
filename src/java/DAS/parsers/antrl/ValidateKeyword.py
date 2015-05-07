from __future__ import print_function
ents = ['ads' , 'config' , 'dataset' , 'release' , 'tier' , 'site' , 'block' , 'file' , 'primds' , 'procds' , 'run' , 'lumi' , 'dq' , 'ilumi' , 'phygrp' , 'group', 'pset', 'algo' , 'datatype' , 'mcdesc' , 'trigdesc' , 'branch']
attr = ['createdate' , 'moddate' , 'starttime' , 'endtime' , 'createby' , 'modby' , 'name' , 'dataset' , 'version' , 'number' , 'startevnum' , 'endevnum' , 'numevents' , 'numfiles' , 'numlss' , 'totlumi' , 'store' , 'size' , 'release' , 'count' , 'status' , 'type' , 'id' , 'parent' , 'child' , 'tier' , 'def' , 'evnum' , 'era' , 'tag' , 'xsection' , 'hash' , 'content' , 'family', 'exe' , 'annotation' , 'checksum']
funcs = ['count', 'sum']

class InvalidKeywordException:
	def __init__(self, message):
		self.msg = message
		print(message)

def validateEntity(entity):
	if entity not in ents: raise InvalidKeywordException('\nEntity ' + entity + ' not allowed\n. Allowed values are ' + str(ents))
	
def validateAttribute(attribute):
	if attribute not in attr: raise InvalidKeywordException('\nAttribute ' + attribute + ' not allowed\n. Allowed values are ' + str(attr))

def validateFunction(function):
	if function not in funcs: raise InvalidKeywordException('\nFunction ' + function + ' not allowed\n. Allowed values are ' + str(funcs))

def validateKw(kw):
	if(kw.find('(') != -1):
		#Aggregate Function 
		tokens = kw.split('(')
		validateFunction(tokens[0])
		#Check for last caharacter . It has to be close bracket
		if kw[len(kw) -1 ] != ')': raise InvalidKeywordException(' Bracket ) missing or improperly placed when using function ' + tokens[0])
		#Validate the parameter of the function. For example count(abc) validate abc
		paramter = kw[kw.find('(') + 1 :kw.find(')')]
		validateKw(paramter)
		
	else: 	
		tokens = kw.split('.')
		if(len(tokens) == 1):
			# No Attribute 
			validateEntity(tokens[0])
		elif(len(tokens) == 2):
			# Both entity and attribute
			validateEntity(tokens[0])
			validateAttribute(tokens[1])
		else : InvalidKeywordException('\nKeyword ' + kw + ' not allowed\n')
		

#validateKw('counta(file.size)')
