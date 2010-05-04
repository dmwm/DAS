#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Query parser for DAS
"""
__revision__ = "$Id: qlparser.py,v 1.1 2009/03/13 21:10:04 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

def dasqlparser(uinput):
    """
    Main routine which does the parsing of input user query
    it creates an output dictionary with selection list, list of queries
    for execution by DAS service and condition list
    """
    uinput = uinput.strip()
    rdict  = {}
    rdict['input'] = uinput

    bckdict  = {}
    counter  = 0
    while 1:
        bckobj = findbracketobj(uinput)
        if  not bckobj:
            break
        bidx = 'bobj_%s' % counter
        bckdict[bidx] = bckobj
        counter += 1
        uinput = uinput.replace(bckobj, bidx)
    
    sellist  = getselectkeys(uinput)
    condlist = getconditions(uinput) 
    query    = uinput
    queries  = {}
    for key, val in condlist.items():
        query   = query.replace(val, key)
        if  val.find('bobj_') != -1:
            val = bckdict[val]
        queries[key] = 'find %s where %s' % (','.join(sellist), val) 
#    rdict['bckobj'] = bckdict
    rdict['query'] = query
    rdict['sellist'] = sellist
    rdict['condlist'] = condlist
    rdict['queries'] = queries
    return rdict
    

def getselectkeys(uinput):
    "Extract selection keys from provided user input query"
    uinput = uinput.split(' where')[0]
    uinput = uinput.replace('find ', '').replace('plot ', '')
    return [x.strip() for x in uinput.split(',')]
        
def getnextcond(uinput):
    """
    Find out next where clause condition. It only understand conditions
    between and and or boolean operators
    """
    obj_and = ' and '
    obj_or  = ' or '
    idx_and = uinput.find(obj_and)
    idx_or  = uinput.find(obj_or)
    if  idx_and == -1 and idx_or == -1:
        return None, uinput
    if  idx_and != -1 and idx_or != -1:
        if  idx_and < idx_or:
            return uinput[0:idx_and], uinput[idx_and+len(obj_and):]
        else:
            return uinput[0:idx_or], uinput[idx_or+len(obj_or):]
    if  idx_and != -1 and idx_or == -1:
        return uinput[0:idx_and], uinput[idx_and+len(obj_and):]
    if  idx_and == -1 and idx_or != -1:
        return uinput[0:idx_or], uinput[idx_or+len(obj_or):]

def getconditions(uinput):
    """
    Find out where clause conditions and store them in output dictionary
    """
    sublist = uinput.split(' where ')
    rdict   = {}
    if  len(sublist)>1:
        substr = ' '.join(sublist[1:])
        counter = 0
        while 1:
            cond, rest = getnextcond(substr)
            substr = rest
            if  not cond:
                break
            rdict['q%s' % counter] = cond
            counter += 1
        if  substr:
            rdict['q%s' % counter] = substr
    return rdict

def findbracketobj(uinput):
    """
    Find out bracket object, e.g. ((test or test) or test)
    """
    left = uinput.find('(')
    robj = ''
    if  left != -1:
        rcount = 0
        for char in uinput[left:]:
            robj += char
            if  char == '(':
                rcount += 1
            if  char == ')':
                rcount -= 1
            if  char == ')' and not rcount:
                break
        if  rcount:
            msg = "Object '%s' has un-equal number of left & right brackets" \
                % uinput
            raise Exception(msg)
    return robj
        

