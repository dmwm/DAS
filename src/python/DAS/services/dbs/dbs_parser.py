#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS XML parser
"""
__revision__ = "$Id: dbs_parser.py,v 1.7 2009/06/09 18:18:48 valya Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "Valentin Kuznetsov"

#from xml.dom.minidom import parseString
try:
    # Python 2.5
    import xml.etree.ElementTree as ET
except:
    # prior requires elementtree
    import elementtree.ElementTree as ET

#CDICT = {
#'STORAGEELEMENT_SENAME':'site',
#'PATH':'dataset',
#'FILES_LOGICALFILENAME':'file',
#'BLOCK_NAME':'block',
#}
#def parser_old(data):
#    """
#    DBS XML parser, it returns a list of dict rows, e.g.
#    [{'file':value, 'run':value}, ...]
#    """
#    dom = parseString(data)
#    odict = {}
#    olist = []
#    for node in dom.getElementsByTagName('result'):
#        odict = {}
#        for attr in node.attributes.keys():
#            key = CDICT[attr]
#            odict[key] = str(node.getAttribute(attr))
#        if  odict:
#            olist.append(odict)
#    return olist

def parser(data):
    """
    DBS XML parser, it returns a list of dict rows, e.g.
    [{'file':value, 'run':value}, ...]
    """
    elem  = ET.fromstring(data)
    for i in elem:
        if  i.tag == 'results':
            for j in i:
                row = {}
                for k in j.getchildren():
                    name = k.tag
                    if  name.find('_') != -1: # agg. function
                        nlist = name.split('_')
                        name  = '%s(%s)' % (nlist[0], nlist[1])
                    row[name] = k.text
                yield row

def parser_list(data):
    """
    DBS XML parser, it returns a list of dict rows, e.g.
    [{'file':value, 'run':value}, ...]
    """
    elem  = ET.fromstring(data)
    olist = []
    for i in elem:
        if  i.tag == 'results':
            for j in i:
                row = {}
                for k in j.getchildren():
                    name = k.tag
                    if  name.find('_') != -1: # agg. function
                        nlist = name.split('_')
                        name  = '%s(%s)' % (nlist[0], nlist[1])
                    row[name] = k.text
                olist.append(row)
    return olist

def parser_dbshelp(data):
    """
    XML parser for DBS getHelp API
    """
    olist = []
    if  not data:
        return olist
    elem  = ET.fromstring(data)
    for i in elem:
        if  i.tag == 'dbs-ql':
            for j in i:
                alist = []
                for k in j.getchildren():
                    if  k.tag == 'name':
                        key = k.text
                    elif k.tag == 'attr':
                        alist.append(k.text)
                olist += [key] + ['%s.%s' % (key, atr) for atr in alist]
    return olist
