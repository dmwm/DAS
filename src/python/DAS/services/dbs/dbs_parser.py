#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS XML parser
"""
__revision__ = "$Id: dbs_parser.py,v 1.1 2009/03/09 19:43:32 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

from xml.dom.minidom import parseString
import elementtree.ElementTree as ET

CDICT = {
'STORAGEELEMENT_SENAME':'site',
'PATH':'dataset',
'FILES_LOGICALFILENAME':'file',
'BLOCK_NAME':'block',
}
def parser(data):
    """
    DBS XML parser, it returns a list of dict rows, e.g.
    [{'file':value, 'run':value}, ...]
    """
    dom = parseString(data)
    odict = {}
    olist = []
    for node in dom.getElementsByTagName('result'):
        odict = {}
        for attr in node.attributes.keys():
            key = CDICT[attr]
            odict[key] = str(node.getAttribute(attr))
#            odict[attr] = str(node.getAttribute(attr))
        if  odict:
            olist.append(odict)
    return olist

def parser_new(data):
    """
    DBS XML parser, it returns a list of dict rows, e.g.
    [{'file':value, 'run':value}, ...]
    """
    dom = parseString(data)
    olist = []
    for node in dom.getElementsByTagName('result'):
        item = []
        for child in node.childNodes:
            node3 = child.firstChild
            if  node3 is None:
                continue
            elem = node3.data
            item.append(elem)
        olist.append(item)
    return olist

def parser_dbshelp(data):
    """
    XML parser for DBS getHelp API
    """
    elem  = ET.fromstring(data)
    olist = []
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
