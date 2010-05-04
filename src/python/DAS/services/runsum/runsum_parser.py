#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
RunSummary XML parser
"""
__revision__ = "$Id: runsum_parser.py,v 1.3 2009/09/01 01:42:47 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

try:
    # Python 2.5
    import xml.etree.ElementTree as ET
except:
    # prior requires elementtree
    import elementtree.ElementTree as ET

def parser(data):
    """
    RunSummary XML parser, it returns a list of dict rows, e.g.
    [{'file':value, 'run':value}, ...]
    """
    elem  = ET.fromstring(data)
    for i in elem:
        if  i.tag == 'runInfo':
            row = {}
            for j in i:
                if  j.tag:
                    newkey = self.dasmapping.notation2das(system, j.tag)
                    row[newkey] = j.text
#                    row[j.tag] = j.text
                nrow = {}
                for k in j.getchildren():
                    if  k.tag:
                        nkey = self.dasmapping.notation2das(system, k.tag)
                        nrow[nkey] = k.text
#                        nrow[k.tag] = k.text
                if  nrow:
                    row[newkey] = nrow
#                    row[j.tag] = nrow
            yield row

#
# main
#
if __name__ == '__main__':
    FDESC = open('runsum.xml', 'r')
    RESULTS = parser(FDESC.read())
    for item in RESULTS:
        print item
