#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Dashboard XML parser
"""
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import xml.etree.cElementTree as ET

def parser(data):
    """
    Dashboard XML parser, it returns a list of dict rows, e.g.
    [{'file':value, 'run':value}, ...]
    """
    elem  = ET.fromstring(data)
    for i in elem:
        if  i.tag == 'summaries':
            for j in i:
                row = {}
                for k in j.getchildren():
                    name = k.tag
                    row[name] = k.text
                yield row

