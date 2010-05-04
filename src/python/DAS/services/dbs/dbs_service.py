#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS service
"""
__revision__ = "$Id: dbs_service.py,v 1.12 2009/10/21 15:41:04 valya Exp $"
__version__ = "$Revision: 1.12 $"
__author__ = "Valentin Kuznetsov"

import xml.etree.cElementTree as ET

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator

class DBSService(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dbs', config)
        self.reserved = ['api', 'apiversion']
        self.map = self.dasmapping.servicemap(self.name, 'javaservlet')
        map_validator(self.map)

    def transform_tag(self, system, tag, cache):
        """Transform given tag name into DAS notation"""
        if  cache.has_key(tag):
            newkey = cache[tag]
        else:
            newkey = self.dasmapping.notation2das(system, tag)
            cache[tag] = newkey
        return newkey

    def transform_row(self, system, row, cache):
        """Transform all keys for given row into DAS notation"""
        for key in row.keys():
            if  cache.has_key(key):
                newkey = cache[key]
            else:
                newkey = self.dasmapping.notation2das(system, key)
                cache[key] = newkey
            if  newkey != key:
                row[newkey] = row[key]
                del row[key] 
            
    def child_info(self, system, row, item, cache):
        """Get child info for given element item"""
        for jtem in item.getchildren():
            newkey = self.transform_tag(system, jtem.tag, cache)
            newrow = jtem.attrib
            self.transform_row(system, newrow, cache)
            if  row.has_key(newkey):
                row[newkey] = row[newkey] + [newrow]
            else:
                row[newkey] = [newrow]

    def parser(self, api, data, params=None):
        """
        DBS XML mini-parser. DBS XML use tag/attribute approach.
        So every DBS XML looks like <dbs><tag_name a=1 b=2/>.
        We return a dict of attributes for found tags
        """

        cache = {}
        for item in ET.fromstring(data):
            row = item.attrib
            if  row:
                self.transform_row(self.name, row, cache)
                self.child_info(self.name, row, item, cache)
                newkey = self.transform_tag(self.name, item.tag, cache)
                newrow = {newkey : row}
                yield newrow
