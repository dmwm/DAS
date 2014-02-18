#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=C0103,C0301

"""
DAS MongoDB SON manipulator.
"""

__author__ = "Valentin Kuznetsov"

from pymongo.son_manipulator import SONManipulator
from bson.son import SON

class DAS_SONManipulator(SONManipulator):
    """DAS SON manipulator"""
    def __init__(self):
        SONManipulator.__init__(self)

    def transform_incoming(self, son, collection):
        """
        Manipulate an incoming SON object.
        """
        if  self.will_copy():
            return SON(son)
        return son

    def transform_outgoing(self, son, collection):
        """
        Manipulate an outgoing SON object.
        """
        if  self.will_copy():
            return SON(son)
        if  isinstance(son, dict) and '_id' in son:
            obj_id = son['_id']
            son['_id'] = str(obj_id)
        if  isinstance(son, dict) and 'cache_id' in son:
            objcache_id = son['cache_id']
            if  isinstance(objcache_id, list):
                son['cache_id'] = [str(r) for r in objcache_id]
            else:
                son['cache_id'] = objcache_id
        if  isinstance(son, dict) and 'das_id' in son:
            objdas_id = son['das_id']
            if  isinstance(objdas_id, list):
                son['das_id'] = [str(r) for r in objdas_id]
            else:
                son['das_id'] = objdas_id
        if  isinstance(son, dict) and 'gridfs_id' in son:
            objdas_id = son['gridfs_id']
            son['gridfs_id'] = objdas_id
        return son

