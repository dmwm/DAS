#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103,C0301,E0611
#  error code E0611 for 
#  E: 15: No name 'son_manipulator' in module 'pymongo'
#  E: 16: No name 'son' in module 'pymongo'

"""
DAS MongoDB SON manipulator.
"""

__revision__ = "$Id: das_son_manipulator.py,v 1.5 2010/04/13 13:33:19 valya Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "Valentin Kuznetsov"

import types

from pymongo.son_manipulator import SONManipulator
from pymongo.son import SON

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
        if  type(son) is types.DictType and son.has_key('_id'):
            obj_id = son['_id']
            son['_id'] = str(obj_id)
        if  type(son) is types.DictType and son.has_key('cache_id'):
            objcache_id = son['cache_id']
            if  type(objcache_id) is types.ListType:
                son['cache_id'] = [str(r) for r in objcache_id]
            else:
                son['cache_id'] = objcache_id
        if  type(son) is types.DictType and son.has_key('das_id'):
            objdas_id = son['das_id']
            if  type(objdas_id) is types.ListType:
                son['das_id'] = [str(r) for r in objdas_id]
            else:
                son['das_id'] = objdas_id
        return son

