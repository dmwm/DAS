#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS MongoDB SON manipulator.
"""

__revision__ = "$Id: das_son_manipulator.py,v 1.3 2010/01/04 15:42:35 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

import types

from pymongo.son_manipulator import SONManipulator
from pymongo.son import SON

class DAS_SONManipulator(SONManipulator):
    def __init__(self):
        SONManipulator.__init__(self)

    def transform_incoming(self, son, collection):
        """Manipulate an incoming SON object.

        :Parameters:
          - `son`: the SON object to be inserted into the database
          - `collection`: the collection the object is being inserted into
        """
        if  self.will_copy():
            return SON(son)
        return son

    def transform_outgoing(self, son, collection):
        """Manipulate an outgoing SON object.
        :Parameters:
          - `son`: the SON object being retrieved from the database
          - `collection`: the collection this object was stored in
        """
        if  self.will_copy():
            return SON(son)
        if  type(son) is types.DictType and son.has_key('_id'):
            obj_id = son['_id']
            son['_id'] = str(obj_id)
        return son

