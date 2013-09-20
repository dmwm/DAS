#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=R0903
"""
File: das_singleton.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: DASCore singleton implementation
"""

from DAS.core.das_core import DASCore
from DAS.utils.utils import genkey

class _DASMgrSingleton(object):
    "DASCore Singleton class"
    def __init__(self):
        # just for the sake of information
        self.instance = "Instance at %d" % self.__hash__()
        self.params = {}

    def create(self, **kwargs):
        "Create DASCore object"
        dashash = genkey(str(kwargs))
        if  dashash in self.params:
            return self.params[dashash]
        else:
            das = DASCore(**kwargs)
            self.params[dashash] = das
            return das

DAS_MGR_SINGLETON = _DASMgrSingleton()

def das_singleton(**kwargs):
    "Return DASCore instance"
    dasinst = DAS_MGR_SINGLETON.create(**kwargs)
    return dasinst
