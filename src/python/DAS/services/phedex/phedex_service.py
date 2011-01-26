#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Phedex service
"""
__revision__ = "$Id: phedex_service.py,v 1.21 2010/02/25 14:53:48 valya Exp $"
__version__ = "$Revision: 1.21 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, xml_parser
import types
import DAS.utils.jsonwrapper as json

class PhedexService(DASAbstractService):
    """
    Helper class to provide Phedex service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'phedex', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)
        self.notationmap = self.notations()

    def adjust_params(self, api, kwds):
        """
        Adjust Phedex parameters for specific query requests
        """
        if  api == 'blockReplicas':
            for key, val in kwds.items():
                if  val == '*':
                    del kwds[key]

    def parser(self, query, dformat, source, api):
        """
        Phedex data-service parser.
        """
        tags = []
        if  api == 'blockReplicas':
            prim_key = 'block'
        elif api == 'fileReplicas':
            prim_key = 'file'
            tags = 'block.name'
        elif api == 'nodes':
            prim_key = 'node'
        elif api == 'nodeusage':
            prim_key = 'node'
        elif api == 'groups':
            prim_key = 'group'
        elif api == 'groupusage':
            prim_key = 'node'
        elif api == 'lfn2pfn':
            prim_key = 'mapping'
        else:
            msg = 'PhedexService::parser, unsupported %s API %s' \
                % (self.name, api)
            raise Exception(msg)
        gen = xml_parser(source, prim_key, tags)
        for row in gen:
            yield row
