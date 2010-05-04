#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS service
"""
__revision__ = "$Id: dbs_service.py,v 1.23 2010/02/25 14:53:49 valya Exp $"
__version__ = "$Revision: 1.23 $"
__author__ = "Valentin Kuznetsov"

import types

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, xml_parser

class DBSService(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dbs', config)
        self.reserved = ['api', 'apiversion']
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def parser(self, dformat, source, api):
        """
        DBS data-service parser.
        """
        if  api == 'listBlocks':
            prim_key = 'block'
        elif api == 'listFiles':
            prim_key = 'file'
        else:
            msg = 'DBSService::parser, unsupported %s API %s' \
                % (self.name, api)
            raise Exception(msg)
        gen = xml_parser(source, prim_key)
        for row in gen:
            yield row
