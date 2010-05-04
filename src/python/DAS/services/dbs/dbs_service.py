#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS service
"""
__revision__ = "$Id: dbs_service.py,v 1.17 2009/11/25 18:22:37 valya Exp $"
__version__ = "$Revision: 1.17 $"
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
        self.map = self.dasmapping.servicemap(self.name, 'javaservlet')
        map_validator(self.map)

    def parser(self, source, api, args=None):
        """
        DBS data-service parser.
        """
        notationmap = self.notations()
        notations = notationmap[''] # use api='', i.e. notations valid for all APIs
        if  notationmap.has_key(api):
            notations.update(notationmap[api])
        add = None
        if  api == 'listBlocks':
            tag = 'block'
        elif api == 'listFiles':
            tag = 'file'
        else:
            msg = 'DBSService::parser, unsupported %s API %s' \
                % (self.name, api)
            raise Exception(msg)
        gen = xml_parser(notations, source, tag, add)
        for row in gen:
            yield row
