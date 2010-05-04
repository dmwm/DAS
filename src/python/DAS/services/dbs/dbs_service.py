#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS service
"""
__revision__ = "$Id: dbs_service.py,v 1.15 2009/11/19 20:12:58 valya Exp $"
__version__ = "$Revision: 1.15 $"
__author__ = "Valentin Kuznetsov"

#import xml.etree.cElementTree as ET
import types
import xml.etree.ElementTree as ET

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

    def parser(self, data_ptr, api, args=None):
        """
        DBS data-service parser.
        """
        add = None
        if  api == 'listBlocks':
            tag = 'block'
        elif api == 'listFiles':
            tag = 'file'
        else:
            msg = 'DBSService::parser, unsupported %s API %s' \
                % (self.name, api)
            raise Exception(msg)
        gen = xml_parser(data_ptr, tag, add)
        for row in gen:
            yield row
