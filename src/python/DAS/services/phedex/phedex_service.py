#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Phedex service
"""
__revision__ = "$Id: phedex_service.py,v 1.16 2009/11/25 18:22:38 valya Exp $"
__version__ = "$Revision: 1.16 $"
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

    def clean_params(self, api, params):
        """
        Clean all parameters to get as much as possible information
        from Phedex. Skip only those which marked as required.
        """
        if  api in ['nodes']:
            args = {}
            for key, val in self.map[api]['params'].items():
                if  val != 'required':
                    args[key] = val
                else:
                    args[key] = params[key]
            return args
        else:
            return params

    def parser(self, source, api, args):
        """
        Phedex data-service parser.
        """
        notationmap = self.notations()
        notations = notationmap[''] # use api='', i.e. notations valid for all APIs
        if  notationmap.has_key(api):
            notations.update(notationmap[api])
        add = None
        if  api == 'blockReplicas':
            tag = 'block'
        elif api == 'fileReplicas':
            tag = 'file'
            add = 'block_name'
        elif api == 'nodes':
            tag = 'node'
        elif api == 'lfn2pfn':
            tag = 'mapping'
        else:
            msg = 'PhedexService::parser, unsupported %s API %s' \
                % (self.name, api)
            raise Exception(msg)
        gen = xml_parser(notations, source, tag, add)
        for row in gen:
            yield row
