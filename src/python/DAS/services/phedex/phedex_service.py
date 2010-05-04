#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Phedex service
"""
__revision__ = "$Id: phedex_service.py,v 1.13 2009/11/16 16:08:06 valya Exp $"
__version__ = "$Revision: 1.13 $"
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

    def parser(self, data_ptr, api, args):
        """
        Phedex data-service parser.
        """
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
            msg = 'Unsupported %s API %s' % (self.name, api)
            raise Exception(msg)
        gen = xml_parser(data_ptr, tag, add)
        for row in gen:
            yield row
