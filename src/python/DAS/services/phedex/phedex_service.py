#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Phedex service
"""
__revision__ = "$Id: phedex_service.py,v 1.11 2009/10/16 18:02:47 valya Exp $"
__version__ = "$Revision: 1.11 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator
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

    def parser(self, api, data, params=None):
        """
        Phedex JSON parser. Phedex uses the following dict:
        {'phedex': {'url':.., 'block':[]}
        """
        data  = json.loads(data)
        if  data.has_key('phedex'):
            data = data['phedex']
            if  api == 'blockReplicas':
                for block in data['block']:
                    row = dict(block=block)
                    self.row2das(self.name, row)
                    yield row
            elif api == 'fileReplicas':
                for block in data['block']:
                    fileinfo = block['file']
                    del block['file']
                    for file in fileinfo:
                        row = dict(file=file, block=block)
                        self.row2das(self.name, row)
#                        print "\n\n#### yield phedex fileReplicas row"
#                        print row
                        yield row
            elif api == 'nodes':
                for node in data['node']:
                    row = dict(site=node)
                    self.row2das(self.name, row)
                    yield row
            elif api == 'lfn2pfn':
                for item in data['mapping']:
                    row = dict(file=item)
                    self.row2das(self.name, row)
                    yield row
            else:
                msg = 'Unsupported phedex API %s' % api
                raise Exception(msg)
 
