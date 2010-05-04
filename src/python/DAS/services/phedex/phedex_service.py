#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Phedex service
"""
__revision__ = "$Id: phedex_service.py,v 1.9 2009/09/01 01:42:46 valya Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator
import types
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json

class PhedexService(DASAbstractService):
    """
    Helper class to provide Phedex service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'phedex', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def row2das(self, system, row, cache):
        """Transform keys of row into DAS notations, e.g. bytes to size"""
        if  type(row) is not types.DictType:
            return
        for key, val in row.items():
            if  cache.has_key(key):
                newkey = cache[key]
            else:
                newkey = self.dasmapping.notation2das(system, key)
                cache[key] = newkey
            if  newkey != key:
                row[newkey] = row[key]
                del row[key]
            if  type(val) is types.DictType:
                self.row2das(system, val, cache)
            elif type(val) is types.ListType:
                for item in val:
                    if  type(item) is types.DictType:
                        self.row2das(system, item, cache)
 
    def parser(self, api, data, params=None):
        """
        Phedex JSON parser. Phedex uses the following dict:
        {'phedex': {'url':.., 'block':[]}
        """
        data  = json.loads(data)
        cache = {}
        if  data.has_key('phedex'):
            data = data['phedex']
            if  api == 'blockReplicas':
                for block in data['block']:
                    row = dict(block=block)
                    self.row2das(self.name, row, cache)
                    yield row
            elif api == 'fileReplicas':
                for block in data['block']:
                    fileinfo = block['file']
                    del block['file']
                    for file in fileinfo:
                        row = dict(file=file, block=block)
                        self.row2das(self.name, row, cache)
#                        print "\n\n#### yield phedex fileReplicas row"
#                        print row
                        yield row
            elif api == 'nodes':
                for node in data['node']:
                    row = dict(node=node)
                    self.row2das(self.name, row, cache)
                    yield row
            elif api == 'lfn2pfn':
                for item in data['mapping']:
                    row = dict(file=item)
                    self.row2das(self.name, row, cache)
                    yield row
            else:
                msg = 'Unsupported phedex API %s' % api
                raise Exception(msg)
 
