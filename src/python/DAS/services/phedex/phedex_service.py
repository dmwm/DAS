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
        if  api.find('blockReplicas') != -1 or \
            api.find('fileReplicas') != -1:
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
        elif api == 'fileReplicas4dataset':
            prim_key = 'file'
            tags = 'block.name'
        elif api == 'fileReplicas4file':
            prim_key = 'file'
            tags = 'block.name'
        elif api == 'dataset4site':
            prim_key = 'block'
            tags = 'block'
        elif api == 'dataset4se':
            prim_key = 'block'
            tags = 'block'
        elif api == 'site4dataset':
            prim_key = 'block'
            tags = 'block.replica.node'
        elif api == 'site4block':
            prim_key = 'block'
            tags = 'block.replica.node'
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
        site_names = []
        seen = set()
        for row in gen:
            if  api == 'site4dataset' or api == 'site4block':
                if  isinstance(row['block']['replica'], list):
                    for replica in row['block']['replica']:
                        result = {'name': replica['node'], 'se': replica['se']}
                        if  result not in site_names:
                            site_names.append(result)
                elif isinstance(row['block']['replica'], dict):
                    replica = row['block']['replica']
                    result  = {'name': replica['node'], 'se': replica['se']}
                    if  result not in site_names:
                        site_names.append(result)
            elif  api == 'dataset4site' or api == 'dataset4se':
                dataset = row['block']['name'].split('#')[0]
                if  dataset not in seen:
                    seen.add(dataset)
                    yield {'dataset':{'name':dataset}}
            else:
                yield row
        if  api == 'site4dataset' or api == 'site4block':
            yield site_names
        del site_names
        del seen
