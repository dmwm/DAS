#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Phedex service
"""
__author__ = "Valentin Kuznetsov"

import socket

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, xml_parser
import types
import DAS.utils.jsonwrapper as json

def get_replica_info(replica):
    """
    Get replica info: name, se, ip
    """
    result = {'name': replica['node'], 'se': replica['se']}
    try:
        result.update({'ip': socket.gethostbyname(replica['se'])})
    except:
        pass
    return result

def site_info(site_dict, block, replica):
    """Helper function to fill out site info"""
    node  = replica['node']
    files = replica['files']
    totfiles = block['files']
    if  site_dict.has_key(node):
        vdict = site_dict[node]
        vdict['files'] += files
        vdict['totfiles'] += totfiles
        site_dict[node] = vdict
    else:
        site_dict[node] = dict(files=files, totfiles=totfiles)

class PhedexService(DASAbstractService):
    """
    Helper class to provide Phedex service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'phedex', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)
        self.notationmap = self.notations()

    def adjust_params(self, api, kwds, inst=None):
        """
        Adjust Phedex parameters for specific query requests
        """
        if  api.find('blockReplicas') != -1 or \
            api.find('fileReplicas') != -1:
            for key, val in kwds.items():
                if  val == '*':
                    del kwds[key]
        if  kwds.has_key('node') and kwds['node'].find('*') == -1:
            if  not api == 'tfc':
                kwds['node'] = kwds['node'] + '*'

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
        elif api == 'site4file':
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
        elif api == 'tfc':
            prim_key = 'storage-mapping'
        else:
            msg = 'PhedexService::parser, unsupported %s API %s' \
                % (self.name, api)
            raise Exception(msg)
        gen = xml_parser(source, prim_key, tags)
        site_names = []
        seen = set()
        tot_files  = 0
        site_info_dict = {}
        for row in gen:
            if  api == 'site4dataset' or api == 'site4block':
                item = row['block']['replica']
                if  isinstance(item, list):
                    for replica in item:
                        result = get_replica_info(replica)
                        site_info(site_info_dict, row['block'], replica)
                        if  not replica['files']:
                            continue
                        if  result not in site_names:
                            site_names.append(result)
                elif isinstance(item, dict):
                    replica = item
                    result = get_replica_info(replica)
                    site_info(site_info_dict, row['block'], replica)
                    if  not replica['files']:
                        continue
                    result = get_replica_info(replica)
                    if  result not in site_names:
                        site_names.append(result)
            elif api == 'site4file':
                item = row['block']['file']['replica']
                if  isinstance(item, list):
                    for replica in item:
                        result = get_replica_info(replica)
                        if  result not in site_names:
                            site_names.append(result)
                elif isinstance(item, dict):
                    replica = item
                    result = get_replica_info(replica)
                    if  result not in site_names:
                        site_names.append(result)
            elif  api == 'dataset4site' or api == 'dataset4se':
                dataset = row['block']['name'].split('#')[0]
                seen.add(dataset)
            else:
                if  row.has_key('node') and row['node'].has_key('se'):
                    try:
                        row['node']['ip'] = \
                        socket.gethostbyname(row['node']['se'])
                    except:
                        pass
                yield row
        if  api == 'site4dataset' or api == 'site4block':
            for row in site_names:
                name = row['name']
                if  site_info_dict.has_key(name):
                    sdict      = site_info_dict[name]
                    sfiles     = float(sdict['files'])
                    tot_files  = float(sdict['totfiles'])
                    file_occ   = '%5.2f%%' % (100*sfiles/tot_files)
                else:
                    file_occ   = '0%%'
                row['file_fraction'] = file_occ.strip()
                yield row
        if  api == 'site4file':
            for row in site_names:
                yield row
        del site_names
        del site_info_dict
        if  seen:
            for dataset in seen:
                yield {'dataset':dict(name=dataset)}
        del seen
