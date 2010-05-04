#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Phedex service
"""
__revision__ = "$Id: phedex_service.py,v 1.8 2009/07/22 20:40:11 valya Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, splitlist

class PhedexService(DASAbstractService):
    """
    Helper class to provide Phedex service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'phedex', config)
        self.map = {
            'blockReplicas' : {
                'keys': ['block', 'node', 'site', 
                         'block.size', 'block.numfiles', 'block.replica',
                         'block.custodial', 'block.is_open',
                         'block.complete', 'block.createdate', 'block.moddate'],
                'params' : {'se':'', 'block':'', 'node':''}
            },
#            'fileReplicas' : {
#                'keys': ['block', 'node', 'site',
#                         'block.size', 'block.numfiles',
#                         'file', 'file.checksum',
#                         'file.node', 'file.origin_node'],
#                'params' : {'se':'', 'block':'required', 'node':''}
#            },
            'fileReplicas' : {
                'keys': ['file', 'file.checksum',
                         'file.node', 'file.origin_node'],
                'params' : {'se':'', 'block':'required', 'node':''}
            },
            'nodes' : {
                'keys': ['site', 'node', 
                         'node.storage', 'node.kind'],
                'params' : {'node':'', 'noempty':''}
            },
            'lfn2pfn' : {
                'keys': ['file.pfn', 'file', 'node', 'file.protocol',
                         'file.custodial'],
                'params' : {'node':'required', 'lfn':'required', 
                            'destination':'', 'protocol':'srmv2'}
            },
            'tfc' : {
                'keys': ['tfc', 'tfc.protocol', 'tfc.element_name'],
                'params' : {'node':'required'}
            },
        }
        map_validator(self.map)

    def api(self, query, cond_dict=None):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        results = []
        if  cond_dict.has_key('block'):
            blocklist = cond_dict['block']

            msg = 'DAS::%s call api, len(blocks)=%s, will split' \
                % (self.name, len(blocklist))
            self.logger.info(msg)

            for blist in splitlist(blocklist, 100):
                msg = 'DAS::%s call api, send %s blocks' \
                        % (self.name, len(blist))
                self.logger.info(msg)
                params = dict(cond_dict)
                params['block'] = blist
                data = self.worker(query, params)
                results += data
        else:
            results = self.worker(query, cond_dict)
        return results

    def update_args(self, api, args, intdict):
        """
        Update args dict from intermediate dict of results (intdict)
        """
        if  api == 'lfn2pfn':
            # extrace lfn/node from fileReplicas
            lfn_list = []
            node_list = []
            for item in intdict['fileReplicas']['phedex']['block']:
                for files in item['file']:
                    lfn   = files['name']
                    if  lfn not in lfn_list:
                        lfn_list.append(lfn)
                    nodes = [i['node'] for i in files['replica']]
                    for node in nodes:
                        if  node not in node_list:
                            node_list.append(node)
            args['lfn'] = lfn_list
            args['node'] = node_list
        return args
