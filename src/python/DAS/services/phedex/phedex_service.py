#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Phedex service
"""
__revision__ = "$Id: phedex_service.py,v 1.4 2009/04/21 22:11:59 valya Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import query_params, map_validator, splitlist

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
            'fileReplicas' : {
                'keys': ['block', 'node', 'site',
                         'block.size', 'block.numfiles',
                         'file', 'file.checksum',
                         'file.node', 'file.origin_node'],
                'params' : {'se':'', 'block':'', 'node':''}
            },
            'nodes' : {
                'keys': ['site', 'node', 
                         'node.storage', 'node.kind'],
                'params' : {'node':'', 'noempty':''}
            },
#            'lfn2pfn' : {
#                'keys': ['file.pfn', 'file', 'node', 'file.protocol',
#                         'file.custodial'],
#                'params' : {'node':'', 'lfn':'', 'destination':'', 'protocol':'srmv2'}
#            },
#            'tfc' : {
#                'keys': ['tfc', 'tfc.protocol', 'tfc.element_name'],
#                'params' : {'node':''}
#            },
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

    def worker_local(self, query, cond_dict=None):
#    def worker(self, query, cond_dict=None):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        selkeys, cond = query_params(query)
        params = {}
        for key in cond.keys():
            oper, val = cond[key]
            if  oper == '=':
                params[key] = val
            else:
                raise Exception("DAS::%s, not supported operator '%s'" \
                % (self.name, oper))
        if  params.has_key('site'):
            val = params['site']
            params['se'] = val
            del params['site']
        for key in cond_dict: # phedex uses se rather then site key
            if  key == 'site':
                name = 'se'
            else:
                name = key
            params[name] = cond_dict[key]

        apiname = ""
        args = {}
        # check if all requested keys are covered by one API
        for api, aparams in self.map.items():
            if  set(selkeys) & set(aparams['keys']) == set(selkeys):
                apiname = api
                for par in aparams['params']:
                    if  params.has_key(par):
                        args[par] = params[par]
                url = self.url + '/' + apiname
                res = self.getdata(url, args)
                res = res.replace('null', '\"null\"')
                jsondict = eval(res)
                if  jsondict.has_key('error'):
                    continue
                data = getattr(self, 'parser_%s' % apiname)(jsondict)
                return data

        # if one API doesn't cover sel keys, will call multiple APIs
        apidict = {}
        for key in selkeys:
            for api, aparams in self.map.items():
                if  aparams['keys'].count(key) and not apidict.has_key(api):
                    args = {}
                    for par in aparams['params']:
                        if  params.has_key(par):
                            args[par] = params[par]
                    apidict[api] = args
        rel_keys = []
        resdict  = {}
        for api, args in apidict.items():
            url = self.url + '/' + api
            res = self.getdata(url, args)
            res = res.replace('null', '\"null\"')
            jsondict = eval(res)
            data = getattr(self, 'parser_%s' % api)(jsondict)
            resdict[api] = data
            first_row = data[0]
            keys = first_row.keys()
            if  not rel_keys:
                rel_keys = set(list(keys))
            else:
                rel_keys = rel_keys & set(keys)
        data = self.product(resdict, rel_keys)
        return data

    def parser_blockReplicas(self, jsondict):
        """
        Parser of output for blockReplicas phedex API
        """
        data = []
        blocklist = jsondict['phedex']['block']
        newrow = {}
        for item in blocklist:
            row = dict(newrow)
            row = {'block':item['name'], 'block.size':item['bytes'], 
                   'block.numfiles':item['files'], 
                   'block.is_open':item['is_open']}
            for rep in item['replica']:
                row['node'] = rep['node']
                row['site'] = rep['se']
                row['block.custodial'] = rep['custodial']
                row['block.createdate'] = rep['time_create']
                row['block.moddate'] = rep['time_update']
                row['block.complete'] = rep['complete']
                data.append(row)
        return data

    def parser_fileReplicas(self, jsondict):
        """
        Parser of output for fileReplicas phedex API
        """
        data = []
        blocklist = jsondict['phedex']['block']
        newrow = {}
        for item in blocklist:
            row = dict(newrow)
            row = {'block':item['name'], 'block.size':item['bytes'], 
                   'block.numfiles':item['files']}
            for elem in item['file']:
                row['file'] = elem['name']
                row['file.checksum'] = elem['checksum']
                row['file.size'] = elem['bytes']
                row['file.createdate'] = elem['time_create']
                row['file.origin_node'] = elem['origin_node']
                for rep in elem['replica']:
                    row['node'] = rep['node']
                    row['site'] = rep['se']
                    data.append(row)
        return data

    def parser_nodes(self, jsondict):
        """
        Parser of output for blockReplicas phedex API
        """
        data = []
        nodelist = jsondict['phedex']['node']
        newrow = {}
        for item in nodelist:
            row = dict(newrow)
            row['node'] = item['name']
            row['site'] = item['se']
            row['node.kind'] = item['kind']
            row['node.storage'] = item['technology']
            data.append(row)
        return data

    def parser_lfn2pfn(self, jsondict):
        """
        Parser of output for blockReplicas phedex API
        """
        data = []
        filelist = jsondict['phedex']['mapping']
        newrow = {}
        for item in filelist:
            row = dict(newrow)
            row['file'] = item['lfn']
            row['file.pfn'] = item['pfn']
            row['file.protocol'] = item['protocol']
            row['file.custodial'] = item['custodial']
            row['node'] = item['node']
            data.append(row)
        return data

    def parser_tfc(self, jsondict):
        """
        Parser of output for blockReplicas phedex API
        """
        data = []
        filelist = jsondict['phedex']['storage-mapping']['array']
        newrow = {}
        for item in filelist:
            row = dict(newrow)
            row['tfc.protocol'] = item['protocol']
            row['tfc.element_name'] = item['element_name']
            row['tfc'] = item['result']
            data.append(row)
        return data

