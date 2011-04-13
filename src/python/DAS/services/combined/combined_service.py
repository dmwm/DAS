#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Combined DAS service
"""
__author__ = "Valentin Kuznetsov"

import re
import time
import traceback
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, xml_parser, qlxml_parser, DotDict

#
# NOTE:
# DBS3 will provide datasets API, once this API will support POST request
# and multiple datasets, I need to epxlore revert logic for combined_dataset4site
# API. First find all blocks at given site, then strip off dataset info
# and ask DBS to provide dataset info for found dataset.
#

class CombinedService(DASAbstractService):
    """
    Helper class to provide combined DAS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'combined', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def helper(self, url, api, args, expire):
        """
        Class helper function which yields results for given
        set of input parameters. It yeilds the data record which
        must contain combined attribute corresponding to systems
        used to produce record content.
        """
        if  api == 'combined_dataset4site_via_phedex':
            headers = {'Accept': 'text/xml'}
            if  args['site'].find('.') != -1: # it is SE
                phedex_args = {'se': '%s' % args['site']}
            else:
                phedex_args = {'node': '%s*' % args['site']}
            if  args.has_key('dataset') and args['dataset'] != 'optional':
                phedex_args.update({'dataset':args['dataset']})
            phedex_url = url['phedex']
            source, expire = \
            self.getdata(phedex_url, phedex_args, expire, headers)
            prim_key = 'block'
            tags = 'block.replica.node'
            found = {}
            for rec in xml_parser(source, prim_key, tags):
                ddict = DotDict(rec)
                block = ddict._get('block.name')
                bytes = ddict._get('block.bytes')
                cust  = ddict._get('block.replica.custodial')
                dataset = block.split('#')[0]
                tier = dataset.split('/')[-1].split('-')
                if  found.has_key(dataset):
                    val = found[dataset]
                    found[dataset] = {'bytes': val['bytes'] + bytes, 
                                        'custodial':cust, 'tier':tier}
                else:
                    found[dataset] = {'bytes':bytes, 'custodial':cust, 'tier':tier}
            for name, val in found.items():
                record = dict(name=name, combined=['dbs', 'phedex'],
                            size=val['bytes'], custodial=val['custodial'])
                yield {'dataset':record}
            del found
        if  api == 'combined_dataset4site_release':
            # call DBS to obtain dataset for given release
            dbs_url = url['dbs']
            # in DBS3 I'll use datasets API and pass release over there
            query = 'find dataset where release=%s' % args['release']
            dbs_args = {'api':'executeQuery', 'apiversion': 'DBS_2_0_9',\
                        'query':query}
            headers = {'Accept': 'text/xml'}
            source, expire = self.getdata(dbs_url, dbs_args, expire, headers)
            prim_key = 'dataset'
            datasets = set()
            for row in qlxml_parser(source, prim_key):
                dataset = row['dataset']['dataset']
                datasets.add(dataset)
            if  args['site'].find('.') != -1: # it is SE
                phedex_args = {'dataset':list(datasets), 
                                'se': '%s' % args['site']}
            else:
                phedex_args = {'dataset':list(datasets), 
                                'node': '%s*' % args['site']}
            phedex_url = url['phedex']
            source, expire = \
            self.getdata(phedex_url, phedex_args, expire, headers, post=True)
            prim_key = 'block'
            tags = 'block.replica.node'
            found = {}
            for rec in xml_parser(source, prim_key, tags):
                ddict = DotDict(rec)
                block = ddict._get('block.name')
                bytes = ddict._get('block.bytes')
                files = ddict._get('block.files')
                found_dataset = block.split('#')[0]
                if  found.has_key(found_dataset):
                    val = found[found_dataset]
                    found[found_dataset] = {'bytes': val['bytes'] + bytes,
                        'files': val['files'] + files}
                else:
                    found[found_dataset] = {'bytes': bytes, 'files': files}
            for name, val in found.items():
                record = dict(name=name, size=val['bytes'], files=val['files'],
                                combined=['dbs', 'phedex']) 
                yield {'dataset':record}
            del datasets
            del found

    def apicall(self, query, url, api, args, dformat, expire):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        time0 = time.time()
        if  api == 'combined_dataset4site_release':
            genrows = self.helper(url, api, args, expire)
        if  api == 'combined_dataset4site':
            msg = "--- combined rejects API %s, not enough arguments" % api
            self.logger.info(msg)
            error   = "The provided set of arguments is too loose, please add"
            error  += " release or dataset pattern to your query"
            record  = dict(name="N/A", error=error, combined=['dbs', 'phedex'])
            genrows = [dict(dataset=record)]
        dasrows = self.set_misses(query, api, genrows)
        ctime = time.time() - time0
        try:
            url = "combined: %s" % url.values()
            self.write_to_cache(query, expire, url, api, args, dasrows, ctime)
        except:
            traceback.print_exc()
            self.logger.info('Fail to write_to_cache for combined service')
            pass
        
