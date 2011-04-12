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
        if  api == 'dataset4release_site':
            found = set()
            # call DBS to obtain dataset for given release
            dbs_url = url['dbs']
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
            # call Phedex to get site info for every dataset
            phedex_args = {'dataset':list(datasets), 
                            'node': '%s*' % args['site']}
            phedex_url = url['phedex']
            source, expire = \
            self.getdata(phedex_url, phedex_args, expire, headers, post=True)
            prim_key = 'block'
            tags = 'block.replica.node'
            for rec in xml_parser(source, prim_key, tags):
                block = DotDict(rec)._get('block.name')
                found_dataset = block.split('#')[0]
                if  found_dataset not in found:
                    found.add(found_dataset)
                    yield {'dataset':{'name':found_dataset, 
                                      'combined':['dbs', 'phedex']}}
            del datasets
            del found

    def apicall(self, query, url, api, args, dformat, expire):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        time0 = time.time()
        if  api == 'dataset4release_site':
            genrows = self.helper(url, api, args, expire)
        dasrows = self.set_misses(query, api, genrows)
        ctime = time.time() - time0
        try:
            url = "combined: %s" % url.values()
            self.write_to_cache(query, expire, url, api, args, dasrows, ctime)
        except:
            traceback.print_exc()
            self.logger.info('Fail to write_to_cache for combined service')
            pass
        
