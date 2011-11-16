#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Combined DAS service (DBS+Phedex). It can be used to get
information which is divided between two services. For example
to find dataset at given site and release name or
find out dataset presence on a given site.

We use the following definitions for dataset presence:
- dataset_fraction is defined as a number of files at a site X
  divided by total number of files in a dataset
- block_fraction is a total number of blocks at a site X
  divided by total number of blocks in a dataset
"""
__author__ = "Valentin Kuznetsov"

import time
import DAS.utils.jsonwrapper as json
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, xml_parser, qlxml_parser
from DAS.utils.utils import json_parser
from DAS.utils.ddict import DotDict
from DAS.utils.utils import expire_timestamp, print_exc

#
# NOTE:
# DBS3 will provide datasets API, once this API will support POST request
# and multiple datasets, I need to epxlore revert logic for combined_dataset4site
# API. First find all blocks at given site, then strip off dataset info
# and ask DBS to provide dataset info for found dataset.
#

def parse_data(data):
    """
    Helper to parse input data
    """

    for item in json.load(data):
        if  isinstance(item, list):
            for row in item:
                yield row
        else:
            yield item

def which_dbs(dbs_url):
    """Determine DBS version based on given DBS url"""
    if  dbs_url.find('servlet') != -1:
        return 'dbs2'
    return 'dbs3'

def dbs_dataset4site_release(dbs_url, getdata, release):
    expire = 600 # set some expire since we're not going to use it
    if  which_dbs(dbs_url) == 'dbs2':
        # in DBS3 I'll use datasets API and pass release over there
        query = 'find dataset where release=%s' % release
        dbs_args = {'api':'executeQuery', 'apiversion': 'DBS_2_0_9', \
                    'query':query}
        headers = {'Accept': 'text/xml'}
        source, expire = getdata(dbs_url, dbs_args, expire, headers)
        prim_key = 'dataset'
        datasets = set()
        for row in qlxml_parser(source, prim_key):
            dataset = row['dataset']['dataset']
            yield dataset
    else:
        # we call datasets?release=release to get list of datasets
        dbs_args = \
        {'release_version': release, 'dataset_access_type':'PRODUCTION'}
        headers = {'Accept': 'application/json;text/json'}
        source, expire = getdata(dbs_url, dbs_args, expire, headers)
        for rec in json_parser(source, None):
            for row in rec:
                yield row['dataset']

def dataset_summary(dbs_url, getdata, dataset):
    """
    Invoke DBS2/DBS3 call to get information about total
    number of filesi/blocks in a given dataset.
    """
    expire = 600 # set some expire since we're not going to use it
    if  which_dbs(dbs_url) == 'dbs2':
        # DBS2 call
        query = 'find count(file.name), count(block.name) where dataset=%s'\
                 % dataset
        dbs_args = {'api':'executeQuery', 'apiversion': 'DBS_2_0_9', \
                    'query':query}
        headers = {'Accept': 'text/xml'}
        source, expire = getdata(dbs_url, dbs_args, expire, headers)
        prim_key = 'dataset'
        datasets = set()
        for row in qlxml_parser(source, prim_key):
            totfiles  = row['dataset']['count_file.name']
            totblocks = row['dataset']['count_block.name']
            return totblocks, totfiles
    else:
        # we call filesummaries?dataset=dataset to get number of files/blks
        dbs_args = {'dataset': dataset}
        headers = {'Accept': 'application/json;text/json'}
        source, expire = getdata(dbs_url, dbs_args, expire, headers)
        for row in json_parser(source, None):
            totfiles  = row[0]['num_file']
            totblocks = row[0]['num_block']
            return totblocks, totfiles

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
        dbs_url = url['dbs']
        phedex_url = url['phedex']
        if  api == 'combined_dataset4site_release':
            # DBS part
            datasets = set()
            for row in dbs_dataset4site_release(dbs_url, self.getdata, args['release']):
                datasets.add(row)
            # Phedex part
            if  args['site'].find('.') != -1: # it is SE
                phedex_args = {'dataset':list(datasets), 
                                'se': '%s' % args['site']}
            else:
                phedex_args = {'dataset':list(datasets), 
                                'node': '%s*' % args['site']}
            headers = {'Accept': 'text/xml'}
            source, expire = \
            self.getdata(phedex_url, phedex_args, expire, headers, post=True)
            prim_key = 'block'
            tags = 'block.replica.node'
            found = {}
            for rec in xml_parser(source, prim_key, tags):
                ddict = DotDict(rec)
                block = ddict.get('block.name')
                bbytes = ddict.get('block.bytes')
                files = ddict.get('block.files')
                found_dataset = block.split('#')[0]
                if  found.has_key(found_dataset):
                    val = found[found_dataset]
                    found[found_dataset] = {'bytes': val['bytes'] + bbytes,
                        'files': val['files'] + files}
                else:
                    found[found_dataset] = {'bytes': bbytes, 'files': files}
            for name, val in found.items():
                record = dict(name=name, size=val['bytes'], files=val['files'],
                                combined=['dbs', 'phedex']) 
                yield {'dataset':record}
            del datasets
            del found
        if  api == 'combined_site4dataset':
            # DBS part
            dataset = args['dataset']
            totblocks, totfiles = \
                dataset_summary(dbs_url, self.getdata, dataset)
            # Phedex part
            phedex_args = {'dataset':args['dataset']}
            headers = {'Accept': 'text/xml'}
            source, expire = \
            self.getdata(phedex_url, phedex_args, expire, headers, post=True)
            prim_key = 'block'
            tags = 'block.replica.node'
            found = {}
            site_info = {}
            for rec in xml_parser(source, prim_key, tags):
                ddict = DotDict(rec)
                for row in ddict.get('block.replica'):
                    node = row['node']
                    files = int(row['files'])
                    if  site_info.has_key(node):
                        nfiles = site_info[node]['files'] + files
                        nblks  = site_info[node]['blocks'] + 1
                        site_info[node] = {'files': nfiles, 'blocks': nblks}
                    else:
                        site_info[node] = {'files': files, 'blocks': 1}
            row = {}
            for key, val in site_info.items():
                if  totfiles:
                    nfiles = \
                    '%5.2f%%' % (100*float(site_info[key]['files'])/totfiles)
                else:
                    nfiles = 'N/A'
                if  totblocks:
                    nblks  = \
                    '%5.2f%%' % (100*float(site_info[key]['blocks'])/totblocks)
                else:
                    nblks = 'N/A'
                row = {'site':{'name':key, 'dataset_fraction': nfiles,
                        'block_fraction': nblks}}
                yield row

    def apicall(self, query, url, api, args, dformat, expire):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        # NOTE: I use helper function since it is 2 step process
        # therefore the expire time stamp will not be changed, since
        # helper function will yield results
        time0 = time.time()
        if  api == 'combined_dataset4site_release' or \
            api == 'combined_site4dataset':
            genrows = self.helper(url, api, args, expire)
        # here I use directly the call to the service which returns
        # proper expire timestamp. Moreover I use HTTP header to look
        # at expires and adjust my expire parameter accordingly
        if  api == 'combined_dataset4site':
            headers = {'Accept': 'application/json;text/json'}
            datastream, expire = self.getdata(url, args, expire, headers)
            try: # get HTTP header and look for Expires
                e_time = expire_timestamp(\
                    datastream.info().__dict__['dict']['expires'])
                if  e_time > time.time():
                    expire = e_time
            except:
                pass
            genrows = parse_data(datastream)

        # proceed with standard workflow
        dasrows = self.set_misses(query, api, genrows)
        ctime   = time.time() - time0
        try:
            if  isinstance(url, dict):
                url = "combined: %s" % url.values()
            self.write_to_cache(query, expire, url, api, args, dasrows, ctime)
        except Exception as exc:
            print_exc(exc)
