#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=R0914,R0904,R0912,R0915,R0913,W0702,W0703

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
- block_completion is a total number of blocks fully transferred to site X
  divided by total number of blocks at a site X
- replica_fraction (defined in services/phedex/phedex_service.py)
  is a total number of files at a site X
  divided by total number of in all block at this site

See discussion on
https://hypernews.cern.ch/HyperNews/CMS/get/comp-ops/1057/1/1/2/1/1.html
"""
__author__ = "Valentin Kuznetsov"

# system modules
import time
import urllib
try:
    import cStringIO as StringIO
except:
    import StringIO

# DAS modules
import DAS.utils.jsonwrapper as json
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, xml_parser, qlxml_parser
from DAS.utils.utils import json_parser, dastimestamp, get_key_cert
from DAS.utils.utils import print_exc
from DAS.utils.ddict import DotDict
from DAS.utils.global_scope import SERVICES
from DAS.utils.url_utils import getdata
from DAS.utils.regex import phedex_node_pattern, se_pattern, int_number_pattern
from DAS.utils.urlfetch_pycurl import getdata as urlfetch_getdata
from DAS.services.dbs3.dbs3_service import dbs_find as dbs3_find

CKEY, CERT = get_key_cert()

def parse_data(data):
    """
    Helper to parse input data
    """
    if  isinstance(data, basestring):
        data = StringIO.StringIO(data)
    try:
        jsondata = json.load(data)
    except Exception as exc:
        jsondata = []
        msg = 'Unable to apply json.load to "%s"' % data
        print msg
    if  isinstance(jsondata, dict):
        yield jsondata
    elif isinstance(jsondata, list):
        for row in jsondata:
            yield row

def phedex_files(phedex_url, kwds):
    "Get file information from Phedex"
    params = dict(kwds) # parameters to be send to Phedex
    site = kwds.get('site', None)
    if  site and phedex_node_pattern.match(site):
        if  not site.endswith('*'):
            # this will account to look-up site names w/o _Buffer or _MSS
            site += '*'
        params.update({'node': site})
        params.pop('site')
    elif site and se_pattern.match(site):
        params.update({'se': site})
        params.pop('site')
    else:
        return
    expire = 600 # set some expire since we're not going to use it
    headers = {'Accept': 'text/xml'}
    source, expire = \
        getdata(phedex_url, params, headers, expire, ckey=CKEY, cert=CERT,
                system='phedex')
    tags = 'block.file.name'
    prim_key = 'block'
    for rec in xml_parser(source, prim_key, tags):
        ddict = DotDict(rec)
        files = ddict.get('block.file')
        if  not isinstance(files, list):
            files = [files]
        for row in files:
            yield row['name']

def dbs_dataset4release_parent(dbs_url, release, parent=None):
    "Get dataset for given release and optional parent dataset"
    expire = 600 # set some expire since we're not going to use it
    # we call datasets?release=release to get list of datasets
    dbs_url += '/datasets'
    dbs_args = \
    {'release_version': release, 'dataset_access_type':'VALID'}
    if  parent:
        dbs_args.update({'parent_dataset': parent})
    headers = {'Accept': 'application/json;text/json'}
    source, expire = \
        getdata(dbs_url, dbs_args, headers, expire, ckey=CKEY, cert=CERT,
                system='dbs3')
    for rec in json_parser(source, None):
        for row in rec:
            yield row['dataset']

def dataset_summary(dbs_url, dataset):
    """
    Invoke DBS2/DBS3 call to get information about total
    number of filesi/blocks in a given dataset.
    """
    expire = 600 # set some expire since we're not going to use it
    # we call filesummaries?dataset=dataset to get number of files/blks
    dbs_url += '/filesummaries'
    dbs_args = {'dataset': dataset}
    headers = {'Accept': 'application/json;text/json'}
    source, expire = \
        getdata(dbs_url, dbs_args, headers, expire, ckey=CKEY, cert=CERT,
                system='dbs3')
    for row in json_parser(source, None):
        totfiles  = row[0]['num_file']
        totblocks = row[0]['num_block']
        return totblocks, totfiles

def site4dataset(dbs_url, phedex_api, args, expire):
    "Yield site information about given dataset"
    # DBS part
    dataset = args['dataset']
    try:
        totblocks, totfiles = dataset_summary(dbs_url, dataset)
    except Exception as err:
        error  = 'combined service unable to process your request'
        reason = "Fail to parse #block, #files info, %s" % str(err)
        yield {'site': {'name': 'N/A', 'se': 'N/A',
                        'error': error, 'reason': reason}}
        return
    # Phedex part
    phedex_args = {'dataset':args['dataset']}
    headers = {'Accept': 'text/xml'}
    source, expire = \
        getdata(phedex_api, phedex_args, headers, expire, post=True,
                system='phedex')
    prim_key = 'block'
    tags = 'block.replica.node'
    site_info = {}
    for rec in xml_parser(source, prim_key, tags):
        ddict = DotDict(rec)
        replicas = ddict.get('block.replica')
        if  not isinstance(replicas, list):
            replicas = [replicas]
        for row in replicas:
            if  not row or 'node' not in row:
                continue
            node = row['node']
            files = int(row['files'])
            complete = 1 if row['complete'] == 'y' else 0
            if  node in site_info:
                files = site_info[node]['files'] + files
                nblks  = site_info[node]['blocks'] + 1
                bc_val = site_info[node]['blocks_complete']
                b_complete = bc_val+1 if complete else bc_val
            else:
                b_complete = 1 if complete else 0
                nblks = 1
            site_info[node] = {'files': files, 'blocks': nblks,
                        'blocks_complete': b_complete}
    row = {}
    for key, val in site_info.iteritems():
        if  totfiles:
            nfiles = '%5.2f%%' % (100*float(val['files'])/totfiles)
        else:
            nfiles = 'N/A'
        if  totblocks:
            nblks  = '%5.2f%%' % (100*float(val['blocks'])/totblocks)
        else:
            nblks = 'N/A'
        ratio = float(val['blocks_complete'])/val['blocks']
        b_completion = '%5.2f%%' % (100*ratio)
        row = {'site':{'name':key, 'dataset_fraction': nfiles,
            'block_fraction': nblks, 'block_completion': b_completion}}
        yield row

class CombinedService(DASAbstractService):
    """
    Helper class to provide combined DAS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'combined', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)
        self.dbs = 'dbs3'

    def services(self):
        "Return data-services used by this class"
        return {self.name: [self.dbs, 'phedex']}

    def helper(self, api, args, expire):
        """
        Class helper function which yields results for given
        set of input parameters. It yeilds the data record which
        must contain combined attribute corresponding to systems
        used to produce record content.
        """
        dbs_url = self.map[api]['services'][self.dbs]
        phedex_url = self.map[api]['services']['phedex']
        # make phedex_api from url, but use xml version for processing
        phedex_api = phedex_url.replace('/json/', '/xml/') + '/blockReplicas'
        if  api == 'dataset4site_release' or \
            api == 'dataset4site_release_parent' or \
            api == 'child4site_release_dataset':
            # DBS part
            datasets = set()
            release = args['release']
            parent = args.get('parent', None)
            for row in dbs_dataset4release_parent(dbs_url, release, parent):
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
                getdata(phedex_api, phedex_args, headers, expire, post=True,
                        system='phedex')
            prim_key = 'block'
            tags = 'block.replica.node'
            found = {}
            for rec in xml_parser(source, prim_key, tags):
                ddict = DotDict(rec)
                block = ddict.get('block.name')
                bbytes = ddict.get('block.bytes')
                files = ddict.get('block.files')
                found_dataset = block.split('#')[0]
                if  found_dataset in found:
                    val = found[found_dataset]
                    found[found_dataset] = {'bytes': val['bytes'] + bbytes,
                        'files': val['files'] + files}
                else:
                    found[found_dataset] = {'bytes': bbytes, 'files': files}
            for name, val in found.iteritems():
                record = dict(name=name, size=val['bytes'], files=val['files'])
                if  api == 'child4site_release_dataset':
                    yield {'child': record}
                else:
                    yield {'dataset':record}
            del datasets
            del found
        if  api == 'site4dataset':
            try:
                gen = site4dataset(dbs_url, phedex_api, args, expire)
                for row in gen:
                    yield row
            except Exception as err:
                print_exc(err)
                tstamp = dastimestamp('')
                msg  = tstamp + ' Exception while processing DBS/Phedex info:'
                msg += str(err)
                row = {'site':{'name':'Fail to look-up site info',
                    'error':msg, 'dataset_fraction': 'N/A',
                    'block_fraction':'N/A', 'block_completion':'N/A'},
                    'error': msg}
                yield row
        if  api == 'files4dataset_runs_site' or \
            api == 'files4block_runs_site':
            run_value = args.get('run', [])
            if  isinstance(run_value, dict) and '$in' in run_value:
                runs = run_value['$in']
            elif isinstance(run_value, list):
                runs = run_value
            else:
                if  int_number_pattern.match(str(run_value)):
                    runs = [run_value]
                else:
                    runs = []
            args.update({'runs': runs})
            files = dbs_find('file', dbs_url, args)
            site  = args.get('site')
            phedex_api = phedex_url.replace('/json/', '/xml/') + '/fileReplicas'
            for fname in files4site(phedex_api, files, site):
                yield {'file':{'name':fname}}

    def apicall(self, dasquery, url, api, args, dformat, expire):
        """
        A service worker. It parses input query, invoke service API
        and return results in a list with provided row.
        """
        # NOTE: I use helper function since it is 2 step process
        # therefore the expire time stamp will not be changed, since
        # helper function will yield results
        time0 = time.time()
        if  api == 'dataset4site_release' or \
            api == 'site4dataset' or 'files4dataset_runs_site':
            genrows = self.helper(api, args, expire)
        # here I use directly the call to the service which returns
        # proper expire timestamp. Moreover I use HTTP header to look
        # at expires and adjust my expire parameter accordingly
# NOTE: disable dataset4site, lumi4site since they take too much load
#       see combined.yml
#        if  api == 'dataset4site':
#            headers = {'Accept': 'application/json;text/json'}
#            datastream, expire = \
#                    getdata(url, args, headers, expire, system='combined')
#            genrows = parse_data(datastream)
#        if  api == 'lumi4dataset':
#            headers = {'Accept': 'application/json;text/json'}
#            data, expire = \
#                    getdata(url, args, headers, expire, system='combined')
#            genrows = json_parser(data, None)

        # proceed with standard workflow
        ctime   = time.time() - time0
        try:
            if  isinstance(url, dict):
                url = "combined: %s" % url.values()
            self.write_to_cache(dasquery, expire, url, api, \
                    args, genrows, ctime)
        except Exception as exc:
            print_exc(exc)

def dbs_find(entity, url, kwds):
    "Find given DBS entity for given set of parameters"
    gen = dbs3_find(entity, url, kwds)
    for row in gen:
        yield row

def files4site(phedex_url, files, site):
    "Find site for given files"

    params = {}
    if  site and phedex_node_pattern.match(site):
        if  not site.endswith('*'):
            # this will account to look-up site names w/o _Buffer or _MSS
            site += '*'
        params.update({'node': site})
    elif site and se_pattern.match(site):
        params.update({'se': site})
    else:
        return
    sname = urllib.urlencode(params)
    urls = []
    for fname in files:
        url = '%s?lfn=%s&%s' % (phedex_url, fname, sname)
        urls.append(url)
    tags = 'block.replica.node'
    prim_key = 'block'
    gen = urlfetch_getdata(urls, CKEY, CERT)
    for rec in gen:
        if  'error' in rec.keys():
            # TODO: should handle the error
            pass
        else:
            # convert record string into StringIO for xml_parser
            source = StringIO.StringIO(rec['data'])
            for row in xml_parser(source, prim_key, tags):
                fobj = row['block']['file']
                fname = fobj['name']
                replica = fobj['replica']
                for item in replica:
                    yield fname
                    break # only need to yield once since its replica
