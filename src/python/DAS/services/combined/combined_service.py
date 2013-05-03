#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=R0914,R0904,R0912,R0915,R0913,W0702,W0703

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
from DAS.utils.utils import json_parser, get_key_cert, dastimestamp
from DAS.utils.utils import print_exc, convert2ranges
from DAS.utils.ddict import DotDict
from DAS.utils.global_scope import SERVICES
from DAS.utils.url_utils import getdata
from DAS.utils.regex import site_pattern, se_pattern, int_number_pattern
from DAS.utils.urlfetch_pycurl import getdata as urlfetch_getdata

#
# NOTE:
# DBS3 will provide datasets API, once this API will support POST request
# and multiple datasets, I need to epxlore revert logic for dataset4site
# API. First find all blocks at given site, then strip off dataset info
# and ask DBS to provide dataset info for found dataset.
#

CKEY, CERT = get_key_cert()

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
        return 'dbs'
    return 'dbs3'

def phedex_files(phedex_url, kwds):
    "Get file information from Phedex"
    params = dict(kwds) # parameters to be send to Phedex
    site = kwds.get('site', None)
    if  site and site_pattern.match(site):
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
        getdata(phedex_url, params, headers, expire, ckey=CKEY, cert=CERT)
    tags = 'block.file.name'
    prim_key = 'block'
    for rec in xml_parser(source, prim_key, tags):
        ddict = DotDict(rec)
        files = ddict.get('block.file')
        if  not isinstance(files, list):
            files = [files]
        for row in files:
            yield row['name']

def dbs_dataset4site_release(dbs_url, release):
    "Get dataset for given site and release"
    expire = 600 # set some expire since we're not going to use it
    if  which_dbs(dbs_url) == 'dbs':
        # in DBS3 I'll use datasets API and pass release over there
        query = 'find dataset where release=%s' % release
        dbs_args = {'api':'executeQuery', 'apiversion': 'DBS_2_0_9', \
                    'query':query}
        headers = {'Accept': 'text/xml'}
        source, expire = \
            getdata(dbs_url, dbs_args, headers, expire, ckey=CKEY, cert=CERT)
        prim_key = 'dataset'
        for row in qlxml_parser(source, prim_key):
            if  row.has_key('dataset'):
                dataset = row['dataset']['dataset']
                yield dataset
            elif row.has_key('error'):
                err = row.get('reason', None)
                err = err if err else row['error']
                yield 'DBS error: %s' % err
    else:
        # we call datasets?release=release to get list of datasets
        dbs_url += '/datasets'
        dbs_args = \
        {'release_version': release, 'dataset_access_type':'VALID'}
        headers = {'Accept': 'application/json;text/json'}
        source, expire = \
            getdata(dbs_url, dbs_args, headers, expire, ckey=CKEY, cert=CERT)
        for rec in json_parser(source, None):
            for row in rec:
                yield row['dataset']

def dataset_summary(dbs_url, dataset):
    """
    Invoke DBS2/DBS3 call to get information about total
    number of filesi/blocks in a given dataset.
    """
    expire = 600 # set some expire since we're not going to use it
    if  which_dbs(dbs_url) == 'dbs':
        # DBS2 call
        query  = 'find count(file.name), count(block.name)'
        query += ' where dataset=%s and dataset.status=*' % dataset
        dbs_args = {'api':'executeQuery', 'apiversion': 'DBS_2_0_9', \
                    'query':query}
        headers = {'Accept': 'text/xml'}
        source, expire = \
            getdata(dbs_url, dbs_args, headers, expire, ckey=CKEY, cert=CERT)
        prim_key = 'dataset'
        for row in qlxml_parser(source, prim_key):
            if  row.has_key('dataset'):
                totfiles  = row['dataset']['count_file.name']
                totblocks = row['dataset']['count_block.name']
                return totblocks, totfiles
            elif row.has_key('error'):
                raise Exception(row.get('reason', row['error']))
        # if we're here we didn't find a dataset, throw the error
        msg = 'empty set'
        raise Exception(msg)
    else:
        # we call filesummaries?dataset=dataset to get number of files/blks
        dbs_url += '/filesummaries'
        dbs_args = {'dataset': dataset}
        headers = {'Accept': 'application/json;text/json'}
        source, expire = \
            getdata(dbs_url, dbs_args, headers, expire, ckey=CKEY, cert=CERT)
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
        error  = str(err)
        reason = "Can't find #block, #files info in DBS for dataset=%s" \
                % dataset
        yield {'site': {'error': error, 'reason': reason}}
        return
    # Phedex part
    phedex_args = {'dataset':args['dataset']}
    headers = {'Accept': 'text/xml'}
    source, expire = \
        getdata(phedex_api, phedex_args, headers, expire, post=True)
    prim_key = 'block'
    tags = 'block.replica.node'
    site_info = {}
    for rec in xml_parser(source, prim_key, tags):
        ddict = DotDict(rec)
        replicas = ddict.get('block.replica')
        if  not isinstance(replicas, list):
            replicas = [replicas]
        for row in replicas:
            if  not row or not row.has_key('node'):
                continue
            node = row['node']
            files = int(row['files'])
            complete = 1 if row['complete'] == 'y' else 0
            if  site_info.has_key(node):
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

def process_lumis_with(ikey, gen):
    "Helper function to process lumis with given key from provided generator"
    odict = {}
    for row in gen:
        lfn, run, lumi = row
        if  ikey == 'file':
            key = lfn
        if  ikey == 'run':
            key = run
        if  isinstance(lumi, list):
            for ilumi in lumi:
                odict.setdefault(key, []).append(ilumi)
        else:
            odict.setdefault(key, []).append(ilumi)
    for key, lumi_list in odict.iteritems():
        lumis = convert2ranges(lumi_list)
        if  ikey == 'file':
            yield {'file':{'name':key}, 'lumi':{'number':lumis}}
        if  ikey == 'run':
            yield {'run':{'run_number':key}, 'lumi':{'number':lumis}}

class CombinedService(DASAbstractService):
    """
    Helper class to provide combined DAS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'combined', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def helper(self, api, args, expire):
        """
        Class helper function which yields results for given
        set of input parameters. It yeilds the data record which
        must contain combined attribute corresponding to systems
        used to produce record content.
        """
        if  SERVICES.has_key('dbs'):
            dbs = 'dbs'
        elif SERVICES.has_key('dbs3'):
            dbs = 'dbs3'
        else:
            raise Exception('Unsupport DBS system')
        dbs_url = self.map[api]['services'][dbs]
        phedex_url = self.map[api]['services']['phedex']
        # make phedex_api from url, but use xml version for processing
        phedex_api = phedex_url.replace('/json/', '/xml/') + '/blockReplicas'
        if  api == 'dataset4site_release':
            # DBS part
            datasets = set()
            for row in dbs_dataset4site_release(dbs_url, args['release']):
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
                getdata(phedex_api, phedex_args, headers, expire, post=True)
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
            for name, val in found.iteritems():
                record = dict(name=name, size=val['bytes'], files=val['files'],
                                combined=[which_dbs(dbs_url), 'phedex'])
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
            if  isinstance(run_value, dict) and run_value.has_key('$in'):
                runs = run_value['$in']
            elif isinstance(run_value, list):
                runs = run_value
            else:
                if  int_number_pattern.match(run_value):
                    runs = [run_value]
                else:
                    runs = []
            args.update({'runs': runs})
            files = dbs_find('file', dbs_url, args)
            site  = args.get('site')
            phedex_api = phedex_url.replace('/json/', '/xml/') + '/fileReplicas'
            for fname in files4site(phedex_api, files, site):
                yield {'file':{'name':fname}}
        if  api == 'run_lumi4dataset' or api == 'run_lumi4block' or \
            api == 'file_lumi4dataset' or api == 'file_lumi4block' or \
            api == 'file_run_lumi4dataset' or api == 'file_run_lumi4block':
            run_value = args.get('run', [])
            if  isinstance(run_value, dict) and run_value.has_key('$in'):
                runs = run_value['$in']
            elif isinstance(run_value, list):
                runs = run_value
            else:
                if  int_number_pattern.match(run_value):
                    runs = [run_value]
                else:
                    runs = []
            args.update({'runs': runs})
            blocks = dbs_find('block', dbs_url, args)
            gen = file_run_lumis(dbs_url, blocks, runs)
            if  api.startswith('run_lumi'):
                for row in process_lumis_with('run', gen):
                    yield row
            if  api.startswith('file_lumi'):
                for row in process_lumis_with('file', gen):
                    yield row
            if  api.startswith('file_run_lumi'):
                for row in file_run_lumis(dbs_url, blocks, runs):
                    lfn, run, lumi = row
                    yield {'run':{'run_number':run}, 'lumi':{'number':lumi},
                           'file':{'name': lfn}}

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
        if  api == 'dataset4site':
            headers = {'Accept': 'application/json;text/json'}
            datastream, expire = getdata(url, args, headers, expire)
            genrows = parse_data(datastream)
        if  api == 'lumi4dataset':
            headers = {'Accept': 'application/json;text/json'}
            data, expire = getdata(url, args, headers, expire)
            genrows = json_parser(data, None)

        # proceed with standard workflow
        dasrows = self.set_misses(dasquery, api, genrows)
        ctime   = time.time() - time0
        try:
            if  isinstance(url, dict):
                url = "combined: %s" % url.values()
            self.write_to_cache(dasquery, expire, url, api, \
                    args, dasrows, ctime)
        except Exception as exc:
            print_exc(exc)

def dbs2_find(entity, url, kwds):
    "Find files for given set of parameters"
    if  entity not in ['run', 'file', 'block']:
        msg = 'Unsupported entity key=%s' % entity
        raise Exception(msg)
    expire  = 600
    dataset = kwds.get('dataset', None)
    block   = kwds.get('block', None)
    lfn     = kwds.get('lfn', None)
    runs    = kwds.get('runs', [])
    if  not (dataset or block or lfn):
        return
    query = 'find %s' % entity
    if  dataset:
        query += ' where dataset=%s' % dataset
    elif block:
        query += ' where block=%s' % block
    elif lfn:
        query += ' where file=%s' % lfn
    if  runs:
        rcond   = ' or '.join(['run=%s' % r for r in runs])
        query  += ' and (%s)' % rcond
    params  = {'api':'executeQuery', 'apiversion':'DBS_2_0_9', 'query':query}
    headers = {'Accept': 'text/xml'}
    source, expire = \
        getdata(url, params, headers, expire, ckey=CKEY, cert=CERT)
    pkey    = entity
    for row in qlxml_parser(source, pkey):
        val = row[entity][entity]
        yield val

def dbs3_find(entity, url, kwds):
    "Find DBS3 entity for given set of parameters"
    if  entity not in ['run', 'file', 'block']:
        msg = 'Unsupported entity key=%s' % entity
        raise Exception(msg)
    expire  = 600
    dataset = kwds.get('dataset', None)
    block   = kwds.get('block', None)
    lfn     = kwds.get('file', None)
    runs    = kwds.get('runs', [])
    if  not (dataset or block or lfn):
        return
    url = '%s/%ss' % (url, entity) # DBS3 APIs use plural entity value
    if  dataset:
        params = {'dataset':dataset}
    elif block:
        params = {'block_name': block}
    elif lfn:
        params = {'logical_file_name': lfn}
    # TODO: replace minrun/maxrun with new run range parameter once DBS3 will
    # be ready PLEASE NOTE: different DBS3 APIs uses different convention for
    # run parameter see https://svnweb.cern.ch/trac/CMSDMWM/ticket/4193 so I
    # need to use minrun/maxrun for files API, while run_num for
    # datasets/blocks/etc. APIs
    if  runs:
        if  entity == 'file':
            params.update({'minrun': runs[0], 'maxrun': runs[0]})
        else:
            params.update({'run_num': runs[0]})
    headers = {'Accept': 'application/json;text/json'}
    source, expire = \
        getdata(url, params, headers, expire, ckey=CKEY, cert=CERT)
    for row in json_parser(source, None):
        for rec in row:
            if  entity == 'file':
                yield rec['logical_file_name']
            elif  entity == 'block':
                yield rec['block_name']
            elif  entity == 'file':
                yield rec['dataset']

def dbs_find(entity, url, kwds):
    "Find given DBS entity for given set of parameters"
    if  which_dbs(url) == 'dbs':
        gen = dbs2_find(entity, url, kwds)
    else:
        gen = dbs3_find(entity, url, kwds)
    for row in gen:
        yield row

def files4site(phedex_url, files, site):
    "Find site for given files"

    params = {}
    if  site and site_pattern.match(site):
        params.update({'node': site})
    elif site and se_pattern.match(site):
        params.update({'se': site})
    else:
        return
    urls = []
    for fname in files:
        url = '%s?lfn=%s' % (phedex_url, fname)
        urls.append(url)
    tags = 'block.replica.node'
    prim_key = 'block'
    gen = urlfetch_getdata(urls, CKEY, CERT)
    for rec in gen:
        # convert record string into StringIO for xml_parser
        source = StringIO.StringIO(rec)
        for row in xml_parser(source, prim_key, tags):
            fobj = row['block']['file']
            fname = fobj['name']
            replica = fobj['replica']
            for item in replica:
                if  params.has_key('node') and item['node'] == site:
                    yield fname
                elif params.has_key('se') and item['se'] == site:
                    yield fname

def file_run_lumis(url, blocks, runs=None):
    """
    Find file, run, lumi tuple for given set of files and (optional) runs.
    """
    urls = []
    for blk in blocks:
        if  not blk:
            continue
        if  which_dbs(url) == 'dbs':
            query   = 'find file,run,lumi where block=%s' % blk
            if  runs and isinstance(runs, list):
                val = ' or '.join(['run=%s' % r for r in runs])
                query += ' and (%s)' % val
            params  = {'api':'executeQuery', 'apiversion':'DBS_2_0_9',
                       'query':query}
            headers = {'Accept': 'text/xml'}
            dbs_url = url + '?' + urllib.urlencode(params)
        else:
            dbs_url = url + '/filelumis'
            dbs_url = '%s?block_name=%s' % (dbs_url, urllib.quote(blk))
            if  runs and isinstance(runs, list):
                # TODO: I need to add run-range condition once DBS3 ready
                pass
            headers = {'Accept': 'application/json;text/json'}
        urls.append(dbs_url)
    if  not urls:
        return
    gen = urlfetch_getdata(urls, CKEY, CERT, headers)
    prim_key = 'row'
    odict = {} # output dict
    for rec in gen:
        if  which_dbs(url) == 'dbs':
            source   = StringIO.StringIO(rec)
            lumis    = []
            for row in qlxml_parser(source, prim_key):
                run  = row['row']['run']
                lfn  = row['row']['file']
                lumi = row['row']['lumi']
                key  = (lfn, run)
                odict.setdefault(key, []).append(lumi)
        else:
            for row in json.loads(rec):
                run = row['run_num']
                lfn = row['logical_file_name']
                lumilist = row['lumi_section_num']
                key = (lfn, run)
                for lumi in lumilist:
                    odict.setdefault(key, []).append(lumi)
    for key, lumis in odict.iteritems():
        lfn, run = key
        yield lfn, run, lumis

def test_dbs_files():
    """
    Test the following DAS query:
    file dataset=/a/b/c run in [1,2,3] site=X
    file block=/a/b/c#123 run in [1,2,3] site=X
    """
    phedex_url = 'https://cmsweb.cern.ch/phedex/datasvc/xml/prod/fileReplicas'
    url     = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    url     = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
    dataset = '/SingleMu/Run2011B-WMu-19Nov2011-v1/RAW-RECO'
    block   = '%s#19110c74-1b66-11e1-a98b-003048f02c8a' % dataset
    runs    = [177718, 177053]
    kwds    = {'block':block, 'runs':runs}
    kwds    = {'dataset':dataset, 'runs':runs}
    site    = 'T2_IN_TIFR'
    files   = dbs_find('file', url, kwds)
    count   = 0
    for fname in files4site(phedex_url, files, site):
        count += 1
        print fname
    print "Site=%s: nfiles=%s" % (site, count)

def test_file_run_lumis():
    """
    Test the following DAS query:
    block, run, lumi dataset=/a/b/c run in [1,2,3]
    here run clause is optional
    """
    time0   = time.time()
    url     = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    url     = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
    dataset = '/SingleMu/Run2011B-WMu-19Nov2011-v1/RAW-RECO'
#    block   = '%s#19110c74-1b66-11e1-a98b-003048f02c8a' % dataset
    runs    = [177718, 177053]
    kwds    = {'dataset':dataset, 'runs':runs}
#    kwds    = {'dataset':dataset}
    blocks  = [b for b in dbs_find('block', url, kwds)]
    print "Found %s blocks" % len(blocks)
    count   = 0
    for row in file_run_lumis(url, blocks, runs):
        count += 1
        print row
    print "# of lfn,run,lumi tuples: %s, elapsed time: %s sec" \
            % (count, time.time()-time0)

if __name__ == '__main__':
#    test_dbs_files()
    test_file_run_lumis()
