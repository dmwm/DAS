#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0702,R0913,R0912,R0915,R0904,R0914
"""
DBS3 data-service plugin.
NOTE: DBS3 APIs provide flat namespace JSON records. It means
      that data record contains all keys at one level, e.g
      {dataset:value, files:10}. This creates a problem in DAS
      since DAS data-record is nested dictionaries, e.g.
      {dataset:{name:value}}. To resolve the issue we need to
      re-map flat namespace key, e.g. dataset, into its appropriate
      DAS meaning, e.g. name. This can be done either at dbs3.yml
      map or explicitly here in parser method. Due to this fact
      extended timestamp assignment will only work for DAS data-records,
      nested dicts and therefore it is better to re-map DBS3 data-records
      directly in a code.
"""
from __future__ import print_function

__author__ = "Valentin Kuznetsov"

# system modules
import sys
import time
try: # python3
    import urllib.parse as urllib
except ImportError: # fallback to python2, we use urllib.urlencode
    import urllib
from types import GeneratorType

# python3
if  sys.version.startswith('3.'):
    basestring = str

# DAS modules
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, json_parser, print_exc
from DAS.utils.utils import dastimestamp
from DAS.utils.utils import expire_timestamp, convert2ranges, get_key_cert
from DAS.utils.url_utils import getdata, url_args
from DAS.utils.urlfetch_pycurl import getdata as urlfetch_getdata
from DAS.utils.regex import int_number_pattern

import DAS.utils.jsonwrapper as json

CKEY, CERT = get_key_cert()

def process_lumis_with(ikey, gen):
    "Helper function to process lumis with given key from provided generator"
    odict = {}
    for row in gen:
        if  'error' in row:
            yield row
            continue
        lfn, run, lumi = row
        if  ikey == 'file':
            key = lfn
        elif  ikey == 'run':
            key = run
        elif  ikey == 'file_run' or 'block_run':
            key = (lfn, run) # here lfn refers either to lfn or block
        if  isinstance(lumi, list):
            for ilumi in lumi:
                odict.setdefault(key, []).append(ilumi)
        else:
            odict.setdefault(key, []).append(ilumi)
    for key, lumi_list in odict.items():
        lumi_list.sort()
        lumis = convert2ranges(lumi_list)
        if  ikey == 'file':
            yield {'file':{'name':key}, 'lumi':{'number':lumis}}
        elif  ikey == 'run':
            yield {'run':{'run_number':key}, 'lumi':{'number':lumis}}
        elif  ikey == 'file_run':
            lfn, run = key
            yield {'run':{'run_number':run}, 'lumi':{'number':lumis},
                   'file':{'name': lfn}}
        elif  ikey == 'block_run':
            blk, run = key
            yield {'run':{'run_number':run}, 'lumi':{'number':lumis},
                   'block':{'name': blk}}

def dbs_find(entity, url, kwds, verbose=0):
    "Find DBS3 entity for given set of parameters"
    if  entity not in ['run', 'file', 'block']:
        msg = 'Unsupported entity key=%s' % entity
        raise Exception(msg)
    expire  = 600
    dataset = kwds.get('dataset', None)
    block   = kwds.get('block_name', None)
    if  not block:
        # TODO: this should go away when DBS will be retired (user in combined srv)
        block = kwds.get('block', None)
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
    if  runs:
        params.update({'run_num': runs})
    headers = {'Accept': 'application/json;text/json'}
    source, expire = \
        getdata(url, params, headers, expire, ckey=CKEY, cert=CERT,
                verbose=verbose)
    for row in json_parser(source, None):
        for rec in row:
            try:
                if  isinstance(rec, basestring):
                    print(dastimestamp('DBS3 ERROR:'), row)
                elif  entity == 'file':
                    yield rec['logical_file_name']
                elif  entity == 'block':
                    yield rec['block_name']
                elif  entity == 'file':
                    yield rec['dataset']
            except Exception as exp:
                msg = 'Fail to parse "%s", exception="%s"' % (rec, exp)
                print_exc(msg)

def block_run_lumis(url, blocks, runs=None, verbose=0):
    """
    Find block, run, lumi tuple for given set of files and (optional) runs.
    """
    headers = {'Accept': 'application/json;text/json'}
    urls = []
    params = {}
    for blk in blocks:
        if  not blk:
            continue
        dbs_url = '%s/filelumis/?block_name=%s' % (url, urllib.quote(blk))
        if  runs and isinstance(runs, list):
            params.update({'run_num': urllib.quote(str(runs))})
        urls.append(dbs_url)
    if  not urls:
        return
    if  verbose > 1:
        print("\nDEBUG: block_run_lumis")
        print(urls)
    gen = urlfetch_getdata(urls, CKEY, CERT, headers)
    odict = {} # output dict
    for rec in gen:
        blk = urllib.unquote(url_args(rec['url'])['block_name'])
        if  'error' in rec:
            error  = rec.get('error')
            reason = rec.get('reason', '')
            yield {'error':error, 'reason':reason}
        else:
            for row in json.loads(rec['data']):
                run = row['run_num']
                lumilist = row['lumi_section_num']
                key = (blk, run)
                for lumi in lumilist:
                    odict.setdefault(key, []).append(lumi)
    for key, lumis in odict.items():
        blk, run = key
        yield blk, run, lumis

def file_run_lumis(url, blocks, runs=None, valid=None, verbose=0):
    """
    Find file, run, lumi tuple for given set of files and (optional) runs.
    """
    headers = {'Accept': 'application/json;text/json'}
    urls = []
    for blk in blocks:
        if  not blk:
            continue
        dbs_url = '%s/filelumis/?block_name=%s' % (url, urllib.quote(blk))
        if  valid:
            dbs_url += '&validFileOnly=1'
        if  runs:
            dbs_url += "&run_num=%s" % urllib.quote(str(runs))
        urls.append(dbs_url)
    if  not urls:
        return
    if  verbose > 1:
        print("\nDEBUG: file_run_lumis")
        print(urls)
    gen = urlfetch_getdata(urls, CKEY, CERT, headers)
    odict = {} # output dict
    for rec in gen:
        if  'error' in rec:
            error  = rec.get('error')
            reason = rec.get('reason', '')
            yield {'error':error, 'reason':reason}
        else:
            for row in json.loads(rec['data']):
                run = row['run_num']
                lfn = row['logical_file_name']
                lumilist = row['lumi_section_num']
                key = (lfn, run)
                for lumi in lumilist:
                    odict.setdefault(key, []).append(lumi)
    for key, lumis in odict.items():
        lfn, run = key
        yield lfn, run, lumis

def get_api(url):
    "Extract from DBS3 URL the api name"
    if  url[-1] == '/':
        url = url[:-1]
    return url.split('/')[-1]

def get_modification_time(record):
    "Get modification timestamp from DBS data-record"
    for key in ['dataset', 'block', 'file']:
        if  key in record:
            obj = record[key]
            if  isinstance(obj, dict) and 'last_modification_date' in obj:
                return record[key]['last_modification_date']
    if  isinstance(record, dict) and 'last_modification_date' in record:
        return record['last_modification_date']
    return None

def old_timestamp(tstamp, threshold=2592000):
    "Check if given timestamp is old enough"
    if  not threshold:
        return False
    if  tstamp < (time.mktime(time.gmtime())-threshold):
        return True
    return False

def get_block_run_lumis(url, api, args, verbose=0):
    "Helper function to deal with block,run,lumi requests"
    run_value = args.get('run_num', [])
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
    blocks = dbs_find('block', url, args, verbose)
    gen = block_run_lumis(url, blocks, runs, verbose)
    key = 'block_run'
    for row in process_lumis_with(key, gen):
        yield row

def get_file_run_lumis(url, api, args, verbose=0):
    "Helper function to deal with file,run,lumi requests"
    run_value = args.get('run_num', [])
    if  isinstance(run_value, dict) and '$in' in run_value:
        runs = run_value['$in']
    elif isinstance(run_value, list):
        runs = run_value
    else:
        if  int_number_pattern.match(str(run_value)):
            runs = [run_value]
        elif run_value[0]=='[' and run_value[-1]==']':
            if  '-' in run_value: # continuous range
                runs = run_value.replace("'", '').replace('[', '').replace(']', '')
            else:
                runs = json.loads(run_value)
        else:
            runs = run_value
    args.update({'runs': runs})
    blk = args.get('block_name', None)
    if  blk: # we don't need to look-up blocks
        blocks = [blk]
    else:
        blocks = dbs_find('block', url, args, verbose)
    if  not blocks:
        return
    valid = 1 if args.get('validFileOnly', '') else 0
    gen = file_run_lumis(url, blocks, runs, valid, verbose)
    key = 'file_run'
    if  api.startswith('run_lumi'):
        key = 'run'
    if  api.startswith('file_lumi'):
        key = 'file'
    if  api.startswith('file_run_lumi'):
        key = 'file_run'
    for row in process_lumis_with(key, gen):
        yield row

def get_file4dataset_run_lumi(url, api, args, verbose=0):
    "Helper function to deal with file dataset=/a/b/c run=123 lumi=1 requests"
    run_value = args.get('run_num', [])
    if  isinstance(run_value, dict) and '$in' in run_value:
        runs = run_value['$in']
    elif isinstance(run_value, list):
        runs = run_value
    else:
        runs = [run_value]
    ilumi = args.get('lumi')
    args.update({'runs': runs})
    blocks = dbs_find('block', url, args, verbose)
    valid = 1 if args.get('validFileOnly', '') else 0
    gen = file_run_lumis(url, blocks, runs, valid, verbose)
    for lfn, _run, lumi in gen:
        if  lumi == ilumi:
            yield lfn

def get_lumis4block_run(url, api, args, verbose=0):
    "Get lumi numbers for given block/run parameters"
    for row in get_file_run_lumis(url, api, args, verbose):
        yield dict(lumi=row['lumi'])

### helper functions for get_blocks4tier_dates
def process(gen):
    "Process generator from getdata"
    for row in gen:
        if  'error' in row:
            error = row.get('error')
            reason = row.get('reason', '')
            print(dastimestamp('DAS ERROR'), error, reason)
            yield row
            continue
        if  'data' in row:
            yield json.loads(row['data'])

def blocks4tier_date(dbs, tier, min_cdate, max_cdate, verbose=0):
    "Get list of blocks for given parameters"
    headers = {'Accept':'text/json;application/json'}
    url     = dbs + "/blocks"
    params  = {'data_tier_name':tier,
               'min_cdate':min_cdate,
               'max_cdate':max_cdate}
    urls    = ['%s?%s' % (url, urllib.urlencode(params))]
    if  verbose > 1:
        print("\nblocks4tier_date")
        print(urls)
    res     = process(urlfetch_getdata(urls, CKEY, CERT, headers))
    err     = 'Unable to get blocks for tier=%s, mindate=%s, maxdate=%s' \
                % (tier, min_cdate, max_cdate)
    for blist in res:
        if 'error' in blist:
            yield blist
            continue
        if  isinstance(blist, dict):
            if  'block_name' not in blist:
                msg = err + ', reason=%s' % json.dumps(blist)
                raise Exception(msg)
        for row in blist:
            yield row['block_name']

def block_summary(dbs, blocks):
    "Get block summary information for given set of blocks"
    headers = {'Accept':'text/json;application/json'}
    url     = dbs + "/blocksummaries"
    urls    = ['%s/?block_name=%s' % (url, urllib.quote(b)) for b in blocks]
    res     = urlfetch_getdata(urls, CKEY, CERT, headers)
    for row in res:
        if  'error' in row:
            error  = row.get('error')
            reason = row.get('reason', '')
            yield {'error':error, 'reason':reason}
            continue
        url = row['url']
        blk = urllib.unquote(url.split('=')[-1])
        for rec in json.loads(row['data']):
            data = {'name': blk, 'size': rec['file_size'],
                    'nfiles': rec['num_file'],
                    'nevents': rec['num_event']}
            yield dict(block=data)

### this needs to be revisited with new DBS3 API blockSummaries
### https://svnweb.cern.ch/trac/CMSDMWM/ticket/4148
### currently the result of get_blocks4tier_dates is not precise since
### blocks creation dates are not the same as dataset ones
def get_blocks4tier_dates(dbs_url, api, args, verbose=0):
    "Helper function to get blocks for given tier and date range"
    fullday = 24*60*60
    tier    = args.get('tier')
    ddict   = args.get('date')
    if  isinstance(ddict, dict):
        if  '$lte' in ddict:
            date1 = ddict['$gte']
            date2 = ddict['$lte'] + fullday
        elif '$in' in ddict:
            date1 = ddict['$in'][0]
            date2 = ddict['$in'][-1]
        else:
            msg = '%s, unsupported date dict, "%s"' % (api, ddict)
            raise Exception(msg)
    else:
        date1 = ddict
        date1 = ddict + fullday

    try:
        # get block list
        blist   = (r for r in \
                blocks4tier_date(dbs_url, tier, date1, date2, verbose))

        # get summaries
        for row in block_summary(dbs_url, blist):
            yield row
    except Exception as exc:
        data = {'error': 'get_blocks4tier_dates API error',
                'reason': str(exc),
                'ts': time.time()}
        yield dict(block=data)

def get_dataset4block(args, verbose=0):
    "Get dataset name for given block"
    block = args.get('block_name')
    yield {'dataset':{'name':block.split('#')[0]}}

class DBS3Service(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dbs3', config)
        self.reserved = ['api', 'apiversion']
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)
        self.prim_instance = self.dasmapping.dbs_global_instance(self.name)
        self.instances = self.dasmapping.dbs_instances(self.name)
        self.extended_expire = config['dbs'].get('extended_expire', 0)
        self.extended_threshold = config['dbs'].get('extended_threshold', 0)
        self.dbs_choice = config['das'].get('main_dbs', 'dbs3')

    def apicall(self, dasquery, url, api, args, dformat, expire):
        "DBS3 implementation of AbstractService:apicall method"
        if  api == 'run_lumi4dataset' or api == 'run_lumi4block' or \
            api == 'file_lumi4dataset' or api == 'file_lumi4block' or \
            api == 'file_run_lumi4dataset' or api == 'file_run_lumi4block' or \
            api == 'block_run_lumi4dataset' or \
            api == 'file4dataset_run_lumi' or \
            api == 'blocks4tier_dates' or api == 'dataset4block' or \
            api == 'lumi4block_run':
            time0 = time.time()
            dbs_url = '/'.join(url.split('/')[:-1])
            if  api == 'block_run_lumi4dataset':
                dasrows = get_block_run_lumis(dbs_url, api, args, self.verbose)
            elif api == 'blocks4tier_dates':
                dasrows = get_blocks4tier_dates(dbs_url, api, args,
                        self.verbose)
            elif api == 'file4dataset_run_lumi':
                dasrows = get_file4dataset_run_lumi(dbs_url, api, args,
                        self.verbose)
            elif api == 'lumi4block_run':
                dasrows = get_lumis4block_run(dbs_url, api, args, self.verbose)
            elif api == 'dataset4block':
                dasrows = get_dataset4block(args, self.verbose)
            else:
                dasrows = get_file_run_lumis(dbs_url, api, args, self.verbose)
            ctime = time.time()-time0
            self.write_to_cache(dasquery, expire, url, api, args,
                    dasrows, ctime)
        else:
            super(DBS3Service, self).apicall(\
                    dasquery, url, api, args, dformat, expire)

    def getdata(self, url, params, expire, headers=None, post=None):
        """URL call wrapper"""
        if  not headers:
            headers =  {'Accept': 'application/json' } # DBS3 always needs that
        if  url.find('datasetlist') != -1:
            post = True
            headers['Content-type'] = 'application/json'
        return getdata(url, params, headers, expire, post,
                self.error_expire, self.verbose, self.ckey, self.cert,
                doseq=False, system=self.name)

    def url_instance(self, url, instance):
        """
        Adjust URL for a given instance
        """
        if  instance in self.instances:
            if  isinstance(url, basestring):
                if  '/' in instance and '/' not in self.prim_instance:
                    return url.replace('prod/%s' % self.prim_instance, instance)
                return url.replace(self.prim_instance, instance)
            elif isinstance(url, list):
                if  '/' in instance and '/' not in self.prim_instance:
                    urls = [u.replace('prod/%' % self.prim_instance, instance) for u in url]
                else:
                    urls = [u.replace(self.prim_instance, instance) for u in url]
                return urls
        else:
            return None
        return url

    def adjust_params(self, api, kwds, inst=None):
        """
        Adjust DBS2 parameters for specific query requests
        """
        # adjust run parameter if it is present in kwds
        val = kwds.get('run_num', None)
        if  val:
            if  isinstance(val, dict): # we got a run range
                if  '$in' in val:
                    kwds['run_num'] = val['$in']
                if  '$lte' in val:
                    kwds['run_num'] = '%s-%s' % (val['$gte'], val['$lte'])
            else:
                kwds['run_num'] = val
        if  api in ['site4dataset', 'site4block']:
            # skip API call if inst is global one (data provided by phedex)
            if  inst == self.prim_instance:
                kwds['dataset'] = 'required' # we skip
        if  api == 'acquisitioneras':
            try:
                del kwds['era']
            except KeyError:
                pass
        if  api == 'datasets' or api == 'datasetlist':
            if  isinstance(kwds['dataset'], dict):
                kwds['dataset'] = kwds['dataset']['$in']
            if  kwds['dataset'][0] == '*':
                kwds['dataset'] = '/' + kwds['dataset']
            if  kwds['dataset'] == '*' and kwds['block_name']:
                kwds['dataset'] = kwds['block_name'].split('#')[0]
            if  api == 'datasetlist' and 'detail' in kwds:
                if  kwds['detail'] == 'True':
                    kwds['detail'] = True
                else:
                    kwds['detail'] = False
            if  api == 'datasets':
                if  kwds['dataset'].startswith('/'):
                    _, prim, proc, tier = kwds['dataset'].split('/')
                    if  prim == '*' and proc == '*':
                        kwds['data_tier_name'] = tier
                        del kwds['dataset']
            if  'cdate' in kwds:
                min_date = None
                max_date = None
                if  isinstance(kwds['cdate'], int):
                    min_date = kwds['cdate']
                    max_date = min_date + 24*60*60
                elif isinstance(kwds['cdate'], dict): # we got date range
                    min_date = kwds['cdate']['$gte']
                    max_date = kwds['cdate']['$lte']
                if  min_date and max_date:
                    kwds['min_cdate'] = min_date
                    kwds['max_cdate'] = max_date
                del kwds['cdate']
            if  'min_cdate' in kwds:
                min_date = None
                max_date = None
                if  isinstance(kwds['min_cdate'], int):
                    min_date = kwds['min_cdate']
                    max_date = min_date + 24*60*60
                elif isinstance(kwds['min_cdate'], dict): # we got date range
                    min_date = kwds['min_cdate']['$gte']
                    max_date = kwds['min_cdate']['$lte']
                if  min_date and max_date:
                    kwds['min_cdate'] = min_date
                    kwds['max_cdate'] = max_date
            try:
                del kwds['block_name']
            except KeyError:
                pass
        if  api == 'file4DatasetRunLumi':
            val = kwds['lumi_list']
            if  val:
                kwds['lumi_list'] = [val]
        if  api == 'files' or api == 'files_via_dataset' or \
            api == 'files_via_block':
            # we can't pass status input parameter to DBS3 API since
            # it does not accepted, instead it will be used for filtering
            # end-results
            del kwds['status']
        if  api == 'filesummaries' or api == 'dataset_info':
            if  kwds['dataset'].find('*') != -1:
                kwds['dataset'] = 'required'
        if  api == 'filesummaries' or api == 'summary4dataset_run':
            if  kwds['validFileOnly'] == '*':
                kwds['validFileOnly'] = 0 # show all files

    def parser(self, query, dformat, source, api):
        """
        DBS3 data-service parser.
        """
        if  isinstance(source, GeneratorType):
            for row in source:
                yield row
            return
        for row in self.parser_helper(query, dformat, source, api):
            mod_time = get_modification_time(row)
            if  self.extended_expire:
                new_expire = expire_timestamp(self.extended_expire)
                if  mod_time and \
                    old_timestamp(mod_time, self.extended_threshold):
                    row.update({'das':{'expire': new_expire}})
                # filesummaries is summary DBS API about dataset,
                # it collects information about number of files/blocks/events
                # for given dataset and therefore will be merged with datasets
                # API record. To make a proper merge with extended
                # timestamp/threshold options I need explicitly assign
                # das.expire=extended_timestamp, otherwise
                # the merged record will pick-up smallest between
                # filesummaries and datasets records.
                if  api == 'filesummaries':
                    row.update({'das': {'expire': new_expire}})
            yield row

    def parser_helper(self, query, dformat, source, api):
        """
        DBS3 data-service parser helper, it is used by parser method.
        """
        if  api in ['site4dataset', 'site4block']:
            gen = json_parser(source, self.logger)
        else:
            gen = DASAbstractService.parser(self, query, dformat, source, api)
        if  api in ['site4dataset', 'site4block']:
            sites = set()
            for rec in gen:
                if  isinstance(rec, list):
                    for row in rec:
                        orig_site = row['origin_site_name']
                        if  orig_site not in sites:
                            sites.add(orig_site)
                else:
                    orig_site = rec.get('origin_site_name', None)
                    if  orig_site and orig_site not in sites:
                        sites.add(orig_site)
            for site in sites:
                yield {'site': {'name': site}}
        elif api == 'datasets' or api == 'dataset_info' or api == 'datasetlist':
            for row in gen:
                row['name'] = row['dataset']
                del row['dataset']
                yield {'dataset':row}
        elif api == 'filesummaries':
            name = query.mongo_query['spec']['dataset.name']
            for row in gen:
                row['dataset']['name'] = name
                yield row
        elif api == 'summary4dataset_run' or api == 'summary4block_run':
            spec = query.mongo_query.get('spec', {})
            dataset = spec.get('dataset.name', '')
            block = spec.get('block.name', '')
            run = spec.get('run.run_number', 0)
            if  isinstance(run, dict): # we got a run range
                if  '$in' in run:
                    run = run['$in']
                elif '$lte' in run:
                    run = range(run['$gte'], run['$lte'])
            for row in gen:
                if  run:
                    row.update({"run": run})
                if  dataset:
                    row.update({"dataset": dataset})
                if  block:
                    row.update({"block": block})
                yield row
        elif api == 'releaseversions':
            for row in gen:
                values = row['release']['release_version']
                for val in values:
                    yield dict(release=dict(name=val))
        elif api == 'datasetaccesstypes':
            for row in gen:
                values = row['status']['dataset_access_type']
                for val in values:
                    yield dict(status=dict(name=val))
        elif api == 'blockorigin':
            for row in gen:
                yield row
        elif api == 'blockparents':
            for row in gen:
                try:
                    del row['parent']['this_block_name']
                except:
                    pass
                yield row
        elif api == 'fileparents':
            for row in gen:
                parent = row['parent']
                for val in parent['parent_logical_file_name']:
                    yield dict(name=val)
        elif api == 'runs_via_dataset' or api == 'runs':
            for row in gen:
                values = row['run']['run_num']
                if  isinstance(values, list):
                    for val in values:
                        yield dict(run_number=val)
                else:
                    yield dict(run_number=values)
        elif api == 'filechildren':
            for row in gen:
                parent = row['child']
                for val in parent['child_logical_file_name']:
                    yield dict(name=val)
        elif api == 'files' or api == 'files_via_dataset' or \
            api == 'files_via_block':
            status = 'VALID'
            for row in gen:
                if  'spec' in query.mongo_query:
                    if  'status.name' in query.mongo_query['spec']:
                        status = query.mongo_query['spec']['status.name']
                try:
                    file_status = row['file']['is_file_valid']
                except KeyError:
                    file_status = 0 # file status is unknown
                if  status == '*': # any file
                    pass
                elif  status == 'INVALID': # filter out valid files
                    if  int(file_status) == 1:# valid status
                        row = None
                else: # filter out invalid files
                    if  int(file_status) == 0:# invalid status
                        row = None
                if  row:
                    yield row
        elif api == 'filelumis' or api == 'filelumis4block':
            for row in gen:
                if  'lumi' in row:
                    if  'lumi_section_num' in row['lumi']:
                        val = row['lumi']['lumi_section_num']
                        row['lumi']['lumi_section_num'] = convert2ranges(val)
                    yield row
                else:
                    yield row
        else:
            for row in gen:
                yield row
