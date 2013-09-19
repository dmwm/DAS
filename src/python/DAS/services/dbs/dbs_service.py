#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0702,R0914,R0912,R0915

"""
DBS service
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
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, xml_parser, qlxml_parser
from DAS.utils.utils import dbsql_opt_map, convert_datetime, dastimestamp
from DAS.utils.utils import expire_timestamp, get_key_cert, convert2ranges
from DAS.utils.global_scope import SERVICES
from DAS.utils.url_utils import getdata
from DAS.utils.urlfetch_pycurl import getdata as urlfetch_getdata
from DAS.utils.regex import int_number_pattern

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
            key = (lfn, run) # here lfn can refer to lfn or blk
        if  isinstance(lumi, list):
            for ilumi in lumi:
                odict.setdefault(key, []).append(ilumi)
        else:
            odict.setdefault(key, []).append(ilumi)
    for key, lumi_list in odict.iteritems():
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

def dbs_find(entity, url, kwds):
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

def block_run_lumis(url, blocks, runs=None):
    """
    Find block, run, lumi tuple for given set of files and (optional) runs.
    """
    headers = {'Accept': 'text/xml'}
    urls = []
    for blk in blocks:
        if  not blk:
            continue
        query   = 'find block,run,lumi where block=%s' % blk
        if  runs and isinstance(runs, list):
            val = ' or '.join(['run=%s' % r for r in runs])
            query += ' and (%s)' % val
        params  = {'api':'executeQuery', 'apiversion':'DBS_2_0_9',
                   'query':query}
        dbs_url = url + '?' + urllib.urlencode(params)
        urls.append(dbs_url)
    if  not urls:
        return
    gen = urlfetch_getdata(urls, CKEY, CERT, headers)
    prim_key = 'row'
    odict = {} # output dict
    for rec in gen:
        if  'error' in rec:
            error  = rec.get('error')
            reason = rec.get('reason', '')
            print dastimestamp('DAS ERROR'), error, reason
            yield {'error': error, 'reason': reason}
        else:
            source   = StringIO.StringIO(rec['data'])
            lumis    = []
            for row in qlxml_parser(source, prim_key):
                run  = row['row']['run']
                blk  = row['row']['block']
                lumi = row['row']['lumi']
                key  = (blk, run)
                odict.setdefault(key, []).append(lumi)
    for key, lumis in odict.iteritems():
        blk, run = key
        yield blk, run, lumis

def file_run_lumis(url, blocks, runs=None):
    """
    Find file, run, lumi tuple for given set of files and (optional) runs.
    """
    headers = {'Accept': 'text/xml'}
    urls = []
    for blk in blocks:
        if  not blk:
            continue
        query   = 'find file,run,lumi where block=%s' % blk
        if  runs and isinstance(runs, list):
            val = ' or '.join(['run=%s' % r for r in runs])
            query += ' and (%s)' % val
        params  = {'api':'executeQuery', 'apiversion':'DBS_2_0_9',
                   'query':query}
        dbs_url = url + '?' + urllib.urlencode(params)
        urls.append(dbs_url)
    if  not urls:
        return
    gen = urlfetch_getdata(urls, CKEY, CERT, headers)
    prim_key = 'row'
    odict = {} # output dict
    for rec in gen:
        if  'error' in rec:
            error  = rec.get('error')
            reason = rec.get('reason', '')
            print dastimestamp('DAS ERROR'), error, reason
            yield {'error': error, 'reason': reason}
        else:
            source   = StringIO.StringIO(rec['data'])
            lumis    = []
            for row in qlxml_parser(source, prim_key):
                run  = row['row']['run']
                lfn  = row['row']['file']
                lumi = row['row']['lumi']
                key  = (lfn, run)
                odict.setdefault(key, []).append(lumi)
    for key, lumis in odict.iteritems():
        lfn, run = key
        yield lfn, run, lumis

def get_modification_time(record):
    "Get modification timestamp from DBS record"
    if  record.has_key('dataset'):
        if  record['dataset'].has_key('procds.moddate'):
            return record['dataset']['procds.moddate']
    for key in ['block', 'file_lfn']:
        if  record.has_key(key):
            if  record[key].has_key('last_modification_date'):
                return record[key]['last_modification_date']
    return None

def old_timestamp(tstamp, threshold=2592000):
    "Check if given timestamp is old enough"
    if  not threshold:
        return False
    if  tstamp < (time.mktime(time.gmtime())-threshold):
        return True
    return False

def convert_dot(row, key, attrs):
    """Convert dot notation key.attr into storage one"""
    for attr in attrs:
        if  row.has_key(key) and row[key].has_key(attr):
            name = attr.split('.')[-1]
            row[key][name] = row[key][attr]
            del row[key][attr]

def get_block_run_lumis(url, args):
    "Helper function to deal with block,run,lumi requests"
    run_value = args.get('run', [])
    if  isinstance(run_value, dict) and run_value.has_key('$in'):
        runs = run_value['$in']
    elif isinstance(run_value, list):
        runs = run_value
    else:
        if  int_number_pattern.match(str(run_value)):
            runs = [run_value]
        else:
            runs = []
    args.update({'runs': runs})
    blocks = dbs_find('block', url, args)
    gen = block_run_lumis(url, blocks, runs)
    key = 'block_run'
    for row in process_lumis_with(key, gen):
        yield row

def get_file_run_lumis(url, api, args):
    "Helper function to deal with file,run,lumi requests"
    run_value = args.get('run', [])
    if  isinstance(run_value, dict) and run_value.has_key('$in'):
        runs = run_value['$in']
    elif isinstance(run_value, list):
        runs = run_value
    else:
        if  int_number_pattern.match(str(run_value)):
            runs = [run_value]
        else:
            runs = []
    args.update({'runs': runs})
    blocks = dbs_find('block', url, args)
    gen = file_run_lumis(url, blocks, runs)
    if  api.startswith('run_lumi'):
        key = 'run'
    if  api.startswith('file_lumi'):
        key = 'file'
    if  api.startswith('file_run_lumi'):
        key = 'file_run'
    for row in process_lumis_with(key, gen):
        yield row

def summary4dataset_run(url, kwds):
    "Helper function to deal with summary dataset=/a/b/c requests"
    urls = []
    cond = ''
    val = kwds.get('run', 'optional')
    if  val != 'optional':
        if  isinstance(val, dict):
            min_run = 0
            max_run = 0
            if  val.has_key('$lte'):
                max_run = val['$lte']
            if  val.has_key('$gte'):
                min_run = val['$gte']
            if  min_run and max_run:
                val = "run >=%s and run <= %s" % (min_run, max_run)
            elif val.has_key('$in'):
                val = ' or '.join(['run=%s' % r for r in val['$in']])
                val = '(%s)' % val
        elif isinstance(val, int):
            val = "run=%d" % val
        cond += ' and %s' % val
    val = kwds.get('dataset', None)
    if  val and val != 'optional':
        cond += ' and dataset=%s' % val
    val = kwds.get('block', None)
    if  val and val != 'optional':
        cond += ' and block=%s' % val
    query = "find file, file.size, file.numevents where " + cond[4:]
    params  = {'api':'executeQuery', 'apiversion':'DBS_2_0_9', 'query':query}
    url1 = url + '?' + urllib.urlencode(params)
    urls.append(url1)
    query = "find run, count(lumi) where " + cond[4:]
    params  = {'api':'executeQuery', 'apiversion':'DBS_2_0_9', 'query':query}
    url2 = url + '?' + urllib.urlencode(params)
    urls.append(url2)
    headers = {'Accept': 'text/xml'}
    gen = urlfetch_getdata(urls, CKEY, CERT, headers)
    tot_size  = 0
    tot_evts  = 0
    tot_lumis = 0
    tot_files = 0
    for rec in gen:
        if  'error' in rec:
            error  = rec.get('error')
            reason = rec.get('reason', '')
            srec = {'summary':'', 'error':error, 'reason':reason}
            yield srec
        url = rec['url']
        data = rec['data']
        stream = StringIO.StringIO(data)
        if  url.find('file') != -1:
            prim_key = 'file'
        else:
            prim_key = 'run'
        for row in qlxml_parser(stream, prim_key):
            if  prim_key == 'file':
                fdata = row['file']
                tot_size  += fdata['file.size']
                tot_evts  += fdata['file.numevents']
                tot_files += 1
            else:
                fdata = row['run']
                tot_lumis += fdata['count_lumi']
    srec = {'summary': {'file_size':tot_size, 'nevents':tot_evts,
        'nlumis':tot_lumis, 'nfiles': tot_files}}
    yield srec

class DBSService(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dbs', config)
        self.reserved = ['api', 'apiversion']
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)
        self.prim_instance = self.dasmapping.dbs_global_instance(self.name)
        self.instances = self.dasmapping.dbs_instances(self.name)
        self.extended_expire = config['dbs'].get('extended_expire', 0)
        self.extended_threshold = config['dbs'].get('extended_threshold', 0)

    def apicall(self, dasquery, url, api, args, dformat, expire):
        "DBS2 implementation of AbstractService:apicall method"
        if  api == 'run_lumi4dataset' or api == 'run_lumi4block' or \
            api == 'file_lumi4dataset' or api == 'file_lumi4block' or \
            api == 'file_run_lumi4dataset' or api == 'file_run_lumi4block' or \
            api == 'summary4dataset_run' or \
            api == 'block_run_lumi4dataset':
            time0 = time.time()
            if  api == 'block_run_lumi4dataset':
                dasrows = get_block_run_lumis(url, args)
            elif api == 'summary4dataset_run':
                dasrows = summary4dataset_run(url, args)
            else:
                dasrows = get_file_run_lumis(url, api, args)
            ctime = time.time()-time0
            self.write_to_cache(dasquery, expire, url, api, args,
                    dasrows, ctime)
        else:
            super(DBSService, self).apicall(\
                    dasquery, url, api, args, dformat, expire)

    def url_instance(self, url, instance):
        """
        Adjust URL for a given instance
        """
        if  instance in self.instances:
            return url.replace(self.prim_instance, instance)
        return url

    def adjust_params(self, api, kwds, inst=None):
        """
        Adjust DBS2 parameters for specific query requests
        To mimic DBS3 behavior we only allow dataset summary information
        for fakeDatasetSummary and fakeListDataset4Block APIs who uses
        full dataset and block name, respectively.
        """
        sitedb = SERVICES.get('sitedb2', None) # SiteDB from global scope
        if  api == 'fakeRun4Block':
            val = kwds['block']
            if  val != 'required':
                kwds['query'] = 'find run where block=%s' % val
            else:
                kwds['query'] = 'required'
            kwds.pop('block')
        if  api == 'fakeStatus':
            val = kwds['status']
            if  val:
                kwds['query'] = \
                'find dataset.status where dataset.status=%s' % val.upper()
            else:
                kwds['query'] = 'find dataset.status'
            val = kwds['dataset']
            if  val:
                if  kwds['query'].find(' where ') != -1:
                    kwds['query'] += ' and dataset=%s' % val
                else:
                    kwds['query'] += ' where dataset=%s' % val
            kwds.pop('status')
        if  api == 'listPrimaryDatasets':
            pat = kwds['pattern']
            if  pat[0] == '/':
                kwds['pattern'] = pat.split('/')[1]
        if  api == 'listProcessedDatasets':
            pat = kwds['processed_datatset_name_pattern']
            if  pat[0] == '/':
                try:
                    kwds['processed_datatset_name_pattern'] = pat.split('/')[2]
                except:
                    pass
        if  api == 'fakeReleases':
            val = kwds['release']
            if  val != 'required':
                kwds['query'] = 'find release where release=%s' % val
            else:
                kwds['query'] = 'required'
            kwds.pop('release')
        if  api == 'fakeRelease4File':
            val = kwds['file']
            if  val != 'required':
                kwds['query'] = 'find release where file=%s' % val
            else:
                kwds['query'] = 'required'
            kwds.pop('file')
        if  api == 'fakeRelease4Dataset':
            val = kwds['dataset']
            if  val != 'required':
                kwds['query'] = 'find release where dataset=%s' % val
            else:
                kwds['query'] = 'required'
            kwds.pop('dataset')
        if  api == 'fakeConfig':
            val = kwds['dataset']
            sel = 'config.name, config.content, config.version, config.type, \
 config.annotation, config.createdate, config.createby, config.moddate, \
 config.modby'
            if  val != 'required':
                kwds['query'] = 'find %s where dataset=%s' % (sel, val)
            else:
                kwds['query'] = 'required'
            kwds.pop('dataset')
        if  api == 'fakeSite4Dataset' and inst and inst != self.prim_instance:
            val = kwds['dataset']
            if  val != 'required':
                kwds['query'] = "find site where dataset=%s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('dataset')
        if  api == 'fakeDataset4Site' and inst and inst != self.prim_instance:
            val = kwds['site']
            if  val != 'required':
                sinfo = sitedb.site_info(val)
                if  sinfo and sinfo.has_key('resources'):
                    for row in sinfo['resources']:
                        if  row['type'] == 'SE' and row.has_key('fqdn'):
                            sename = row['fqdn']
                            kwds['query'] = \
                                    "find dataset,site where site=%s" % sename
                            break
            else:
                kwds['query'] = 'required'
            kwds.pop('site')
        if  api == 'fakeListDataset4File':
            val = kwds['file']
            if  val != 'required':
                kwds['query'] = "find dataset, count(block), count(file.size) \
  where file=%s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('file')
        if  api == 'fakeListDataset4Block':
            val = kwds['block']
            if  val != 'required':
                kwds['query'] = "find dataset, count(block), \
  sum(block.size), sum(block.numfiles), sum(block.numevents) \
  where block=%s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('block')
        if  api == 'fakeRun4Run':
            val = kwds['run']
            if  val != 'required':
                if  isinstance(val, dict):
                    min_run = 0
                    max_run = 0
                    if  val.has_key('$lte'):
                        max_run = val['$lte']
                    if  val.has_key('$gte'):
                        min_run = val['$gte']
                    if  min_run and max_run:
                        val = "run >=%s and run <= %s" % (min_run, max_run)
                    elif val.has_key('$in'):
                        val = ' or '.join(['run=%s' % r for r in val['$in']])
                        val = '(%s)' % val
                elif isinstance(val, int):
                    val = "run = %d" % val
                kwds['query'] = "find run where %s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('run')
        if  api == 'fakeBlock4file':
            lfn = kwds.get('file', 'required')
            if  lfn != 'required':
                kwds['query'] = 'find block.name where file=%s' % lfn
            else:
                kwds['query'] = 'required'
        if  api == 'fakeLumis4block':
            block = kwds.get('block', 'required')
            if  block != 'required':
                kwds['query'] = \
                'find lumi.number, run.number, file.name where block=%s' % block
                kwds.pop('block')
            else:
                kwds['query'] = 'required'
        if  api == 'fakeLumis4FileRun':
            query = kwds.get('query', 'required')
            lfn = kwds.get('lfn', 'required')
            if  lfn != 'required':
                query = \
                'find lumi.number, run.number where file=%s' % lfn
                kwds.pop('lfn')
            run = kwds.get('run', 'optional')
            if  run != 'optional':
                query += ' and run=%s' % run
                kwds.pop('run')
            kwds['query'] = query
        if  api == 'fakeBlock4DatasetRun':
            dataset = kwds.get('dataset', 'required')
            if  dataset != 'required':
                kwds['query'] = 'find block.name where dataset=%s'\
                        % dataset
            else:
                kwds['query'] = 'required'
            val = kwds.get('run', 'required')
            if  val != 'required':
                if  isinstance(val, dict):
                    min_run = 0
                    max_run = 0
                    if  val.has_key('$lte'):
                        max_run = val['$lte']
                    if  val.has_key('$gte'):
                        min_run = val['$gte']
                    if  min_run and max_run:
                        val = "run >=%s and run <= %s" % (min_run, max_run)
                    elif val.has_key('$in'):
                        val = ' or '.join(['run=%s' % r for r in val['$in']])
                        val = '(%s)' % val
                elif isinstance(val, int):
                    val = "run = %d" % val
                kwds['query'] += ' and ' + val
                kwds.pop('dataset')
                kwds.pop('run')
            else:
                kwds['query'] = 'required'
        if  api == 'fakeGroup4Dataset':
            val = kwds['dataset']
            if  val != 'required':
                val = "dataset = %s" % val
                kwds['query'] = "find phygrp where %s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('dataset')
        if  api == 'fakeChild4File':
            val = kwds['file']
            if  val != 'required':
                val = "file = %s" % val
                kwds['query'] = "find file.child where %s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('file')
        if  api == 'fakeChild4Dataset':
            val = kwds['dataset']
            if  val != 'required':
                val = "dataset = %s" % val
                kwds['query'] = "find dataset.child where %s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('dataset')
        if  api == 'fakeDataset4Run':
            val = kwds['run']
            qlist = []
            if  val != 'required':
                if  isinstance(val, dict):
                    min_run = 0
                    max_run = 0
                    if  val.has_key('$lte'):
                        max_run = val['$lte']
                    if  val.has_key('$gte'):
                        min_run = val['$gte']
                    if  min_run and max_run:
                        val = "run >=%s and run <= %s" % (min_run, max_run)
                    elif val.has_key('$in'):
                        val = ' or '.join(['run=%s' % r for r in val['$in']])
                        val = '(%s)' % val
                elif isinstance(val, int):
                    val = "run = %d" % val
                if  kwds.has_key('dataset') and kwds['dataset']:
                    val += ' and dataset=%s' % kwds['dataset']
                kwds['query'] = \
                "find dataset where %s and dataset.status like VALID*" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('run')
            kwds.pop('dataset')
        if  api == 'fakeDataset4User':
            user = kwds['user']
            if  user == 'required':
                kwds['query'] = 'required'
            else:
                val = sitedb.user_dn(kwds['user'])
                if  val:
                    # DBS-QL does not allow = or spaces, so we'll tweak the DN
                    val = val.replace('=', '*').replace(' ', '*')
                    kwds['query'] = "find dataset, dataset.createby " + \
                            "where dataset.createby=%s" % val
                    if  kwds.has_key('dataset') and kwds['dataset']:
                        kwds['query'] += ' and dataset=%s' % kwds['dataset']
                else:
                    kwds['query'] = 'required'
            kwds.pop('user')
            kwds.pop('dataset')
        if  api == 'fakeRun4File':
            val = kwds['file']
            if  val != 'required':
                kwds['query'] = "find run where file = %s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('file')
        if  api == 'fakeFiles4DatasetRunLumis':
            cond = ""
            val = kwds['dataset']
            if  val and val != 'required':
                cond = " and dataset=%s" % val
                kwds.pop('dataset')
            val = kwds['run']
            if  val and val != 'required':
                cond += " and run=%s" % val
                kwds.pop('run')
            val = kwds['lumi']
            if  val and val != 'required':
                cond += " and lumi=%s" % val
                kwds.pop('lumi')
            if  cond:
                kwds['query'] = "find file.name where %s" % cond[4:]
            else:
                kwds['query'] = 'required'
        if  api == 'fakeDatasetSummary' or api == 'fakeDatasetPattern':
            value = ""
            path = False
            for key, val in kwds.iteritems():
                if  key == 'dataset' and val:
                    value += ' and dataset=%s' % val
                    if  len(val.split('/')) == 4: # /a/b/c -> ['','a','b','c']
                        if  val.find('*') == -1:
                            path = True
                if  key == 'primary_dataset' and val:
                    value += ' and primds=%s' % val
                if  key == 'release' and val:
                    value += ' and release=%s' % val
                if  key == 'tier' and val:
                    value += ' and tier=%s' % val
                if  key == 'phygrp' and val:
                    value += ' and phygrp=%s' % val
                if  key == 'datatype' and val:
                    value += ' and datatype=%s' % val
                if  api == 'fakeDatasetPattern':
                    if  key == 'status':
                        if  val:
                            value += ' and dataset.status=%s' % val.upper()
                        else:
                            value += ' and dataset.status like VALID*'
            keys = ['dataset', 'release', 'primary_dataset', 'tier', \
                'phygrp', 'datatype', 'status']
            for key in keys:
                try:
                    del kwds[key]
                except:
                    pass
            if  value:
                query  = "find dataset, datatype, dataset.status, dataset.tag"
                query += ", procds.createdate, procds.createby, procds.moddate"
                query += ", procds.modby"
                if  path: # we have full path, ask for summary information
                    query += ", sum(block.numfiles), sum(block.numevents)"
                    query += ", count(block), sum(block.size)"
                query += " where %s" % value[4:]
                kwds['query'] = query
            else:
                kwds['query'] = 'required'
        if  api == 'fakeListDatasetbyDate':
            value = ''
            if  kwds['status']:
                value = ' and dataset.status=%s' % kwds['status'].upper()
            else:
                value = ' and dataset.status like VALID*'
#           20110126/{'$lte': 20110126}/{'$lte': 20110126, '$gte': 20110124}
            query_for_single = "find dataset, datatype, dataset.status, \
  dataset.tag, \
  dataset.createdate where dataset.createdate %s %s " + value
            query_for_double = "find dataset, datatype, dataset.status, \
  dataset.tag, \
  dataset.createdate where dataset.createdate %s %s \
  and dataset.createdate %s %s " + value
            val = kwds['date']
            qlist = []
            query = ""
            if val != "required":
                if isinstance(val, dict):
                    for opt in val:
                        nopt = dbsql_opt_map(opt)
                        if nopt == ('in'):
                            self.logger.debug(val[opt])
                            nval = [convert_datetime(x) for x in val[opt]]
                        else:
                            nval = convert_datetime(val[opt])
                        qlist.append(nopt)
                        qlist.append(nval)
                    if len(qlist) == 4:
                        query = query_for_double % tuple(qlist)
                    else:
                        msg = "dbs_services::fakeListDatasetbyDate \
 wrong params get, IN date is not support by DBS2 QL"
                        self.logger.info(msg)
                elif isinstance(val, int):
                    val = convert_datetime(val)
                    query = query_for_single % ('=', val)
                kwds['query'] = query
            else:
                kwds['query'] = 'required'
            kwds.pop('date')
        if  api == 'listFiles':
            val = kwds.get('run_number', None)
            if  isinstance(val, dict):
                # listFiles does not support run range, see
                # fakeFiles4DatasetRun API
                kwds['run_number'] = 'required'
            if  not kwds['path'] and not kwds['block_name'] and \
                not kwds['pattern_lfn']:
                kwds['path'] = 'required'
        if  api == 'fakeFiles4DatasetRun' or api == 'fakeFiles4BlockRun':
            cond = ""
            entity = 'dataset'
            if  api == 'fakeFiles4BlockRun':
                entity = 'block'
            val = kwds[entity]
            if  val and val != 'required':
                cond = " and %s=%s" % (entity, val)
                kwds.pop(entity)
            val = kwds['run']
            if  val and val != 'required':
                if  isinstance(val, dict):
                    min_run = 0
                    max_run = 0
                    if  val.has_key('$lte'):
                        max_run = val['$lte']
                    if  val.has_key('$gte'):
                        min_run = val['$gte']
                    if  min_run and max_run:
                        val = "run >=%s and run <= %s" % (min_run, max_run)
                    elif val.has_key('$in'):
                        val = ' or '.join(['run=%s' % r for r in val['$in']])
                        val = '(%s)' % val
                elif isinstance(val, int):
                    val = "run = %d" % val
                cond += " and %s" % val
                kwds.pop('run')
            if  cond:
                kwds['query'] = "find file.name where %s" % cond[4:]
            else:
                kwds['query'] = 'required'

    def parser(self, dasquery, dformat, source, api):
        """
        DBS data-service parser.
        """
        for row in self.parser_helper(dasquery, dformat, source, api):
            if  self.extended_expire:
                new_expire = expire_timestamp(self.extended_expire)
                mod_time = get_modification_time(row)
                if  mod_time and \
                    old_timestamp(mod_time, self.extended_threshold):
                    row.update({'das':{'expire': new_expire}})
            yield row

    def parser_helper(self, dasquery, dformat, source, api):
        """
        DBS data-service parser.
        """
        sitedb = SERVICES.get('sitedb2', None) # SiteDB from global scope
        query = dasquery.mongo_query
        if  api == 'listBlocks':
            prim_key = 'block'
        elif api == 'listBlocks4path':
            api = 'listBlocks'
            prim_key = 'block'
        elif api == 'fakeBlock4file':
            prim_key = 'block'
        elif api == 'listBlockProvenance':
            prim_key = 'block'
        elif api == 'listBlockProvenance4child':
            prim_key = 'block'
        elif api == 'listFiles':
            prim_key = 'file'
        elif api == 'fakeFiles4DatasetRun' or api == 'fakeFiles4BlockRun':
            prim_key = 'file'
        elif api == 'listFileLumis':
            prim_key = 'file_lumi_section'
        elif api == 'listFileProcQuality':
            prim_key = 'file_proc_quality'
        elif api == 'listFileParents':
            prim_key = 'file_parent'
        elif api == 'listTiers':
            prim_key = 'data_tier'
        elif api == 'listDatasetParents':
            prim_key = 'processed_dataset_parent'
        elif api == 'listPrimaryDatasets':
            prim_key = 'primary_dataset'
        elif api == 'listProcessedDatasets':
            prim_key = 'processed_dataset'
        elif api == 'fakeReleases':
            prim_key = 'release'
        elif api == 'listRuns':
            prim_key = 'run'
        elif api == 'fakeLumis4block':
            prim_key = 'lumi'
        elif api == 'fakeLumis4FileRun':
            prim_key = 'lumi'
        elif  api == 'fakeRelease4File':
            prim_key = 'release'
        elif  api == 'fakeRelease4Dataset':
            prim_key = 'release'
        elif  api == 'fakeGroup4Dataset':
            prim_key = 'group'
        elif  api == 'fakeConfig':
            prim_key = 'config'
        elif  api == 'fakeListDataset4Block':
            prim_key = 'dataset'
        elif  api == 'fakeListDataset4File':
            prim_key = 'dataset'
        elif  api == 'fakeListDatasetbyDate':
            prim_key = 'dataset'
        elif  api == 'fakeDatasetPattern':
            prim_key = 'dataset'
        elif  api == 'fakeDatasetSummary':
            prim_key = 'dataset'
        elif  api == 'fakeDataset4Run':
            prim_key = 'dataset'
        elif  api == 'fakeDataset4User':
            prim_key = 'dataset'
        elif  api == 'fakeRun4File':
            prim_key = 'run'
        elif  api == 'fakeRun4Run':
            prim_key = 'run'
        elif api == 'fakeChild4File':
            prim_key = 'child'
        elif api == 'fakeChild4Dataset':
            prim_key = 'child'
        elif api == 'fakeStatus':
            prim_key = 'status'
        elif api == 'fakeFiles4DatasetRunLumis':
            prim_key = 'file'
        elif api == 'fakeRun4Block':
            prim_key = 'run'
        elif api == 'fakeBlock4DatasetRun':
            prim_key = 'block'
        elif api == 'fakeSite4Dataset':
            prim_key = 'site'
        elif api == 'fakeDataset4Site':
            prim_key = 'dataset'
        else:
            msg = 'DBSService::parser, unsupported %s API %s' \
                % (self.name, api)
            raise Exception(msg)
        if  api.find('fake') != -1:
            gen = qlxml_parser(source, prim_key)
        else:
            gen = xml_parser(source, prim_key)
        useless_run_atts = ['number_of_events', 'number_of_lumi_sections', \
                'id', 'total_luminosity', 'store_number', 'end_of_run', \
                'start_of_run']
        config_attrs = ['config.name', 'config.content', 'config.version', \
                 'config.type', 'config.annotation', 'config.createdate', \
                 'config.createby', 'config.moddate', 'config.modby']
        for row in gen:
            if  not row:
                continue
            if  row.has_key('status') and \
                row['status'].has_key('dataset.status'):
                row['status']['name'] = row['status']['dataset.status']
                del row['status']['dataset.status']
            if  row.has_key('file_lumi_section'):
                row['lumi'] = row['file_lumi_section']
                del row['file_lumi_section']
            if  row.has_key('algorithm'):
                del row['algorithm']['ps_content']
            if  row.has_key('processed_dataset') and \
                row['processed_dataset'].has_key('path'):
                if  isinstance(row['processed_dataset']['path'], dict) \
                and row['processed_dataset']['path'].has_key('dataset_path'):
                    path = row['processed_dataset']['path']['dataset_path']
                    del row['processed_dataset']['path']
                    row['processed_dataset']['name'] = path
            # case for fake apis
            # remove useless attribute from results
            if  row.has_key('dataset'):
                if  row['dataset'].has_key('count_file.size'):
                    del row['dataset']['count_file.size']
                if  row['dataset'].has_key('dataset'):
                    name = row['dataset']['dataset']
                    del row['dataset']['dataset']
                    row['dataset']['name'] = name
            if  row.has_key('child') and row['child'].has_key('dataset.child'):
                row['child']['name'] = row['child']['dataset.child']
                del row['child']['dataset.child']
            if  row.has_key('child') and row['child'].has_key('file.child'):
                row['child']['name'] = row['child']['file.child']
                del row['child']['file.child']
            if  row.has_key('block') and query.get('fields') == ['parent']:
                row['parent'] = row['block']
                del row['block']
            if  row.has_key('block') and query.get('fields') == ['child']:
                row['child'] = row['block']
                del row['block']
            if  row.has_key('run') and row['run'].has_key('run'):
                row['run']['run_number'] = row['run']['run']
                del row['run']['run']
            if  row.has_key('release') and row['release'].has_key('release'):
                row['release']['name'] = row['release']['release']
                del row['release']['release']
            if  row.has_key('site'):
                if  row['site'].has_key('site'):
                    row['site']['se'] = row['site']['site']
                    del row['site']['site']
            convert_dot(row, 'config', config_attrs)
            convert_dot(row, 'file', ['file.name'])
            convert_dot(row, 'block', ['block.name'])
            convert_dot(row, 'dataset', ['dataset.tag', 'dataset.status'])
            # remove DBS2 run attributes (to be consistent with DBS3 output)
            # and let people extract this info from CondDB/LumiDB.
            if  row.has_key('run'):
                for att in useless_run_atts:
                    try:
                        del row['run'][att]
                    except:
                        pass
            if  api == 'fakeLumis4FileRun':
                if  row.has_key('lumi'):
                    row = row['lumi']
                    row['lumi'] = {'number': row['lumi.number'],
                                   'run_number': row['run.number']}
                    del row['lumi.number']
                    del row['run.number']
            if  api == 'fakeLumis4block':
                if  row.has_key('lumi'):
                    row = row['lumi']
                    row['lumi'] = {'number': row['lumi.number'],
                                   'run_number': row['run.number'],
                                   'file': row['file.name']}
                    del row['lumi.number']
                    del row['run.number']
                    del row['file.name']
            if  api == 'fakeSite4Dataset' and sitedb:
                site = row.get('site', None)
                if  site and isinstance(site, dict):
                    sename = site.get('se', None)
                    info = sitedb.site_info(sename)
                    if  info:
                        row['site'].update(info)
            if  api == 'fakeDataset4Site':
                sename = row['dataset'].get('site')
                row.update({'site':{'se':sename}})
                del row['dataset']['site']
            if  api == 'listFiles':
                if  query.has_key('spec'):
                    if  query['spec'].has_key('status.name'):
                        spec_status = query['spec']['status.name']
                        if  row.has_key('file'):
                            file_status = row['file'].get('status', '')
                            if  file_status.lower() != spec_status.lower():
                                row = None
            if  row:
                yield row
