#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0702,R0913,R0912,R0915,R0904
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

# TODO: I need to implement support for IN operator, see DBS3 tickets:
#       https://svnweb.cern.ch/trac/CMSDMWM/ticket/4136
#       https://svnweb.cern.ch/trac/CMSDMWM/ticket/4157

__author__ = "Valentin Kuznetsov"

# system modules
import time
import urllib
from   types import GeneratorType

# DAS modules
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, json_parser
from DAS.utils.utils import expire_timestamp, convert2ranges
from DAS.utils.url_utils import getdata, proxy_getdata
import DAS.utils.jsonwrapper as json

def get_api(url):
    "Extract from DBS3 URL the api name"
    if  url[-1] == '/':
        url = url[:-1]
    return url.split('/')[-1]

def get_modification_time(record):
    "Get modification timestamp from DBS data-record"
    for key in ['dataset', 'block', 'file']:
        if  record.has_key(key):
            obj = record[key]
            if  isinstance(obj, dict) and obj.has_key('last_modification_date'):
                return record[key]['last_modification_date']
    if  isinstance(record, dict) and record.has_key('last_modification_date'):
        return record['last_modification_date']
    return None

def old_timestamp(tstamp, threshold=2592000):
    "Check if given timestamp is old enough"
    if  not threshold:
        return False
    if  tstamp < (time.mktime(time.gmtime())-threshold):
        return True
    return False

class DBS3Service(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dbs3', config)
        self.reserved = ['api', 'apiversion']
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)
        self.prim_instance = self.dasmapping.dbs_global_instance()
        self.instances = self.dasmapping.dbs_instances()
        self.extended_expire = config['dbs'].get('extended_expire', 0)
        self.extended_threshold = config['dbs'].get('extended_threshold', 0)

    def getdata_helper(self, url, params, expire, headers=None):
        "URL call wrapper"
        post = None
        if  not headers:
            headers =  {'Accept': 'application/json' } # DBS3 always needs that
        return getdata(url, params, headers, expire, post,
                self.error_expire, self.verbose, self.ckey, self.cert,
                doseq=False, system=self.name)

    def getdata(self, url, params, expire, headers):
        """URL call wrapper"""
        if  isinstance(url, list):
            # multi-stage workflows
            if  [get_api(u) for u in url] == ['blocks', 'filelumis']:
                edict = {'expire':expire}
                dataset = params.get('dataset')
                res = self.file_lumi_helper(url, dataset, edict)
                return res, edict.get('expire')
        return self.getdata_helper(url, params, expire, headers)

    def file_lumi_helper(self, url, dataset, edict):
        "Specialized DBS3 helper to get file, lumi pairs for a given dataset"
        params  = {'dataset': dataset}
        expire  = edict.get('expire')
        data, _ = self.getdata_helper(url[0], params, expire)
        blocks  = json.load(data)

        # TEST data_proxy, please note that urls must be strings
        # and not unicode (therefore we use encode function for conversion)
#        blocks = (r['block_name'] for r in blocks)
#        urls = ('%s?block_name=%s' % (url[1], urllib.quote(b)) \
#                for b in blocks)
#        gen = proxy_getdata(urls)
#        for filelumis in gen:
#            if  isinstance(filelumis, list):
#                for row in filelumis:
#                    lumi = row.get('lumi_section_num', None)
#                    lfn  = row.get('logical_file_name', None)
#                    rec  = {'lumi':{'number':lumi}, 'file':{'name':lfn}}
#                    yield rec
#            else:
#                lumi = row.get('lumi_section_num', None)
#                lfn  = row.get('logical_file_name', None)
#                rec  = {'lumi':{'number':lumi}, 'file':{'name':lfn}}
#                yield rec

        for row in blocks:
            params = {'block_name': row['block_name']}
            val, expire = self.getdata_helper(url[1], params, expire)
            filelumis = json.load(val)
            edict.update({'expire': expire})
            if  isinstance(filelumis, list):
                for row in filelumis:
                    lumi = row.get('lumi_section_num', None)
                    lfn  = row.get('logical_file_name', None)
                    rec  = {'lumi':{'number':lumi}, 'file':{'name':lfn}}
                    yield rec
            else:
                lumi = row.get('lumi_section_num', None)
                lfn  = row.get('logical_file_name', None)
                rec  = {'lumi':{'number':lumi}, 'file':{'name':lfn}}
                yield rec

    def url_instance(self, url, instance):
        """
        Adjust URL for a given instance
        """
        if  instance in self.instances:
            if  isinstance(url, basestring):
                return url.replace(self.prim_instance, instance)
            elif isinstance(url, list):
                urls = [u.replace(self.prim_instance, instance) for u in url]
                return urls
        return url

    def adjust_params(self, api, kwds, inst=None):
        """
        Adjust DBS2 parameters for specific query requests
        """
        if  api == 'site4dataset':
            # skip API call if inst is global one (data provided by phedex)
            if  inst == self.prim_instance:
                kwds['dataset'] = 'required' # we skip
        if  api == 'acquisitioneras':
            try:
                del kwds['era']
            except KeyError:
                pass
        if  api == 'datasets':
            if  kwds['dataset'][0] == '*':
                kwds['dataset'] = '/' + kwds['dataset']
            if  kwds['dataset'] == '*' and kwds['block_name']:
                kwds['dataset'] = kwds['block_name'].split('#')[0]
            if  kwds['cdate']:
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
            try:
                del kwds['block_name']
            except KeyError:
                pass
        if  api == 'runs':
            val = kwds['minrun']
            if  isinstance(val, dict): # we got a run range
                if  val.has_key('$in'):
                    # TODO: this should be corrected when DBS will support
                    # run-ranges
                    kwds['minrun'] = val['$in'][0]
                    kwds['maxrun'] = val['$in'][-1]
                if  val.has_key('$lte'):
                    kwds['minrun'] = val['$gte']
                    kwds['maxrun'] = val['$lte']
        if  api == 'file4DatasetRunLumi':
            val = kwds.get('run', None)
            if  val:
                if  isinstance(val, dict): # we got a run range
                    if  val.has_key('$in'):
                        # TODO: this should be corrected when DBS will support
                        # run-ranges
                        kwds['minrun'] = val['$in'][0]
                        kwds['maxrun'] = val['$in'][-1]
                    if  val.has_key('$lte'):
                        kwds['minrun'] = val['$gte']
                        kwds['maxrun'] = val['$lte']
                else:
                    kwds['minrun'] = val
                    kwds['maxrun'] = val
                del kwds['run']
            val = kwds['lumi_list']
            if  val:
                kwds['lumi_list'] = [val]
        if  api == 'files' or api == 'files_via_dataset' or \
            api == 'files_via_block':
            # we can't pass status input parameter to DBS3 API since
            # it does not accepted, instead it will be used for filtering
            # end-results
            del kwds['status']

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
                    row.update({'das':{'expire': self.new_expire}})
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
        if  api == 'site4dataset':
            gen = json_parser(source, self.logger)
        else:
            gen = DASAbstractService.parser(self, query, dformat, source, api)
        if  api == 'site4dataset':
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
        elif api == 'datasets':
            for row in gen:
                row['name'] = row['dataset']
                del row['dataset']
                yield {'dataset':row}
        elif api == 'filesummaries':
            name = query.mongo_query['spec']['dataset.name']
            for row in gen:
                row['dataset']['name'] = name
                yield row
        elif api == 'summary4run':
            spec = query.mongo_query.get('spec', {})
            dataset = spec.get('dataset.name', '')
            block = spec.get('block.name', '')
            run = spec.get('run.run_number', 0)
            for row in gen:
                if  run:
                    row.update({"run": run})
                if  dataset:
                    row.update({"dataset": dataset})
                if  block:
                    row.update({"block": block})
                yield row
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
        elif api == 'runs_via_dataset':
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
            mongo_query = query.mongo_query
            status = 'VALID'
            for row in gen:
                if  query.mongo_query.has_key('spec'):
                    if  query.mongo_query['spec'].has_key('status.name'):
                        status = query.mongo_query['spec']['status.name']
                file_status = row['file']['is_file_valid']
                if  status == 'INVALID': # filter out valid files
                    if  int(file_status) == 1:# valid status
                        row = None
                else: # filter out invalid files
                    if  int(file_status) == 0:# invalid status
                        row = None
                if  row:
                    yield row
        elif api == 'filelumis' or api == 'filelumis4block':
            for row in gen:
                if  row.has_key('lumi'):
                    if  row['lumi'].has_key('lumi_section_num'):
                        val = row['lumi']['lumi_section_num']
                        row['lumi']['lumi_section_num'] = convert2ranges(val)
                    yield row
                else:
                    yield row
        else:
            for row in gen:
                yield row
