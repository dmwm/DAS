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
__author__ = "Valentin Kuznetsov"

# system modules
import time

# DAS modules
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, json_parser
from DAS.utils.utils import expire_timestamp
from DAS.utils.url_utils import getdata

def get_modification_time(record):
    "Get modification timestamp from DBS data-record"
    for key in ['dataset', 'block', 'file']:
        if  record.has_key(key):
            if  record[key].has_key('last_modification_date'):
                return record[key]['last_modification_date']
    if  record.has_key('last_modification_date'):
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
        self.prim_instance = config['dbs']['dbs_global_instance']
        self.instances = config['dbs']['dbs_instances']
        self.extended_expire = \
                expire_timestamp(config['dbs'].get('extended_expire', 86400))
        self.extended_threshold = \
                config['dbs'].get('extended_threshold', 2592000) # 1 month

    def getdata(self, url, params, expire, headers=None, post=None):
        """URL call wrapper"""
        headers =  {'Accept': 'application/json' } # DBS3 always needs that
        return getdata(url, params, headers, expire, post,
                self.error_expire, self.verbose, self.ckey, self.cert,
                doseq=False, system=self.name)

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
                    kwds['minrun'] = val['$in'][0]
                    kwds['maxrun'] = val['$in'][-1]
                if  val.has_key('$lte'):
                    kwds['minrun'] = val['$gte']
                    kwds['maxrun'] = val['$lte']
        if  api == 'file4DatasetRunLumi':
            val = kwds['run']
            if  val:
                kwds['minrun'] = val
                kwds['maxrun'] = val
            try:
                del kwds['run']
            except:
                pass
            val = kwds['lumi_list']
            if  val:
                kwds['lumi_list'] = [val]

    def parser(self, query, dformat, source, api):
        """
        DBS3 data-service parser.
        """
        for row in self.parser_helper(query, dformat, source, api):
            mod_time = get_modification_time(row)
            if  mod_time and self.extended_expire and \
                old_timestamp(mod_time, self.extended_threshold):
                row.update({'das':{'expire': self.extended_expire}})
            # filesummaries is summary DBS API about dataset,
            # it collects information about number of files/blocks/events
            # for given dataset and therefore will be merged with datasets
            # API record. To make a proper merge with extended
            # timestamp/threshold options I need explicitly assign
            # das.expire=extended_timestamp, otherwise
            # the merged record will pick-up smallest between
            # filesummaries and datasets records.
            if  api == 'filesummaries' and self.extended_expire:
                row.update({'das': {'expire': self.extended_expire}})
            yield row

    def parser_helper(self, query, dformat, source, api):
        """
        DBS3 data-service parser helper, it is used by parser method.
        """
        if  api == 'site4dataset':
            sites = set()
            for rec in json_parser(source, self.logger):
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
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                row['name'] = row['dataset']
                del row['dataset']
                yield {'dataset':row}
        elif api == 'filesummaries':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            name = query.mongo_query['spec']['dataset.name']
            for row in gen:
                row['dataset']['name'] = name
                yield row
        elif api == 'blocks4site':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                print "\n### please revisit, row=", row
                yield row
        elif api == 'blockparents':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                try:
                    del row['parent']['this_block_name']
                except:
                    pass
                yield row
        elif api == 'fileparents':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                parent = row['parent']
                for val in parent['parent_logical_file_name']:
                    yield dict(name=val)
        elif api == 'runs_via_dataset':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                values = row['run']['run_num']
                if  isinstance(values, list):
                    for val in values:
                        yield dict(run_number=val)
                else:
                    yield dict(run_number=values)
        elif api == 'filechildren':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                parent = row['child']
                for val in parent['child_logical_file_name']:
                    yield dict(name=val)
        else:
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                yield row
