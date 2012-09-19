#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS service
"""
__revision__ = "$Id: dbs_service.py,v 1.24 2010/04/09 19:41:23 valya Exp $"
__version__ = "$Revision: 1.24 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, json_parser
from DAS.utils.url_utils import getdata

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
        elif api == 'filesummaries':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                yield row['dataset']
        elif api == 'blocks4site':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                print row # TODO: this need to be revisited once DBS3 provides new API
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
