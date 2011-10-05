#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
SiteDB service
"""
__author__ = "Valentin Kuznetsov"

# system modules
from   types import InstanceType

# DAS modules
import DAS.utils.jsonwrapper as json
from   DAS.services.abstract_service import DASAbstractService
from   DAS.utils.utils import map_validator, print_exc
from   DAS.utils.url_utils import getdata

def rowdict(columns, row):
    """Convert given row list into dict with column keys"""
    robj = {}
    for key, val in zip(columns, row):
        robj.setdefault(key, val)
    return robj
    
def sitedb_parser(source):
    """SiteDB parser"""
    if  isinstance(source, str) or isinstance(source, unicode):
        data = json.loads(source)
    elif isinstance(source, InstanceType) or isinstance(source, file):
        # got data descriptor
        try:
            data = json.load(source)
        except Exception as exc:
            print_exc(exc)
            source.close()
            raise
        source.close()
    else:
        data = source
    if  not isinstance(data, dict):
        raise Exception('Wrong data type')
    if  data.has_key('desc'):
        columns = data['desc']['columns']
        for row in data['result']:
            yield rowdict(columns, row)
    else:
        for row in data['result']:
            yield row

class SiteDBService(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'sitedb2', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def getdata(self, url, params, expire, headers=None, post=None):
        """URL call wrapper"""
        headers = {'Accept':'application/json'}
        return getdata(url, params, headers, expire, post,
                self.error_expire, self.verbose, self.ckey, self.cert)

    def adjust_params(self, api, kwds, _inst=None):
        """
        Adjust SiteDB parameters for specific query requests
        """
        if  api.find('site') != -1:
            try:
                for key in kwds.keys():
                    del kwds[key]
            except:
                pass

    def parser(self, query, dformat, source, api):
        """
        SiteDB data-service parser.
        """
        print "\n### NewSiteDB parser", query, dformat, source, api
        spec = query.get('spec')
        if  spec.has_key('site.name'):
            for row in sitedb_parser(source):
                name = row.get('name', None)
                if  name and spec['site.name'].find(name) != -1:
                    yield dict(site=row)
