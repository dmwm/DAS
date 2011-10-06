#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
SiteDB service
"""
__author__ = "Valentin Kuznetsov"

# system modules
import time
from   types import InstanceType

# DAS modules
import DAS.utils.jsonwrapper as json
from   DAS.services.abstract_service import DASAbstractService
from   DAS.utils.utils import map_validator, print_exc, genkey, expire_timestamp
from   DAS.utils.url_utils import getdata
from   DAS.core.das_core import dasheader

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
        raise Exception('Wrong data type, %s' % type(data))
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
        cname = genkey(url)
        col   = self.localcache.conn[self.name][cname]
        local = col.find_one({'expire':{'$gt':expire_timestamp(time.time())}})
        data  = None
        if  local:
            msg = 'SiteDBService reads from %s.%s' % (self.name, cname)
            self.logger.info(msg)
            try: # get data from local cache
                data = [r for r in col.find() if not r.has_key('expire')][0]
                del data['_id']
            except Exception as exc:
                print_exc(exc)
                data = {}
        if  not data or not local:
            headers = {'Accept':'application/json'}
            datastream, expire = getdata(url, params, headers, expire, post,
                    self.error_expire, self.verbose, self.ckey, self.cert)
            try: # read data and write it to local cache
                data = json.load(datastream)
                col.remove()
                col.insert(data)
                col.insert({'expire':expire_timestamp(expire)})
            except Exception as exc:
                print_exc(exc)
        return data, expire

    def adjust_params(self, api, kwds, _inst=None):
        """
        Adjust SiteDB parameters for specific query requests
        """
        # For SiteDB we will use bulk API, so delete all
        # parameters for API call
        if  api:
            try:
                for key in kwds.keys():
                    del kwds[key]
            except:
                pass

    def parser(self, query, dformat, source, api):
        """
        SiteDB data-service parser.
        """
        spec = query.get('spec')
        if  spec.has_key('site.name'):
            for row in sitedb_parser(source):
                name = row.get('name', None)
                if  name and spec['site.name'].find(name) != -1:
                    yield dict(site=row)
        elif spec.has_key('user.name'):
            qname = spec['user.name']
            for row in sitedb_parser(source):
                username = row.get('username', None)
                forename = row.get('forename', None)
                surname  = row.get('surname', None)
                email    = row.get('email', None)
                if  username and qname.find(username) != -1 or \
                    forename and qname.find(forename) != -1 or \
                    surname and qname.find(surname) != -1 or \
                    email and qname.find(email) != -1:
                    yield dict(user=row)
