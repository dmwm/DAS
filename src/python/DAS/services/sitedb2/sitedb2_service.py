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

    def getdata_helper(self, url, params, expire, headers=None, post=None):
        "Helper function to get data from SiteDB or local cache"
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

    def getdata(self, url, params, expire, headers=None, post=None):
        "SiteDB call wrapper"
        if  url.split('/')[-1] == 'site-names':
            data, expire = self.getdata_helper(\
                        url, params, expire, headers, post)
            result = [] # output results
            # get site personel info
            newurl = url.replace('site-names', 'site-responsibilities')
            rdata, expire = self.getdata_helper(\
                        newurl, params, expire, headers, post)
            for row in sitedb_parser(data):
                row['name'] = row['alias']
                del row['alias']
                row['admin'] = []
                for rec in sitedb_parser(rdata):
                    if  rec['site'] == row['site_name']:
                        row['admin'].append(\
                                dict(role=rec['role'], email=rec['email']))
                result.append(row)
            # get sites info
            newurl = url.replace('site-names', 'sites')
            rdata, expire = self.getdata_helper(\
                        newurl, params, expire, headers, post)
            for row in result:
                row['info'] = []
                for rec in sitedb_parser(rdata):
                    if  rec['name'] == row['site_name']:
                        del rec['name']
                        row['info'].append(rec)
            # get site resource info
            newurl = url.replace('site-names', 'site-resources')
            rdata, expire = self.getdata_helper(\
                        newurl, params, expire, headers, post)
            for row in result:
                row['resources'] = []
                for rec in sitedb_parser(rdata):
                    if  rec['name'] == row['site_name']:
                        del rec['name']
                        row['resources'].append(rec)
            return dict(result=result), expire
        else:
            return self.getdata_helper(url, params, expire, headers, post)

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

    def parser(self, dasquery, dformat, source, api):
        """
        SiteDB data-service parser.
        """
        spec = dasquery.mongo_query.get('spec')
        if  spec.has_key('site.name'):
            spec_name = spec['site.name'].replace('*', '')
            for row in sitedb_parser(source):
                name  = row.get('name', None)
                alias = row.get('alias', None)
                if  name and name.find(spec_name) != -1:
                    yield dict(site=row)
                elif alias and alias.find(spec_name) != -1:
                    yield dict(site=row)
        elif spec.has_key('group.name'):
            for row in sitedb_parser(source):
                group = spec['group.name'].lower()
                name = row.get('name', None)
                if  not name:
                    name = row.get('user_group', None)
                if  name and group.find(name.lower()) != -1:
                    if  row.has_key('user_group'):
                        row['name'] = row['user_group']
                        del row['user_group']
                    yield row
        elif spec.has_key('user.email'):
            for row in sitedb_parser(source):
                uemail = spec['user.email'].lower()
                email = row.get('email', None)
                if  email and uemail.find(email.lower()) != -1:
                    yield row
        elif spec.has_key('user.name'):
            qname = spec['user.name'].lower()
            for row in sitedb_parser(source):
                username = row.get('username', None)
                forename = row.get('forename', None)
                surname  = row.get('surname', None)
                email    = row.get('email', None)
                if  username and qname == username.lower() or \
                    forename and qname == forename.lower() or \
                    surname and qname == surname.lower():
                    yield dict(user=row)
