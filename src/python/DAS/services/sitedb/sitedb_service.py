#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
SiteDB service
"""
__revision__ = "$Id: sitedb_service.py,v 1.5 2009/05/11 20:21:20 valya Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "Valentin Kuznetsov"

import re
import types
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import query_params, cartesian_product
from DAS.utils.utils import add2dict, map_validator

class SiteDBService(DASAbstractService):
    """
    Helper class to provide SiteDB service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'sitedb', config)
        self.map = {
            'CMSNametoAdmins' : {
                'keys': ['admin'],
                'params' : {'name':''}
            },
            'SEtoCMSName' : {
                'keys': ['site.cmsname'],
                'params' : {'name':''}
            },
            'CMStoSAMName' : {
                'keys': ['site.samname'],
                'params' : {'name':''}
            },
            'CMStoSiteName' : {
                'keys': ['site.sitename'],
                'params' : {'name':''}
            },
            'CMSNametoCE' : {
                'keys': ['site.cename'],
                'params' : {'name':''}
            },
            'CMSNametoSE' : {
                'keys': ['site.sename', 'site'],
                'params' : {'name':''}
            },
            'CMSNametoPhEDExNode' : {
                'keys': ['site.phedexname'],
                'params' : {'cms_name':''}
            },
            'SiteStatus' : {
                'keys': ['site.status'],
                'params' : {'cms_name':''}
            },
        }
        map_validator(self.map)

    def se2cms(self, site_se):
        """
        Convert SE to CMS names.
        """
        args = {'name': site_se}
        jsondict = self.call_service_api('SEtoCMSName', args)
        for value in jsondict.values():
            return value['name']

    def cms2se(self, cmsname):
        """
        Convert CMS name to SE.
        """
        args = {'name':cmsname}
        jsondict = self.call_service_api('CMSNametoSE', args)
        selist = []
        for value in jsondict.values():
            se = value['name']
            if  not selist.count(se):
                selist.append(se)
        return selist

    def findapi(self, key, site):
        """
        Find SiteDB api corresponding to given key
        """
        api  = ""
        args = {}
        for api, val in self.map.items():
            keys = val['keys']
            args = val['params']
            if  keys.count(key):
                for k in args.keys():
                    if  api == 'SEtoCMSName':
                        args['name'] = self.site2se(site)
                    else:
                        args[k] = self.cmsname(site)
                break
        return api, args

    def site2se(self, site):
        """
        convert given site name to SE
        """
        if  site == '':
            return ''
        # TODO: extend to cover SAM, phedex, site, etc. names
        pat = re.compile('^T[0-9]_')
        if  pat.match(site):
            sitese = self.cms2se(site)
        else:
            sitese = site
        return sitese
        
    def cmsname(self, site):
        """
        convert given site name to CMS name
        """
        if  site == '':
            return ''
        # TODO: extend to cover SAM, phedex, site, etc. names
        if  site.count(".") >= 2:
            sitese = self.se2cms(site)
        else:
            sitese = site
        return sitese

    def api(self, query, cond_dict=None):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        selkeys, cond = query_params(query)
        params = {}
        for key in cond.keys():
            oper, val = cond[key]
            if  oper == '=':
                params[key] = val
            else:
                raise Exception("DAS::%s, not supported operator '%s'" \
                % (self.name, oper))
        for key in cond_dict:
            if  not params.has_key(key):
                params[key] = cond_dict[key]

        if  params.has_key('site'):
            sitename = params['site']
        else:
            sitename = ''
        if  type(sitename) is types.ListType:
            sitelist = list(sitename)
        else:
            if  not sitename:
                sitelist = self.cms2se()
            else:
                sitelist = [sitename]

        data = []
        resdict = {}
        # HACK to use 1 arg and 1 sel keys
        if  len(sitelist) == 1 and len(selkeys) == 1:
            api, args = self.findapi(selkeys[0], sitelist[0])
            jsondict  = self.call_service_api(api, args)
            data = []
            for key, val in jsondict.items():
                row = {selkeys[0]: val['name']}
                data.append(row)
            return data
        # TODO: review how to abstract SiteDB
        for site in sitelist:
            for key in selkeys:
#                if  key == 'site':
#                    continue
                api, args = self.findapi(key, site)
                jsondict = self.call_service_api(api, args)
                data = getattr(self, 'parser_%s' % api)(jsondict, site)
                add2dict(resdict, api, data)
        # create cartesian product of out results
        rel_keys = ['site']
        data = self.product(resdict, rel_keys)
        return data

    def call_service_api(self, apiname, params):
        """
        Call SiteDB API, since its return type is JSON, we eval the results to
        get back the dictionary.
        """
        url = self.url + '/' + apiname
        data  = eval(self.getdata(url, params))
        return data

    def parser_CMSNametoAdmins(self, jsondict, site):
        """
        Parse data from CMSNametoAdmins SiteDB API
        """
        newrow  = {}
        data    = []
        sites   = self.site2se(site)
        for val in jsondict.values():
            row = dict(newrow)
            res = '%s %s (%s)' % \
            (val['forename'], val['surname'], val['email'])
            row['admin'] = res
            if  type(sites) is types.StringType:
                row['site'] = sites
                if  not data.count(row):
                    data.append(row)
            else:
                for se in sites:
                    serow = dict(row)
                    serow['site'] = se
                    if  not data.count(serow):
                        data.append(serow)
        return data
    
    def parser_common(self, key, jsondict, site, name='name'):
        """
        Common parser for data from SiteDB API
        """
        newrow = {}
        data = []
        for val in jsondict.values():
            row = dict(newrow)
            res = val[name]
            row[key] = res
            row['site'] = self.site2se(site)
            data.append(row)
        return data

    def parser_SEtoCMSName(self, jsondict, site):
        """
        Parse data from SEtoCMSName SiteDB API
        """
        return self.parser_common('site.cmsname', jsondict, site)

    def parser_CMStoSAMName(self, jsondict, site):
        """
        Parse data from CMStoSAMName SiteDB API
        """
        return self.parser_common('site.samname', jsondict, site)

    def parser_CMStoSiteName(self, jsondict, site):
        """
        Parse data from CMStoSiteName SiteDB API
        """
        return self.parser_common('site.sitename', jsondict, site)

    def parser_CMSNametoCE(self, jsondict, site):
        """
        Parse data from CMSNametoCE SiteDB API
        """
        return self.parser_common('site.cename', jsondict, site)

    def parser_CMSNametoSE(self, jsondict, site):
        """
        Parse data from CMSNametoSE SiteDB API
        """
        return self.parser_common('site.sename', jsondict, site)

    def parser_CMSNametoPhEDExNode(self, jsondict, site):
        """
        Parse data from CMSNametoPhEDExNode SiteDB API
        """
        return self.parser_common('site.phedexname', jsondict, site, 
                                  'phedex_node')

    def parser_SiteStatus(self, jsondict, site):
        """
        Parse data from SiteStatus SiteDB API
        """
        return self.parser_common('site.status', jsondict, site, 'status')

