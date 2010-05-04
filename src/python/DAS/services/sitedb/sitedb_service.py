#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
SiteDB service
"""
__revision__ = "$Id: sitedb_service.py,v 1.7 2009/05/15 14:19:59 valya Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "Valentin Kuznetsov"

import re
import types
from DAS.services.abstract_service import DASAbstractService
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
                'keys': ['site'],
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

    def adjust(self, apidict):
        """
        Fix apidict if API requires CMS name and provided name is se one.
        And to reverse if necessary.
        """
        pat = re.compile('^CMSName')
        for api, params in apidict.items():
            if  pat.match(api) and params.has_key('name'):
                nlist = []
                for val in params['name']:
                    if  val.find('.') != -1:
                        newval = self.se2cms(val)
                        nlist.append(newval)
                if  nlist:
                    params['name'] = nlist

    def adjust_result(self, api, idict):
        """
        Convert sitedb dict into expected dict structure
        {'key':[results]}.
        """
        jsondict = {}
        for key in self.map[api]['keys'] :
            jsondict[key] = idict.values()
        return jsondict

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

    def call_service_api(self, apiname, params):
        """
        Call SiteDB API, since its return type is JSON, we eval the results to
        get back the dictionary.
        """
        url = self.url + '/' + apiname
        data  = eval(self.getdata(url, params))
        return data
