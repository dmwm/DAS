#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
SiteDB service
"""
__revision__ = "$Id: sitedb_service.py,v 1.11 2009/09/01 01:42:47 valya Exp $"
__version__ = "$Revision: 1.11 $"
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
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def parser(self, api, data, params=None):
        """
        Parser for SiteDB JSON data-services
        """
        jsondict = eval(data)
        pat = re.compile('T[0-9]_')
        for key, val in jsondict.items():
            if  api == 'CMSNametoAdmins':
                row = {'admin':val}
#            elif api == 'CEtoCMSName':
#                row = {'name': val['name']}
            elif api == 'SEtoCMSName':
                row = {'name': val['name']}
            elif api == 'CMStoSAMName':
                row = {'samname': val['name']}
            elif api == 'CMStoSiteName':
                row = {'sitename': val['name']}
            elif api == 'CMSNametoCE':
                row = {'ce': val['name']}
            elif api == 'CMSNametoSE':
                row = {'se': val['name']}
#            elif api == 'CMSNametoPhEDExNode':
#                row = {'node': val['phedex_node']}
            elif api == 'SiteStatus':
                row = val
            else:
                raise Exception('Not implemented yet')
            if  params:
                for key, val in params.items():
                    if  val:
                        if  pat.match(val):
                            if  not row.has_key('cmsname'):
                                row['cmsname'] = val
                        if  val.find('.') != -1: # SE or CE
                            if  not row.has_key('se'):
                                row['se'] = val
            yield {'site': row}

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
        res = self.getdata(url, params)
        if  type(res) is types.GeneratorType:
            res = [i for i in res][0]
        data  = eval(res)
        return data
