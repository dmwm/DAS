#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
SiteDB service
"""
__revision__ = "$Id: sitedb_service.py,v 1.12 2009/09/01 17:06:16 valya Exp $"
__version__ = "$Revision: 1.12 $"
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

#    def clean_params(self, api, params):
#        """
#        Clean all parameters to get as much as possible information
#        from SiteDB. Skip only those which marked as required.
#        """
#        args = {}
#        for key, val in self.map[api]['params'].items():
#            if  val != 'required':
#                args[key] = val
#            else:
#                args[key] = params[key]
#        return args

    def parser(self, api, data, params=None):
        """
        Parser for SiteDB JSON data-services
        """
        cache = {}
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
            self.row2das(self.name, row)
            yield {'site': row}
