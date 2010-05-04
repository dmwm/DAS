#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
SiteDB service
"""
__revision__ = "$Id: sitedb_service.py,v 1.16 2009/11/20 00:58:32 valya Exp $"
__version__ = "$Revision: 1.16 $"
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

    def patterns(self, api, args):
        """
        Define how to deal with parameters when they contain
        wild-card.
        """
        params = dict(args)
        for key, val in params.items():
            if  type(val) is types.StringType or type(val) is types.UnicodeType:
                if  val.find('*') != -1:
                    params[key] = val.replace('*', '')
        return params

    def parser(self, source, api, params=None):
        """
        Parser for SiteDB JSON data-services
        """
        close = False
        if  type(source) is types.InstanceType:
            data = source.read()
            close = True
        else:
            data = source

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
#            self.row2das(self.name, "", row) # here we pass empty api=""
            yield {'site': row}

            if  close:
                source.close()
