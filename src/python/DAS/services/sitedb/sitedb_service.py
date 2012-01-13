#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
SiteDB service
"""
__revision__ = "$Id: sitedb_service.py,v 1.21 2010/03/19 17:25:49 valya Exp $"
__version__ = "$Revision: 1.21 $"
__author__ = "Valentin Kuznetsov"

from   types import InstanceType

from   DAS.services.abstract_service import DASAbstractService
from   DAS.utils.utils import map_validator
from   DAS.utils.utils import print_exc

class SiteDBService(DASAbstractService):
    """
    Helper class to provide SiteDB service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'sitedb', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def adjust_params(self, api, kwds, inst=None):
        """
        Adjust Phedex parameters for specific query requests
        """
        for key, val in kwds.iteritems():
            kwds[key] = val.replace('*', '')

    def parser(self, query, dformat, source, api):
        """
        Parser for SiteDB JSON data-services
        """
        if  isinstance(source, InstanceType):
            data = source.read()
            source.close()
        else:
            data = source
        # SiteDB incorrectly decode json. So instead of trying json.loads
        # I switched directly to eval its data (brrr, I know its bad)
        # but barking json is not good either
        try:
            jsondict = eval(data, { "__builtins__": None }, {})
        except Exception as err:
            print_exc(err)
            jsondict = {}
        for _key, val in jsondict.iteritems():
            if  api == 'CMSNametoAdmins':
                row = {'admin':val}
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
            elif api == 'SiteStatus':
                row = val
            else:
                raise Exception('Not implemented yet')
            yield {'site': row}

