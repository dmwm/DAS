#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
SiteDB service
"""
__revision__ = "$Id: sitedb_service.py,v 1.20 2010/02/25 14:49:59 valya Exp $"
__version__ = "$Revision: 1.20 $"
__author__ = "Valentin Kuznetsov"

import re
import types
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import add2dict, map_validator
from DAS.utils.utils import row2das
import DAS.utils.jsonwrapper as json

class SiteDBService(DASAbstractService):
    """
    Helper class to provide SiteDB service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'sitedb', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def parser(self, dformat, source, api):
        """
        Parser for SiteDB JSON data-services
        """
        if  type(source) is types.InstanceType:
            data = source.read()
            source.close()
        else:
            data = source

        try:
            jsondict = json.loads(data)
        except:
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
            yield {'site': row}

