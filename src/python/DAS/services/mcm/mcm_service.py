#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
MCM data-service plugin.
"""
__author__ = "Valentin Kuznetsov"

import re
import DAS.utils.jsonwrapper as json
from   DAS.services.abstract_service import DASAbstractService
from   DAS.utils.utils import map_validator
from   DAS.utils.url_utils import getdata

class MCMService(DASAbstractService):
    """
    Helper class to provide MCM data-service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'mcm', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def getdata(self, url, params, expire, headers=None, post=None):
        """URL call wrapper"""
        if  not headers:
            headers =  {'Accept': 'application/json' } # DBS3 always needs that
        # MCM uses rest API
        url = '%s/%s' % (url, params.get('prepid'))
        params = {}
        return getdata(url, params, headers, expire, post,
                self.error_expire, self.verbose, self.ckey, self.cert,
                doseq=False, system=self.name)
