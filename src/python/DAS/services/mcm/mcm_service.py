#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0703,R0913,R0914

"""
MCM data-service plugin.
"""
__author__ = "Valentin Kuznetsov"

# system modules
import time

# DAS modules
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, get_key_cert
from DAS.utils.url_utils import getdata
from DAS.utils.global_scope import SERVICES
from DAS.utils.urlfetch_pycurl import getdata as urlfetch_getdata

import DAS.utils.jsonwrapper as json

CKEY, CERT = get_key_cert()

def mcm4dataset(url, reqmgr, dataset, expire):
    "Get McM info for given dataset"
    # get data from ReqMgr, we use reqmgr map and extract inputdataset URL
    req_url = reqmgr.map['inputdataset']['url']
    params  = {'dataset':dataset}
    headers =  {'Accept': 'application/json' }
    data, expire = reqmgr.getdata(req_url, params, expire, headers)
    mcmurls = []

    # parse ReqMgr data, collect PrepID and construct McM URLs
    try:
        rinfo = json.load(data)
        for rec in rinfo:
            key = 'WMCore.RequestManager.DataStructs.Request.Request'
            prepid = rec[key].get('PrepID', None)
            if  prepid:
                mcmurls.append('%s/%s' % (url, prepid))
    except Exception as exp:
        error = 'Fail to parse ReqMgr output'
        reason = 'data=%s, json.load error=%s' % (row, str(exp))
        yield {'mcm':{'error':error, 'reason':reason}}

    # contact McM data-service and get data for our list of McM URLs
    headers = {}
    gen = urlfetch_getdata(mcmurls, CKEY, CERT, headers)
    for row in gen:
        url = row['url']
        data = json.loads(row['data'])
        yield dict(mcm=data['results'])

class MCMService(DASAbstractService):
    """
    Helper class to provide MCM data-service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'mcm', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def apicall(self, dasquery, url, api, args, dformat, expire):
        "McM implementation of AbstractService:apicall method"
        if  api == 'mcm4dataset':
            time0   = time.time()
            reqmgr  = SERVICES.get('reqmgr', None) # reqmgr from global scope
            rawrows = mcm4dataset(url, reqmgr, args['dataset'], expire)
            dasrows = self.translator(api, rawrows)
            ctime   = time.time()-time0
            self.write_to_cache(dasquery, expire, url, api, args,
                    dasrows, ctime)
        else:
            super(MCMService, self).apicall(\
                    dasquery, url, api, args, dformat, expire)

    def getdata(self, url, params, expire, headers=None, post=None):
        """URL call wrapper"""
        if  not headers:
            headers =  {'Accept': 'application/json' } # DBS3 always needs that
        # MCM uses rest API
        url = '%s/%s' % (url, params.get('mcm'))
        params = {}
        return getdata(url, params, headers, expire, post,
                self.error_expire, self.verbose, self.ckey, self.cert,
                doseq=False, system=self.name)
