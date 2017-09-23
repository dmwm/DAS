#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0702,W0613,R0912,R0913
"""
ReqMgr service
"""
__author__ = "Valentin Kuznetsov"

# system modules
import json
import time

# DAS modules
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, get_key_cert, json_parser
from DAS.utils.url_utils import getdata
from DAS.utils.urlfetch_pycurl import getdata as urlfetch_getdata


CKEY, CERT = get_key_cert()

def get_ids(url, params, dataset, verbose=False):
    "Query either ReqMgr or WMStats and retrieve request ids"
    headers = {'Accept': 'application/json;text/json'}
    expire = 600 # dummy number, we don't need it here
    ids = []
    source, expire = \
        getdata(url, params, headers, expire, ckey=CKEY, cert=CERT,
                verbose=verbose)
    for row in json_parser(source, None):
        for rec in row.get('rows', []):
            doc = rec['doc']
            if  not doc:
                continue
            if  'ProcConfigCacheID' in doc:
                ids.append(doc['ProcConfigCacheID'])
            elif 'ConfigCacheID' in doc:
                ids.append(doc['ConfigCacheID'])
            elif 'SkimConfigCacheID' in doc:
                ids.append(doc['SkimConfigCacheID'])
            else:
                if  'id' in rec and 'key' in rec and rec['key'] == dataset:
                    ids.append(rec['id'])
    return ids

def findReqMgrIds(dataset, base='https://cmsweb.cern.ch', verbose=False):
    """
    Find ReqMgrIds for a given dataset. This is quite complex procedure in CMS.
    We need to query ReqMgr data-service cache and find workflow ids by
    outputdataset name. The ReqMgr returns either document with ids used by MCM
    (i.e. ProcConfigCacheID, ConfigCacheID, SkimConfigCacheID) or we can take
    id of the request which bypass MCM. For refences see these discussions:
    https://github.com/dmwm/DAS/issues/4045
    https://hypernews.cern.ch/HyperNews/CMS/get/dmDevelopment/1501/1/1/1/1.html
    """
    params = {'key': '"%s"' % dataset, 'include_docs':'true', 'stale': 'update_after'}
    idict = {}
    for view in ['byoutputdataset', 'byinputdataset']:
        url = "%s/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/%s" \
            % (base, view)
        ids = get_ids(url, params, dataset, verbose)
        if  view in idict:
            idict[view] += list(set(ids))
        else:
            idict[view] = list(set(ids))
        source = 'ReqMgr'
    if  not ids: # we will query WMStats
        for view in ['requestByOutputDataset', 'requestByInputDataset']:
            url = '%s/couchdb/wmstats/_design/WMStats/_view/%s' % (base, view)
            ids = get_ids(url, params, dataset, verbose)
            if  view in idict:
                idict[view] += list(set(ids))
            else:
                idict[view] = list(set(ids))
            source = 'WMStats'
    return idict, source

def rurl(base, ids):
    "Construct reqmgr config url"
    rurl = '%s/couchdb/reqmgr_config_cache/%s/configFile' % (base, ids)
    return rurl

def configs(url, args, verbose=False):
    """Find config info in ReqMgr"""
    headers = {'Accept': 'application/json;text/json'}
    dataset = args.get('dataset', None)
    if  not dataset:
        return
    base = 'https://%s' % url.split('/')[2]
    idict, source = findReqMgrIds(dataset, base, verbose)
    ids = []
    ids_types = {} # keep track of ids/types relationship
    for key, ilist in idict.items():
        rtype = 'output' if key.lower().find('output') != -1 else 'input'
        for item in ilist:
            ids.append(item)
            ids_types[item] = rtype
    # for hash ids find configs via ReqMgr REST API
    urls = [rurl(base, i) for i in ids if len(i) == 32]
    # for non-hash ids probe to find configs in showWorkload
    req_urls = ['%s/couchdb/reqmgr_workload_cache/%s' \
            % (base, i) for i in ids if len(i) != 32]
    if  req_urls:
        gen  = urlfetch_getdata(req_urls, CKEY, CERT, headers)
        config_urls = []
        for row in gen:
            if  'error' not in row:
                url = row['url']
                for key, rtype in ids_types.items():
                    if  key in url:
                        break
                rdict = json.loads(row['data'])
                for key in rdict.keys():
                    val = rdict[key]
                    if  key.endswith('ConfigCacheID'):
                        if  isinstance(val, basestring):
                            config_urls.append(rurl(base, val))
                            ids_types[val] = rtype
                    elif isinstance(val, dict):
                        for kkk in val.keys():
                            if  kkk.endswith('ConfigCacheID'):
                                vvv = val[kkk]
                                if  isinstance(vvv, basestring):
                                    config_urls.append(rurl(base, vvv))
                                    ids_types[vvv] = rtype
        if  config_urls:
            urls += config_urls
    udict = {}
    for rid, rtype in ids_types.items():
        for url in set(urls):
            if  rid in url:
                udict.setdefault(rtype, []).append(url)
    config = {'dataset':dataset, 'name': source, 'urls': udict, 'ids': ids, 'idict': idict}
    yield {'config': config}

class ReqMgrService(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'reqmgr', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def adjust_params(self, api, kwds, inst=None):
        """
        Adjust parameters for specific query requests
        """
        if  api == 'inputdataset' or api == 'configIDs' or \
                api == 'outputdataset':
            if  kwds.get('dataset', 'required').find('*') != -1:
                kwds['dataset'] = 'required' # we skip patterns

    def apicall(self, dasquery, url, api, args, dformat, expire):
        "ReqMgr implementation of AbstractService:apicall method"
        if  api == 'configs':
            time0 = time.time()
            dasrows = configs(url, args)
            ctime = time.time()-time0
            self.write_to_cache(dasquery, expire, url, api, args,
                    dasrows, ctime)
        else:
            super(ReqMgrService, self).apicall(\
                    dasquery, url, api, args, dformat, expire)

    def getdata(self, url, params, expire, headers=None, post=None):
        """URL call wrapper"""
        if  url[-1] == '/':
            url = url[:-1]
        for key, _val in params.items():
            url = '/'.join([url, params[key]])
        params = {}
        return getdata(url, params, headers, expire, post,
                self.error_expire, self.verbose, self.ckey, self.cert,
                system=self.name)

    def parser(self, query, dformat, source, api):
        """
        ReqMgr data-service parser.
        """
        if  api == 'inputdataset' or api == 'outputdataset':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                key = 'WMCore.RequestManager.DataStructs.Request.Request'
                try:
                    data = row['dataset']
                    if  isinstance(data, dict) and 'error' in data:
                        yield row
                    else:
                        data = data[key]
                        if  'InputDatasetTypes' in data:
                            arr = []
                            for key, val in \
                                    data['InputDatasetTypes'].items():
                                arr.append({'dataset':key, 'type':val})
                            data['InputDatasetTypes'] = arr
                        yield data
                except:
                    yield row
        elif api == 'datasetByPrepID':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                data = row['dataset']
                for val in data.values():
                    if  isinstance(val, basestring):
                        yield {'dataset':{'name': val}}
                    elif isinstance(val, list):
                        for vvv in val:
                            yield {'dataset':{'name': vvv}}
        elif api == 'recentDatasetByPrepID':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                yield {'dataset':{'name':row['dataset']}}
        elif api == 'configIDs':
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                try:
                    data = row['dataset']
                    if  isinstance(data, dict) and 'error' in data:
                        yield row
                    else:
                        for key, val in data.items():
                            yield dict(request_name=key, config_files=val)
                except:
                    pass
        else:
            gen = DASAbstractService.parser(self, query, dformat, source, api)
            for row in gen:
                yield row
