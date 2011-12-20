#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=R0913,W0702,R0914,R0912,R0201
"""
File: pycurl_manager.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: a basic wrapper around pycurl library.
The RequestHandler class provides basic APIs to get data
from a single resource or submit mutliple requests to
underlying data-services.

For CMS the primary usage would be access to DBS3 where
we need to fetch dataset details for a given dataset pattern.
This is broken into 2 APIs, datasets which returns list of
datasets and filesummaries which provides details about
given dataset. So we invoke 1 request to datasets API
followed by N requests to filesummaries API.
"""

import time
import pycurl
import urllib
from DAS.utils.jsonwrapper import json
try:
    import cStringIO as StringIO
except:
    import StringIO

def parse_body(data):
    """Parse body part of URL request"""
    return json.loads(data)

def get_expire(data, error_expire=300, verbose=0):
    """Parser header part of URL request and return expires timestamp"""
    if  verbose:
        print data
    for item in data.split('\n'):
        if  item.find('Expires') != -1:
            try:
                return item.split('Expires:')[1].strip()
            except:
                pass
    return error_expire

class RequestHandler(object):
    """
    RequestHandler provides APIs to fetch single/multiple
    URL requests based on pycurl library
    """
    def __init__(self, config=None):
        super(RequestHandler, self).__init__()
        if  not config:
            config = {}
        self.nosignal = config.get('nosignal', 1)
        self.timeout = config.get('timeout', 300)
        self.connecttimeout = config.get('connecttimeout', 30)
        self.followlocation = config.get('followlocation', 1)
        self.maxredirs = config.get('maxredirs', 5)

    def set_opts(self, curl, url, params, headers,
                 ckey=None, cert=None, verbose=None, post=None, doseq=True):
        """Set options for given curl object"""
        curl.setopt(pycurl.NOSIGNAL, self.nosignal)
        curl.setopt(pycurl.TIMEOUT, self.timeout)
        curl.setopt(pycurl.CONNECTTIMEOUT, self.connecttimeout)
        curl.setopt(pycurl.FOLLOWLOCATION, self.followlocation)
        curl.setopt(pycurl.MAXREDIRS, self.maxredirs)

        encoded_data = urllib.urlencode(params, doseq=doseq)
        if  not post:
            url = url + '?' + encoded_data
        if  post:
            curl.setopt(pycurl.POST, 1)
        curl.setopt(pycurl.URL, url)
        curl.setopt(pycurl.HTTPHEADER, \
                ["%s: %s" % (k, v) for k, v in headers.iteritems()])
        bbuf = StringIO.StringIO()
        hbuf = StringIO.StringIO()
        curl.setopt(pycurl.WRITEFUNCTION, bbuf.write)
        curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
        curl.setopt(pycurl.SSL_VERIFYPEER, False)
        if  ckey:
            curl.setopt(pycurl.SSLKEY, ckey)
        if  cert:
            curl.setopt(pycurl.SSLCERT, cert)
        if  verbose:
            curl.setopt(pycurl.VERBOSE, 1)
            curl.setopt(pycurl.DEBUGFUNCTION, self.debug)
        return bbuf, hbuf

    def debug(self, debug_type, debug_msg):
        """Debug callback implementation"""
        print "debug(%d): %s" % (debug_type, debug_msg)

    def getdata(self, url, params, headers=None, expire=3600, post=None,
                error_expire=300, verbose=0, ckey=None, cert=None, doseq=True):
        """Fetch data for given set of parameters"""
        curl = pycurl.Curl()
        bbuf, hbuf = self.set_opts(curl, url, params, headers,
                ckey, cert, verbose, post, doseq)
        curl.perform()
        data = parse_body(bbuf.getvalue())
        expire = get_expire(hbuf.getvalue(), error_expire, verbose)
        bbuf.flush()
        hbuf.flush()
        return (data, expire)

    def multirequest(self, url, parray, headers=None,
                ckey=None, cert=None, verbose=None):
        """Fetch data for given set of parameters"""
        multi = pycurl.CurlMulti()
        for params in parray:
            curl = pycurl.Curl()
            bbuf, hbuf = \
                self.set_opts(curl, url, params, headers, ckey, cert, verbose)
            multi.add_handle(curl)
            while True:
                ret, num_handles = multi.perform()
                if  ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            while num_handles:
                ret = multi.select(1.0)
                if  ret == -1:
                    continue
                while 1:
                    ret, num_handles = multi.perform()
                    if  ret != pycurl.E_CALL_MULTI_PERFORM:
                        break
            _numq, response, _err = multi.info_read()
            for _cobj in response:
                data = json.loads(bbuf.getvalue())
                if  isinstance(data, dict):
                    data.update(params)
                    yield data
                if  isinstance(data, list):
                    for item in data:
                        if  isinstance(item, dict):
                            item.update(params)
                            yield item
                        else:
                            err = 'Unsupported data format: data=%s, type=%s'\
                                % (item, type(item))
                            raise Exception(err)
                bbuf.flush()
                hbuf.flush()

def datasets(url, cert, ckey, pattern, verbose=None):
    """Look-up dataset and its details for a given dataset pattern"""
    # NOTE: DBS3 API does not allow to pass details parameter for
    # dataset patterns, only for fully qualified dataset.
    params  = {'dataset':pattern, 'dataset_access_type': 'PRODUCTION'}
    headers = {'Accept':'text/json;application/json'}
    reqmgr  = RequestHandler()
    data, expire = reqmgr.getdata(url, params, headers, \
                ckey=ckey, cert=cert, verbose=verbose)
    furl = url.replace('datasets', 'filesummaries')
    res = reqmgr.multirequest(furl, data, headers, \
                ckey=ckey, cert=cert, verbose=verbose)
    for row in res:
        name = row['dataset']
        del row['dataset']
        row['name'] = name
        yield dict(dataset=row)

if __name__ == '__main__':
    import os
    CERT = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
    CKEY = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
    URL  = 'https://localhost:8979/dbs/prod/global/DBSReader/datasets'
    PAT  = '/RelValSingleElectronPt10/CMSSW_3_9_0*'
    PAT  = '/RelValSingle*'
    data = [r for r in datasets(URL, CERT, CKEY, PAT)]
    print data
