#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=R0913,W0702,R0914,R0912,R0201,E1101
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

# system modules
import time
import pycurl
import urllib
from   urllib2 import HTTPError
import threading
try:
    import cStringIO as StringIO
except:
    import StringIO

# DAS modules
from DAS import DAS_SERVER
from DAS.utils.jsonwrapper import json
from DAS.utils.regex import pat_http_msg, pat_expires
from DAS.utils.utils import expire_timestamp

def parse_body(data):
    """Parse body part of URL request"""
    return json.loads(data)

def get_expire(data, error_expire=300, verbose=0):
    """Parser header part of URL request and return expires timestamp"""
    if  verbose > 1:
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
        self.timeout = config.get('timeout', 400)
        self.connecttimeout = config.get('connecttimeout', 30)
        self.followlocation = config.get('followlocation', 1)
        self.maxredirs = config.get('maxredirs', 5)
        self.gcache = {} # cache for GET requests
        self.pcache = {} # cache for POST requests

    def set_opts(self, curl, url, params, headers,
                 ckey=None, cert=None, verbose=None, post=False, doseq=True):
        """Set options for given curl object"""
        # set new options
        curl.setopt(pycurl.NOSIGNAL, self.nosignal)
        curl.setopt(pycurl.TIMEOUT, self.timeout)
        curl.setopt(pycurl.CONNECTTIMEOUT, self.connecttimeout)
        curl.setopt(pycurl.FOLLOWLOCATION, self.followlocation)
        curl.setopt(pycurl.MAXREDIRS, self.maxredirs)

        encoded_data = urllib.urlencode(params, doseq=doseq)
        if  post:
            curl.setopt(pycurl.POST, 1)
            curl.setopt(pycurl.POSTFIELDS, encoded_data)
        else:
            url = url + '?' + encoded_data
        if  verbose > 1:
            print '\nDEBUG: pycurl call', url
        if  isinstance(url, str):
            curl.setopt(pycurl.URL, url)
        elif isinstance(url, unicode):
            curl.setopt(pycurl.URL, url.encode('ascii', 'ignore'))
        else:
            raise TypeError('Wrong type for url="%s", type="%s"' \
                % (url, type(url)))
        if  headers:
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
            if  isinstance(verbose, int) and verbose > 2:
                curl.setopt(pycurl.VERBOSE, 1)
                curl.setopt(pycurl.DEBUGFUNCTION, self.debug)
        return bbuf, hbuf

    def debug(self, debug_type, debug_msg):
        """Debug callback implementation"""
        print "debug(%d): %s" % (debug_type, debug_msg)

    def getdata(self, url, params, headers=None, expire=3600, post=False,
                error_expire=300, verbose=0, ckey=None, cert=None, doseq=True):
        """Fetch data for given set of parameters"""
        time0     = time.time()
        thread    = threading.current_thread().ident
        if  post:
            cache = self.pcache
        else:
            cache = self.gcache
        if  thread in cache:
            curl  = cache.get(thread)
        else:
            curl  = pycurl.Curl()
            cache[thread] = curl
#        print "\n+++ getdata curl gcache", self.gcache.keys()
#        print "+++ getdata curl pcache", self.pcache.keys()
        bbuf, hbuf = self.set_opts(curl, url, params, headers,\
                ckey, cert, verbose, post, doseq)
        curl.perform()

        http_header = hbuf.getvalue()

#        data = parse_body(bbuf.getvalue())
#        data = bbuf.getvalue() # read entire content
#        bbuf.flush()
        bbuf.seek(0)# to use file description seek to the beggning of the stream
        data = bbuf # leave StringIO object, which will serve as file descriptor
        expire = get_expire(http_header, error_expire, verbose)
        hbuf.flush()

        # check for HTTP error
        http_code = curl.getinfo(pycurl.HTTP_CODE)

        # get HTTP status message and Expires
        http_expire  = ''
        http_msg = ''
        for item in http_header.splitlines():
            if  pat_http_msg.match(item):
                http_msg = item
            if  pat_expires.match(item):
                http_expire = item.split('Expires:')[-1].strip()
                e_time = expire_timestamp(http_expire)
                if  e_time < expire_timestamp(time0):
                    expire = max(e_time, expire_timestamp(expire))
                elif e_time > time.time():
                    expire = e_time

        if  http_code < 200 or http_code >= 300:
            effective_url = curl.getinfo(pycurl.EFFECTIVE_URL)
            raise HTTPError(effective_url, http_code, http_msg, \
                    http_header, data)
        return data, expire

    def multirequest(self, url, parray, headers=None,\
                ckey=None, cert=None, verbose=None, decoder='json'):
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
            _, response, _ = multi.info_read()
            for _ in response:
                if  decoder == 'json':
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
                                err = 'Unsupported data format: '
                                err += 'data=%s, type=%s' % (item, type(item))
                                raise Exception(err)
                else:
                    yield bbuf.getvalue()
                bbuf.flush()
                hbuf.flush()

# singleton
REQUEST_HANDLER = RequestHandler()

def datasets(url, cert, ckey, pattern, verbose=None):
    """Look-up dataset and its details for a given dataset pattern"""
    # NOTE: DBS3 API does not allow to pass details parameter for
    # dataset patterns, only for fully qualified dataset.
    params  = {'dataset':pattern, 'dataset_access_type': 'VALID'}
    headers = {'Accept':'text/json;application/json', 'User-Agent': DAS_SERVER}
    reqmgr  = RequestHandler()
    data, _ = reqmgr.getdata(url, params, headers, \
                ckey=ckey, cert=cert, verbose=verbose)
    params = json.load(data)
    furl = url.replace('datasets', 'filesummaries')
    res = reqmgr.multirequest(furl, params, headers, \
                ckey=ckey, cert=cert, verbose=verbose)
    for row in res:
        name = row['dataset']
        del row['dataset']
        row['name'] = name
        yield dict(dataset=row)

def test():
    "Test function"
    import os
    cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
    ckey = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
    url  = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datasets'
    pat  = '/RelVal*'
    data = [r for r in datasets(url, cert, ckey, pat)]
    print data

if __name__ == '__main__':
    test()
