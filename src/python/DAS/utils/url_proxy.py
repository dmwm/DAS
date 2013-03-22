#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable-msg=
"""
File       : url_proxy.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Set of utilities to fetch given URLs via proxy servers
             If no proxy servers is available fall back to sequential
             method via getdata
"""

# system modules
import urllib
import urllib2

# DAS modules
from DAS.utils.url_utils import getdata
from DAS.utils.utils import get_key_cert

CKEY, CERT = get_key_cert()

def go_proxy(urls):
    "Proxy client for Go proxy server"
    params = {'urls': '\n'.join(urls)}
    encoded_data = urllib.urlencode(params)
    go_server = "http://localhost:8000/getdata"
    req = urllib2.Request(go_server)
    data = urllib2.urlopen(req, encoded_data)
    code = data.getcode()
    if  code == 200:
        if  not urls: # ping request
            yield {'ping':'pong'}
        else:
            for row in data.readlines():
                yield row
    else:
        yield {'error':'Fail to contact Go proxy server', 'code':code}

try:
    from pyurlfetch.urlfetch import DownloadError, URLFetchClient
    PROXY = "pyurlfetch"
    def pyurlfetch_proxy(urls):
        """
        Get data for given set of URLs using urlfetch proxy. This method works
        for GET HTTP requests and all URLs will be passed as is to the urlfetcher
        proxy. Client should take care of proper encoding.
        """
        if  not urls:
            return
        client  = URLFetchClient()
        fetches = (client.start_fetch(u) for u in urls)
        for fid in fetches:
            try:
                code, response, _headers = client.get_result(fid)
                if  code == 200:
                    yield response
            except DownloadError as err:
                yield {'error':str(err), 'fid':fid, 'code':code}
        client.close()
except ImportError:
    class DownloadError(Exception):
        """Raised when a download fails."""
    # try GO proxy server
    try:
        RESULT = [r for r in go_proxy([])]
    except DownloadError as _exc:
        RESULT = []
    if  len(RESULT) == 1 and RESULT[0] == {'ping':'pong'}:
        PROXY = "goproxy"
    else:
        PROXY = None

print "\n### DAS PROXY:", PROXY

def proxy_getdata(urls):
    "Get data for given URLs via proxy server"
    if  PROXY == 'pyurlfetch':
        for row in pyurlfetch_proxy(urls):
            yield row
    elif PROXY == 'goproxy':
        for row in go_proxy(urls):
            yield row
    else: # sequential access
        error_expire = 60
        expire = 60
        post = False
        verbose = False
        params = {}
        headers = {}
        for url in urls:
            data, _ = getdata(url, params, headers, expire, post,
                            error_expire, verbose, CKEY, CERT)
            yield data.read()

def test():
    "test function"
    urls = ["http://www.google.com", "http://www.golang.go"]
    for row in proxy_getdata(urls):
        print row
if __name__ == '__main__':
    test()
