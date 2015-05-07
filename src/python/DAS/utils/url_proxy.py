#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : url_proxy.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Set of utilities to fetch given URLs via proxy servers
             If no proxy servers is available fall back to sequential
             method via getdata
"""
from __future__ import print_function

# system modules
import urllib
import urllib2

# DAS modules
from DAS.utils.url_utils import getdata
from DAS.utils.utils import get_key_cert

CKEY, CERT = get_key_cert()

def urlfetch_proxy(urls):
    "Proxy client for Go proxy server"
    params = {'urls': '\n'.join(urls)}
    encoded_data = urllib.urlencode(params)
    server = "http://localhost:8215/fetch"
    req = urllib2.Request(server)
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

def proxy_getdata(urls):
    "Get data for given URLs via proxy server"
    try:
        result = [r for r in urlfetch_proxy([])]
    except Exception as _exc:
        result = []
    if  len(result) == 1 and result[0] == {'ping':'pong'}:
        for row in urlfetch_proxy(urls):
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
    url1 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/help"
    url2 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatiers"
    urls = [url1, url2]
    for row in proxy_getdata(urls):
        print(row)
if __name__ == '__main__':
    test()
