#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Set of useful utilities used by DAS web applications
"""

__revision__ = "$Id: utils.py,v 1.2 2009/06/02 00:48:24 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

import httplib
import urllib
import urllib2

def urllib2_request(url, params, debug=0):
    """request method using GET request from urllib2 library"""
    if  debug:
        httplib.HTTPConnection.debuglevel = 1
    res = urllib2.urlopen(url, urllib.urlencode(params, doseq=True))
    return res.read()

def httplib_request(host, path, params, request='POST', debug=0):
    """request method using provided HTTP request and httplib library"""
    if  debug:
        httplib.HTTPConnection.debuglevel = 1
    params = urllib.urlencode(params, doseq=True)
    if  debug:
        print "input parameters", params
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": "text/plain"}
    if  host.find('https://') != -1:
        host = host.replace('https://', '')
        conn = httplib.HTTPSConnection(host)
    else:
        host = host.replace('http://', '')
        conn = httplib.HTTPConnection(host)
    conn.request(request, path, params, headers)
    response = conn.getresponse()

    if  response.reason != "OK":
        print response.status, response.reason
        return 0
    else:
        res = response.read()
        return res
    conn.close()

class UrlProxy(object): 
    def __init__(self, location): 
        self._url = urlopen(location) 
    def headers(self): 
        return dict(self._url.headers.items()) 
    def get(self): 
        return self._url.read()
