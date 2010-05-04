#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Set of useful utilities used by DAS web applications
"""

__revision__ = "$Id: utils.py,v 1.6 2009/09/09 18:44:55 valya Exp $"
__version__ = "$Revision: 1.6 $"
__author__ = "Valentin Kuznetsov"

import types
import httplib
import urllib
import urllib2
try:
    import json # since python 2.6
except:
    import simplejson as json # prior python 2.6

def urllib2_request(request, url, params, headers={}, debug=0):
    """request method using GET request from urllib2 library"""
    debug = 1
    if  request == 'GET' or request == 'DELETE':
        encoded_data=urllib.urlencode(params, doseq=True)
        url += '?%s' % encoded_data
        req = UrlRequest(request, url=url, headers=headers)
    else:
        encoded_data=json.dumps(params)
        req = UrlRequest(request, url=url, data=encoded_data, headers=headers)
    if  debug:
        h=urllib2.HTTPHandler(debuglevel=1)
        opener = urllib2.build_opener(h)
    else:
        opener = urllib2.build_opener()
    fdesc = opener.open(req)
    data = fdesc.read()
    fdesc.close()
    return data

def httplib_request(host, path, params, request='POST', debug=0):
    """request method using provided HTTP request and httplib library"""
    if  debug:
        httplib.HTTPConnection.debuglevel = 1
    if  type(params) is not types.StringType:
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
    if  request == 'GET':
        conn.request(request, path)
    else:
        conn.request(request, path, params, headers)
#    conn.request(request, path, params, headers)
    response = conn.getresponse()

    if  response.reason != "OK":
        print response.status, response.reason, response.read()
        return 0
    else:
        res = response.read()
        return res
    conn.close()

class UrlProxy(object): 
    """Proxy class to handle URLs"""
    def __init__(self, location): 
        self._url = urllib2.urlopen(location) 

    def headers(self):
        """Get URL headers""" 
        return dict(self._url.headers.items()) 

    def get(self): 
        """Get URL data"""
        return self._url.read()

class UrlRequest(urllib2.Request):
    """
    URL requestor class which supports all RESTful request methods.
    It is based on urllib2.Request class and overwrite request method.
    Usage: UrlRequest(method, url=url, data=data), where method is
    GET, POST, PUT, DELETE.
    """
    def __init__(self, method, *args, **kwargs):
        self._method = method
        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        """Return request method"""
        return self._method

