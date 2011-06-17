#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: url_utils.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: Set of url utilities for DAS
"""

# system modules
import time
import urllib
import urllib2
import httplib

# DAS modules
from   DAS.utils.das_timer import das_timer
from   DAS.utils.utils import expire_timestamp, extract_http_error
import DAS.utils.jsonwrapper as json

def getdata(url, params, headers=None, expire=3600, post=None, 
                error_expire=300, verbose=0):
    """
    Invoke URL call and retrieve data from data-service based
    on provided URL and set of parameters. Use post=True to
    invoke POST request.
    """
    timer_key = '%s?%s' % (url, urllib.urlencode(params, doseq=True))
    das_timer(timer_key, verbose)
    encoded_data = urllib.urlencode(params, doseq=True)
    if  not post:
        url = url + '?' + encoded_data
    if  not headers:
        headers = {}
    if  verbose:
        print '+++ getdata, url=%s, headers=%s' % (url, headers)
    req = urllib2.Request(url)
    for key, val in headers.items():
        req.add_header(key, val)
    if  verbose > 1:
        handler = urllib2.HTTPHandler(debuglevel=1)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    try:
        if  post:
            data = urllib2.urlopen(req, encoded_data)
        else:
            data = urllib2.urlopen(req)
        try: # get HTTP header and look for Expires
            e_time = expire_timestamp(\
                data.info().__dict__['dict']['expires'])
            if  e_time > time.time():
                expire = e_time
        except Exception as _exp:
            pass
    except urllib2.HTTPError, httperror:
        msg  = 'HTTPError, url=%s, args=%s, headers=%s' \
                    % (url, params, headers)
        data = {'error': msg}
        try:
            err  = httperror.read()
            data.update({'httperror':extract_http_error(err)})
            msg += '\n' + err
        except Exception as exp:
            data.update({'httperror': None})
            msg += '\n' + str(exp)
        print msg
        data = str(data)
        expire = expire_timestamp(error_expire)
    except Exception as exp:
        msg  = 'HTTPError, url=%s, args=%s, headers=%s' \
                    % (url, params, headers)
        print msg + '\n' + str(exp)
        data = {'error': msg, 
                'reason': 'Unable to invoke HTTP call to data-service'}
        data = json.dumps(data)
        expire = expire_timestamp(error_expire)
    das_timer(timer_key, verbose)
    return data, expire

def urllib2_request(request, url, params, headers=None, debug=0):
    """request method using GET request from urllib2 library"""
    if  not headers:
        headers = {}
    if  request == 'GET' or request == 'DELETE':
        encoded_data = urllib.urlencode(params, doseq=True)
        url += '?%s' % encoded_data
        req = UrlRequest(request, url=url, headers=headers)
    else:
        encoded_data = json.dumps(params)
        req = UrlRequest(request, url=url, data=encoded_data, headers=headers)
    if  debug:
        hdlr   = urllib2.HTTPHandler(debuglevel=1)
        opener = urllib2.build_opener(hdlr)
    else:
        opener = urllib2.build_opener()
    fdesc  = opener.open(req)
    result = fdesc.read()
    fdesc.close()
    return result

def httplib_request(host, path, params, request='POST', debug=0):
    """request method using provided HTTP request and httplib library"""
    if  debug:
        httplib.HTTPConnection.debuglevel = 1
    if  not isinstance(params, str):
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

