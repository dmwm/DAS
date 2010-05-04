#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Set of useful utilities used by DAS web applications
"""

__revision__ = "$Id: utils.py,v 1.8 2009/10/13 18:06:19 valya Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "Valentin Kuznetsov"

import re
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

def json2html(idict, pad="", short=False):
    """
    Convert input JSON into HTML code snippet.
    """
    newline = '\n'
    if  short:
        newline = ''
        pad = ''
    pat=re.compile('^[0-9][0-9\.]*$')
    orig_pad = pad
    sss = pad + '{' + newline
    for key, val in idict.items():
        if  type(val) is types.ListType:
            sss += pad + """ <code class="key">"%s":</code>""" % key
            sss += pad + '[' + newline
            if  not short:
                pad += " "*3
            for item in val:
                if  type(item) is types.DictType:
                    sss += json2html(item, pad, short)
                else:
                    if type(item) is types.NoneType:
                        sss += """%s<code class="null">None</code>""" % pad
                    elif  type(item) is types.IntType or pat.match(item):
                        sss += """%s<code class="number">%s</code>""" % (pad, item)
                    else:
                        sss += """%s<code class="string">"%s"</code>""" % (pad, item)
                if  item != val[-1]:
                    sss += ',' + newline
            sss += ']'
            pad = orig_pad
        elif type(val) is types.DictType:
            sss += json2html(val, pad, short)
        else:
            sss += pad + """ <code class="key">"%s"</code>""" % key
            if type(val) is types.NoneType:
                sss += """:<code class="null">None</code>"""
            elif  type(val) is types.IntType or pat.match(str(val)):
                sss += """:<code class="number">%s</code>""" % val
            else:
                sss += """:<code class="string">"%s"</code>""" % val
        if  key != idict.keys()[-1]:
            sss += ',' + newline
        else:
            sss += ""
    sss += newline + pad + '}'
    return sss
