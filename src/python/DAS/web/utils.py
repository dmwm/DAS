#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Set of useful utilities used by DAS web applications
"""

__revision__ = "$Id: utils.py,v 1.21 2010/04/30 16:41:18 valya Exp $"
__version__ = "$Revision: 1.21 $"
__author__ = "Valentin Kuznetsov"

import re
import types
from   types import NoneType
import time
import httplib
import urllib
import urllib2
import DAS.utils.jsonwrapper as json
from DAS.utils.regex import number_pattern

def urllib2_request(request, url, params, headers={}, debug=0):
    """request method using GET request from urllib2 library"""
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

def json2html(idict, pad=""):
    """
    Convert input JSON into HTML code snippet.
    """
    width = 100
    newline = '\n'
    pat = number_pattern
    orig_pad = pad
    sss = pad + '{' + newline
    for key, val in idict.items():
        if  key == 'das_id' or key == 'cache_id':
            if  isinstance(val, list):
                value = '['
                counter = 0
                lpad = ' '*len(' "das_id": [')
                _width = len("'', ")
                for item in val:
                    _width += len(item)
                    if  _width > width:
                        value += '\n' + lpad
                        _width = len("'', ")
                    value += \
                        "<a href=\"/das/records/%s?collection=cache\">%s</a>, "\
                        % (item, item)
                    counter += 1
                value = value[:-2] + ']'
            else:
                value = "<a href=\"/das/records/%s?collection=cache\">%s</a>"\
                        % (val, val)
                if  len(str(val)) < 3: # aggregator's ids
                    value = val
            sss += pad + """ <code class="key">"%s": </code>%s""" % (key, value)
        elif isinstance(val, list):
            if  len(val) == 1:
                nline = ''
            else:
                nline = newline
            sss += pad + """ <code class="key">"%s": </code>""" % key
            sss += '[' + nline
            pad += " "*3
            ppp  = pad
            if  not nline:
                ppp  = ''
            for idx in range(0, len(val)):
                item = val[idx]
                if  isinstance(item, dict):
                    sss += json2html(item, pad)
                else:
                    if  isinstance(item, NoneType):
                        sss += """%s<code class="null">None</code>""" % ppp
                    elif  isinstance(item, int) or pat.match(str(item)):
                        sss += """%s<code class="number">%s</code>""" % (ppp, item)
                    else:
                        sss += """%s<code class="string">"%s"</code>""" % (ppp, item)
                if  idx < len(val) - 1:
                    sss += ',' + nline
            sss += ']'
            pad = orig_pad
        elif isinstance(val, dict):
            sss += pad + """ <code class="key">"%s"</code>: """ % key
            pad += ' '*3
            sss += json2html(val, pad)[len(pad):] # don't account for first pad
            pad  = pad[:-3]
        else:
            sss += pad + """ <code class="key">"%s"</code>""" % key
            if  isinstance(val, NoneType):
                sss += """: <code class="null">None</code>"""
            elif isinstance(val, int) or \
                (isinstance(val, str) and pat.match(str(val))):
                sss += """: <code class="number">%s</code>""" % val
            else:
                sss += """: <code class="string">"%s"</code>""" % val
        if  key != idict.keys()[-1]:
            sss += ',' + newline
        else:
            sss += ""
    sss += newline + pad + '}'
    return sss

def web_time():
    """
    Return time in format record in Cherrypy log.
    """
    # 2010-01-04 10:40:53,850
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))

def ajax_response_orig(msg, tag="_response", element="object"):
    """AJAX response wrapper"""
    page  = """<ajax-response><response type="%s" id="%s">""" % (element, tag)
    page += msg
    page += "</response></ajax-response>"
    print page
    return page

def ajax_response(msg):
    """AJAX response wrapper"""
    page  = """<ajax-response>"""
    page += "<div>" + msg + "</div>"
    page += "</ajax-response>"
    return page


