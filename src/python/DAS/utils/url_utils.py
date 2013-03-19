#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0703,R0912,R0913,R0914,R0915
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
from   types import InstanceType

# DAS modules
from   DAS.utils.das_timer import das_timer
from   DAS.utils.utils import expire_timestamp, extract_http_error
from   DAS.utils.utils import http_timestamp
from   DAS.utils.pycurl_manager import RequestHandler
from   DAS.utils.pycurl_manager import REQUEST_HANDLER
import DAS.utils.jsonwrapper as json

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Simple HTTPS client authentication class based on provided 
    key/ca information
    """
    def __init__(self, key=None, cert=None, level=0):
        if  level > 1:
            urllib2.HTTPSHandler.__init__(self, debuglevel=1)
        else:
            urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        """Open request method"""
        #Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if  self.key:
            return httplib.HTTPSConnection(host, key_file=self.key,
                                                cert_file=self.cert)
        return httplib.HTTPSConnection(host)

def getdata(url, params, headers=None, expire=3600, post=False,
    error_expire=300, verbose=0, ckey=None, cert=None, doseq=True, system=None):
    "Fetch data from remote URL for given set of parameters"
#    return getdata_urllib(url, params, headers, expire, post, \
#                error_expire, verbose, ckey, cert, doseq, system)
    return getdata_pycurl(url, params, headers, expire, post, \
                error_expire, verbose, ckey, cert, doseq, system)

def getdata_pycurl(url, params, headers=None, expire=3600, post=None,
    error_expire=300, verbose=0, ckey=None, cert=None, doseq=True, system=None):
    "Fetch data via pycurl library"
    contact = 'data-service.'
    if  system:
        contact = system + ' ' + contact
    timer_key = '%s?%s' % (url, urllib.urlencode(params, doseq=True))
    das_timer(timer_key, verbose)
    handler = REQUEST_HANDLER
    try:
        data, expire = handler.getdata(url, params, headers, expire, post, \
                    error_expire, verbose, ckey, cert, doseq)
    except urllib2.HTTPError as httperror:
        msg  = 'HTTPError, url=%s, args=%s, headers=%s' \
                    % (url, params, headers)
        data = {'error': 'Unable to contact %s' % contact, 'reason': msg}
        try:
            err  = '%s %s' % (contact, extract_http_error(httperror.read()))
            data.update({'error':err})
            msg += '\n' + err
        except Exception as exp:
            data.update({'httperror': None})
            msg += '\n' + str(exp)
        print msg
        data = json.dumps(data)
        expire = expire_timestamp(error_expire)
    except Exception as exp:
        msg  = 'HTTPError, url=%s, args=%s, headers=%s' \
                    % (url, params, headers)
        print msg + '\n' + str(exp)
        data = {'error': 'Unable to contact %s' % contact, 'reason': msg}
        data = json.dumps(data)
        expire = expire_timestamp(error_expire)
    das_timer(timer_key, verbose)
    return data, expire

def getdata_urllib(url, params, headers=None, expire=3600, post=None,
    error_expire=300, verbose=0, ckey=None, cert=None, doseq=True, system=None,
    tstamp=None):
    """
    Invoke URL call and retrieve data from data-service based
    on provided URL and set of parameters. Use post=True to
    invoke POST request.
    """
    contact = 'data-service.'
    if  system:
        contact = system + ' ' + contact
    timer_key = '%s?%s' % (url, urllib.urlencode(params, doseq=True))
    das_timer(timer_key, verbose)
    encoded_data = urllib.urlencode(params, doseq=doseq)
    if  not post:
        url = url + '?' + encoded_data
    if  not headers:
        headers = {}
    if  tstamp and 'If-Modified-Since' not in headers.keys():
        headers['If-Modified-Since'] = http_timestamp(tstamp)
    if  verbose:
        print '+++ getdata, url=%s, headers=%s' % (url, headers)
    req = urllib2.Request(url)
    for key, val in headers.iteritems():
        req.add_header(key, val)
    if  verbose > 1:
        handler = urllib2.HTTPHandler(debuglevel=1)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    if  ckey and cert:
        handler = HTTPSClientAuthHandler(ckey, cert, verbose)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    try:
        time0 = time.time()
        if  post:
            data = urllib2.urlopen(req, encoded_data)
        else:
            data = urllib2.urlopen(req)
        data_srv_time = time.time()-time0
        info = data.info()
        code = data.getcode()
        if  verbose > 1:
            print "+++ response code:", code
            print "+++ response info\n", info
        try: # get HTTP header and look for Expires
            e_time = expire_timestamp(\
                info.__dict__['dict']['expires'])
            if  e_time < expire_timestamp(data_srv_time):
                expire = max(e_time, expire_timestamp(expire))
            elif e_time > time.time():
                expire = e_time
        except Exception as _exp:
            pass
    except urllib2.HTTPError as httperror:
        msg  = 'HTTPError, url=%s, args=%s, headers=%s' \
                    % (url, params, headers)
        data = {'error': 'Unable to contact %s' % contact, 'reason': msg}
        try:
            err  = '%s %s' % (contact, extract_http_error(httperror.read()))
            data.update({'error':err})
            msg += '\n' + err
        except Exception as exp:
            data.update({'httperror': None})
            msg += '\n' + str(exp)
        print msg
        data = json.dumps(data)
        expire = expire_timestamp(error_expire)
    except Exception as exp:
        msg  = 'HTTPError, url=%s, args=%s, headers=%s' \
                    % (url, params, headers)
        print msg + '\n' + str(exp)
        data = {'error': 'Unable to contact %s' % contact, 'reason': msg}
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
        return dict(self._url.headers.iteritems())

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

def close(stream):
    "Close given stream"
    if  isinstance(stream, InstanceType) or isinstance(stream, file):
        stream.close()

def get_proxy():
    "Test if proxy_getdata is loadable and return proxy function or False"
    try:
        from pyurlfetch.urlfetch import DownloadError, URLFetchClient
        func = proxy_getdata
        return func
    except ImportError:
        return False

def proxy_getdata(urls):
    """
    Get data for given set of URLs using urlfetch proxy. This method works
    for GET HTTP requests and all URLs will be passed as is to the urlfetcher
    proxy. Client should take care of proper encoding.
    """
    if  not urls:
        return
    from pyurlfetch.urlfetch import DownloadError, URLFetchClient
    client  = URLFetchClient()
    fetches = (client.start_fetch(u) for u in urls)
    for fid in fetches:
        try:
            code, response, _headers = client.get_result(fid)
            if  code == 200:
                yield response
        except DownloadError as err:
            raise Exception('urlfetch download error=%s' % str(err))
    client.close()
