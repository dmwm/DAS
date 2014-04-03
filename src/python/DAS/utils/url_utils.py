#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703,R0912,R0913,R0914,R0915
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

import cherrypy

# DAS modules
from   DAS import DAS_SERVER
from   DAS.utils.das_timer import das_timer
from   DAS.utils.utils import expire_timestamp, extract_http_error
from   DAS.utils.utils import http_timestamp, dastimestamp
from   DAS.utils.pycurl_manager import RequestHandler
from   DAS.utils.pycurl_manager import REQUEST_HANDLER
from   DAS.utils.regex import int_number_pattern
import DAS.utils.jsonwrapper as json


def disable_urllib2Proxy():
    """
    Setup once and forever urllib2 proxy, see
    http://kember.net/articles/obscure-python-urllib2-proxy-gotcha
    """
    proxy_support = urllib2.ProxyHandler({})
    opener = urllib2.build_opener(proxy_support)
    urllib2.install_opener(opener)

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
    if  headers:
        headers.update({'User-Agent': DAS_SERVER})
    else:
        headers = {'User-Agent': DAS_SERVER}
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
        msg  = 'urllib2.HTTPError, system=%s, url=%s, args=%s, headers=%s' \
                    % (system, url, json.dumps(params), json.dumps(headers))
        data = {'error': 'Received HTTP error from %s data-service' % contact,
                'reason': msg, 'ts':time.time()}
        try:
            reason = extract_http_error(httperror.read())
            data.update({'reason': reason, 'request': msg})
            # TODO: err variable did not exit in this function!
            msg += '\n' + reason
        except Exception as exp:
            data.update({'httperror': None})
            msg += '\n' + str(exp)
        print dastimestamp('getdata_pycurl'), msg
        data = json.dumps(data)
        expire = expire_timestamp(error_expire)
    except Exception as exp:
        msg  = 'HTTPError, system=%s, url=%s, args=%s, headers=%s' \
                    % (system, url, json.dumps(params), json.dumps(headers))
        print dastimestamp('getdata_pycurl'), msg + '\n' + str(exp)
        data = {'error': 'Received generic error from %s data-service' % contact,
                'reason': msg, 'ts':time.time()}
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
        data = {'error': 'Received HTTP error from %s data-service' % contact,
                'reason': msg}
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
        data = {'error': 'Received generic error from %s data-service' % contact,
                'reason': msg}
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
        result = [r for r in urlfetch_proxy([])]
    except Exception as _exc:
        result = []
    if  len(result) == 1 and result[0] == {'ping':'pong'}:
        return urlfetch_proxy
    print "\n### No UrlFecth Proxy Server"
    return False

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
                if  row and row[0] == '{' and row.find('data'):
                    # Erlang response
                    rec = json.loads(row)
                    for line in rec['data'].split('\n'):
                        yield line
                else:
                    # Go response
                    yield row
    else:
        yield {'error':'Fail to contact UrlFetch proxy server', 'code':code}

def url_args(url, convert_types=False):
    """
    Extract args from given url, e.g. http://a.b.com/api?arg1=1&arg2=2
    will yield {'arg1':1, 'arg2':2}
    """
    args = {}
    for item in url.split("?")[-1].split('&'):
        key, value = item.split('=')
        if  convert_types:
            if  int_number_pattern.match(value):
                args[key] = int(value)
            else:
                args[key] = value
        else:
            args[key] = value
    return args


def url_extend_params_as_dict(**overrides):
    """
    returns a dict of (url) query params extended with params in overrides
    (preserving the default values for instance, page size, etc)
    """
    to_preserve = ['view', 'instance', 'input', 'limit']
    params = dict((param, v)
                  for param, v in cherrypy.request.params.items()
                  if param in to_preserve)
    # override
    for param, value in overrides.items():
        params[param] = value
    return params


def url_extend_params(url, **overrides):
    """
    returns a link to given url taking into account existing parameters
    already present in cherry.request (e.g. instance, page size, etc) """
    params = url_extend_params_as_dict(**overrides)
    return url + '?' + urllib.urlencode(params)
