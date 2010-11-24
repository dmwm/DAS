#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Set of useful utilities used by DAS web applications
"""

__revision__ = "$Id: utils.py,v 1.21 2010/04/30 16:41:18 valya Exp $"
__version__ = "$Revision: 1.21 $"
__author__ = "Valentin Kuznetsov"

from   types import NoneType
import time
import httplib
import urllib
import urllib2
import cherrypy
import plistlib
from   cherrypy import HTTPError
from   json import JSONEncoder

# DAS modules
import DAS.utils.jsonwrapper as json
from   DAS.utils.regex import number_pattern
from   DAS.utils.regex import web_arg_pattern
from   DAS.web.das_codes import web_code

def get_ecode(error):
    """
    Process input HTTPError and extract DAS error code. If not found
    return back the error.
    """
    for line in error.split('\n'):
        if  line.find('DAS error, code') != -1:
            return line.strip().replace('<p>', '').replace('</p>', '')
    return error

def checkarg(kwds, arg, atype=str):
    """Check arg in a dict that it has provided type"""
    return kwds.has_key(arg) and isinstance(kwds[arg], atype)

def checkargs(supported):
    """
    Decorator to check arguments in provided supported list for DAS servers
    """
    def wrap(func):
        """Wrap input function"""
        def wrapped_f(self, *args, **kwds):
            """Wrap function arguments"""
            # check request headers. For methods POST/PUT
            # we need to read request body to get parameters
            headers = cherrypy.request.headers
            if  cherrypy.request.method == 'POST' or\
                cherrypy.request.method == 'PUT':
                try:
                    body = cherrypy.request.body.read()
                except:
                    body = None
                if  args and kwds:
                    code = web_code('Misleading request')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
                if  body:
                    jsondict = json.loads(body, encoding='latin-1')
                else:
                    jsondict = kwds
                for key, val in jsondict.items():
                    kwds[str(key)] = str(val)

            pat = web_arg_pattern
            if  not kwds:
                if  args:
                    kwds = args[-1]
            keys = []
            if  kwds:
                keys = [i for i in kwds.keys() if i not in supported]
            if  keys:
                code  = web_code('Unsupported key')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'query') and not len(kwds['query']):
                code  = web_code('Invalid query')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'expire') and not pat.match(str(kwds['expire'])):
                code  = web_code('Unsupported expire value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'order'):
                if  kwds['order'] not in ['asc', 'desc']:
                    code  = web_code('Unsupported order value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'skey') and not isinstance(kwds['skey'], str):
                code  = web_code('Unsupported skey value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  kwds.has_key('input') and not isinstance(kwds['input'], str):
                code  = web_code('Invalid input')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'idx') and not pat.match(str(kwds['idx'])):
                code  = web_code('Unsupported idx value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'limit') and not pat.match(str(kwds['limit'])):
                code  = web_code('Unsupported limit value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'method'):
                if  kwds['method'] not in ['GET', 'PUT', 'DELETE', 'POST']:
                    code  = web_code('Unsupported method')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'dir'):
                if  kwds['dir'] not in ['asc', 'desc']:
                    code  = web_code('Unsupported dir value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'sort'):
                if  kwds['sort'] not in ['true', 'false']:
                    code  = web_code('Unsupported sort value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'view'):
                if  kwds['view'] not in ['list', 'xml', 'json']:
                    code  = web_code('Unsupported view')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'format'):
                if  kwds['format'] not in ['xml', 'json']:
                    code  = web_code('Unsupported format')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'database') and \
                not isinstance(kwds['database'], str):
                if  kwds['database'] not in \
                    ['das', 'analytics', 'admin', 'logging', 'mapping']:
                    code  = web_code('Unsupported database')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'collection'):
                if  kwds['collection'] not in ['cache', 'merge']:
                    code  = web_code('Unsupported collection')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'ajax'):
                if  str(kwds['ajax']) not in ['0', '1']:
                    code  = web_code('Unsupported ajax value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'show'):
                if  kwds['show'] not in ['json', 'code', 'yaml']:
                    code  = web_code('Unsupported show value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'dasquery') and not len(kwds['dasquery']):
                code = web_code('Unsupported dasquery value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'dbcoll'):
                if  kwds['dbcoll'].find('.') == -1:
                    code = web_code('Unsupported dbcoll value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'msg') and not isinstance(kwds['msg'], str):
                code = web_code('Unsupported msg value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'fid') and len(kwds['fid']) != 24:
                code = web_code('Unsupported id value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            data = func (self, *args, **kwds)
            return data
        wrapped_f.__doc__  = func.__doc__
        wrapped_f.__name__ = func.__name__
        wrapped_f.exposed  = True
        return wrapped_f
    wrap.exposed = True
    return wrap


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
                value = value[:-2] + ']'
            else:
                value = "<a href=\"/das/records/%s?collection=cache\">%s</a>"\
                        % (val, val)
                if  len(str(val)) < 3: # aggregator's ids
                    value = val
            sss += pad + """ <code class="key">"%s": </code>%s""" % (key, value)
        elif key == 'gridfs_id':
            value = "<a href=\"/das/gridfs/%s\">%s</das>" % (val, val)
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
                        sss += """%s<code class="number">%s</code>""" \
                                % (ppp, item)
                    else:
                        sss += """%s<code class="string">"%s"</code>""" \
                                % (ppp, item)
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

def wrap2dasjson(data):
    """DAS JSON wrapper"""
    encoder = JSONEncoder()
    cherrypy.response.headers['Content-Type'] = "text/json"
    try:
        jsondata = encoder.encode(data)
        return jsondata
    except:
        return dict(error="Failed to JSONtify obj '%s' type '%s'" \
            % (data, type(data)))

def wrap2dasxml(data):
    """DAS XML wrapper.
    Return data in XML plist format, 
    see http://docs.python.org/library/plistlib.html#module-plistlib
    """
    plist_str = plistlib.writePlistToString(data)
    cherrypy.response.headers['Content-Type'] = "application/xml"
    return plist_str

