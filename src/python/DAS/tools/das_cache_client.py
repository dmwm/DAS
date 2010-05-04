#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS cache client tools 
"""
__revision__ = "$Id: das_cache_client.py,v 1.15 2009/09/09 18:43:05 valya Exp $"
__version__ = "$Revision: 1.15 $"
__author__ = "Valentin Kuznetsov"

import urllib2, urllib
from optparse import OptionParser
try:
    # Python 2.6
    import json
    from json import JSONDecoder
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
    from simplejson import JSONDecoder

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

class DASOptionParser: 
    """
    DAS cache client option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                               type="int", default=0, dest="verbose",
             help="verbose output")
        self.parser.add_option("--query", action="store", type="string", 
                               default=False, dest="query",
             help="specify query for your request")
        self.parser.add_option("--input", action="store", type="string", 
                               default=False, dest="input",
             help="specify input for your request; the input should be in a form of dict")
        self.parser.add_option("--host", action="store", type="string", 
                               default='http://localhost:8211', dest="host",
             help="specify host name, default http://localhost:8211")
        self.parser.add_option("--idx", action="store", type="int", 
                               default=0, dest="idx",
             help="start index for returned result set, aka pagination, use w/ limit")
        self.parser.add_option("--limit", action="store", type="int", 
                               default=10, dest="limit",
             help="number of returned results (results per page)")
        self.parser.add_option("--request", action="store", type="string", 
                               default='GET', dest="request",
             help="specify request type: GET (default), POST, PUT, DELETE")
        self.parser.add_option("--format", action="store", type="string", 
                               default='json', dest="format",
             help="specify desired format output: JSON (default), XML, DASJSON, DASXML, PLIST")
    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()
#
# main
#
if __name__ == '__main__':
    optManager   = DASOptionParser()
    (opts, args) = optManager.getOpt()

    host    = opts.host
    debug   = opts.verbose
    request = opts.request
    query   = opts.query
    idx     = opts.idx
    limit   = opts.limit
    if  opts.input:
        params = eval(opts.input)
    elif opts.query:
        params = {'query':query, 'idx':idx, 'limit':limit}
    else:
        msg = 'You need to provide either input dict or query.'
        raise Exception(msg)
#    path    = '/rest/%s/%s' % (opts.format.lower(), opts.request.upper())
    if  opts.request.lower() == 'post':
        method = 'create'
    elif opts.request.lower() == 'put':
        method = 'replace'
    elif opts.request.lower() == 'delete':
        method = 'delete'
    else:
        method = 'request'
    path = '/rest/%s' % method

    if  host.find('http://') == -1:
        host = 'http://' + host
    url = host + path

    headers={"Accept": "application/json"}
    format = opts.format.lower()
    if  format == 'json' or format == 'dasjson':
        headers={"Accept": "application/json"}
    elif format == 'xml' or format == 'dasxml':
        headers={"Accept": "application/xml"}
    else:
        headers={"Accept": "application/%s" % format}

    if  request == 'GET' or request == 'DELETE':
        encoded_data=urllib.urlencode(params, doseq=True)
        url += '?%s' % encoded_data
        req = UrlRequest(request, url=url, headers=headers)
    else:
        headers['Content-type'] = 'application/json'
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

    if  opts.format == 'json' or opts.format == 'dasjson':
        decoder = JSONDecoder()
        print decoder.decode(data)
    else:
        print data
