#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS cache client tools 
"""
__revision__ = "$Id: das_cache_client.py,v 1.4 2009/05/28 19:23:42 valya Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "Valentin Kuznetsov"

import httplib
import urllib
import urllib2

from optparse import OptionParser

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

class DASOptionParser: 
    """
    DAS cache client option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                               type="int", default=0, dest="verbose",
             help="verbose output")
        self.parser.add_option("--input", action="store", type="string", 
                               default=False, dest="input",
             help="specify input for your request.")
        self.parser.add_option("--host", action="store", type="string", 
                               default='http://localhost:8011', dest="host",
             help="specify host name, e.g. http://hostname:port")
        self.parser.add_option("--idx", action="store", type="int", 
                               default=0, dest="idx",
             help="start index for returned result set, aka pagination, use w/ limit")
        self.parser.add_option("--limit", action="store", type="int", 
                               default=10, dest="limit",
             help="limit number of returned results")
        self.parser.add_option("--lib", action="store", type="string", 
                               default='urllib2', dest="lib",
             help="specify which lib to use, httplib or urllib2 (default)")
        self.parser.add_option("--request", action="store", type="string", 
                               default='GET', dest="request",
             help="specify request type: GET, POST, PUT, DELETE")
        self.parser.add_option("--format", action="store", type="string", 
                               default='json', dest="format",
             help="specify desired format output: JSON, XML")
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
    request = opts.request
    debug   = opts.verbose
    params  = {'query':opts.input, 'idx':opts.idx, 'limit':opts.limit}
    path    = '/rest/%s/%s' % (opts.format.lower(), opts.request.upper())

    if  opts.lib == 'urllib2':
        if  host[-1] == '/':
            host = host[:-1]
        data = urllib2_request(host+path, params, debug)
    elif opts.lib == 'httplib':
        data = httplib_request(host, path, params, request, debug)
    else:
        raise Exception('Unsupported lib %s' % opts.lib)
    print data
