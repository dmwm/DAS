#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_cache_client.py,v 1.1 2009/05/27 20:28:04 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import httplib
import urllib
import urllib2

httplib.HTTPConnection.debuglevel = 1
def get_request(iurl, iparams):
    """request method using GET request from urllib2 library"""
    res = urllib2.urlopen(iurl, urllib.urlencode(iparams, doseq=True))
    return res.read()

def post_request(ihost, ipath, iparams, irequest='POST', idebug=0):
    """request method using provided HTTP request and httplib library"""
    if  idebug:
        httplib.HTTPConnection.debuglevel = 1
    iparams = urllib.urlencode(iparams, doseq=True)
    print iparams
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": "text/plain"}
    conn = httplib.HTTPConnection(ihost)
    conn.request(irequest, ipath, iparams, headers)
    response = conn.getresponse()

    if  response.reason != "OK":
        print response.status, response.reason
        return 0
    else:
        res = response.read()
        return res
    conn.close()

host = 'localhost:8011'

#request = 'DELETE'
#path = '/rest/json/%s' % request
#params = {'query': 'find site where site=T2_UK'}
#print "DELETE"
#data = post_request(host, path, params, request, debug=1)
#print data
#data = get_request('http://'+host+path, params)
#print data

#request = 'GET'
#path = '/rest/json/%s' % request
#params = {'query': 'find site where site=T2_UK'}
#print "GET"
#data = post_request(host, path, params, request, debug=1)
#print data
#data = get_request('http://'+host+path, params)
#print data

#request = 'PUT'
#path = '/rest/json/%s' % request
#params = {'query': 'find site where site=T2_UK'}
#print 'PUT'
#data = post_request(host, path, params, request, debug=1)
#print data
#data = get_request('http://'+host+path, params)
#print data

request = 'POST'
path = '/rest/json/%s' % request
params = {'query': 'find site where site=T2_UK', 'idx':0, 'limit':10}
print 'POST'
#data = post_request(host, path, params, request, debug=1)
#print data
data = get_request('http://'+host+path, params)
print data

params = {'query': 'find site where site=T2', 'idx':0, 'limit':10}
data = get_request('http://'+host+path, params)
print data

#time.sleep(1)

params = {'query': 'find site where site=T2_UK', 'idx':0, 'limit':10}
data = get_request('http://'+host+path, params)
print data
