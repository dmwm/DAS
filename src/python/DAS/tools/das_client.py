#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line tool
"""
__author__ = "Valentin Kuznetsov"

import re
import sys
import time
import urllib
import urllib2
from   optparse import OptionParser

if  sys.version_info < (2, 6):
    raise Exception("DAS requires python 2.6 or greater")

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
        self.parser.add_option("--host", action="store", type="string", 
                               default='https://cmsweb.cern.ch', dest="host",
             help="specify host name of DAS cache server, default https://cmsweb.cern.ch")
        self.parser.add_option("--idx", action="store", type="int", 
                               default=0, dest="idx",
             help="start index for returned result set, aka pagination, use w/ limit")
        self.parser.add_option("--limit", action="store", type="int", 
                               default=10, dest="limit",
             help="number of returned results (results per page)")
    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def main():
    """Main function"""
    optmgr  = DASOptionParser()
    opts, _ = optmgr.getOpt()
    host    = opts.host
    debug   = opts.verbose
    query   = opts.query
    idx     = opts.idx
    limit   = opts.limit
    if  opts.query:
        params = {'input':query, 'idx':idx, 'limit':limit}
    else:
        raise Exception('You must provide input query')
    path    = '/das/cache'
    pat     = re.compile('http[s]{0,1}://')
    if  not pat.match(host):
        msg = 'Invalid hostname: %s' % host
        raise Exception(msg)
    url = host + path
    headers = {"Accept": "application/json"}
    encoded_data = urllib.urlencode(params, doseq=True)
    url += '?%s' % encoded_data
    req  = urllib2.Request(url=url, headers=headers)
    if  debug:
        h   = urllib2.HTTPHandler(debuglevel=1)
        opener = urllib2.build_opener(h)
    else:
        opener = urllib2.build_opener()
    fdesc = opener.open(req)
    data = fdesc.read()
    fdesc.close()

    pat = re.compile(r'(^[0-9]$|^[0-9][0-9]*$)')
    if  data and pat.match(data[0]):
        pid = data
    else:
        pid = None
    count = 2 # initial waiting time
    while pid:
        params.update({'pid':data})
        encoded_data = urllib.urlencode(params, doseq=True)
        url  = host + path + '?%s' % encoded_data
        req  = urllib2.Request(url=url, headers=headers)
        fdesc = opener.open(req)
        data = fdesc.read()
        fdesc.close()
        if  data and pat.match(data[0]):
            pid = data
        else:
            pid = None
        time.sleep(count)
        if  count < 60:
            count *= 2
    print data

#
# main
#
if __name__ == '__main__':
    main()
