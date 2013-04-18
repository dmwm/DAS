#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-

"""
File: url_utils.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: pycurl module for processing multiple URLs.
Credits: original idea has been taken and modified from
https://github.com/Lispython/pycurl/blob/master/examples/retriever-multi.py
"""

import os
import re
import pycurl

# We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
# the libcurl tutorial for more info.
try:
    import signal
    from signal import SIGPIPE, SIG_IGN
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
    pass

try:
    import cStringIO as StringIO
except:
    import StringIO

PAT = re.compile(\
        "(https|http)://[-A-Za-z0-9_+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]")

def validate_url(url):
    "Validate URL"
    if  PAT.match(url):
        return True
    return False

def getdata(urls, ckey, cert, num_conn=10):
    """
    Get data for given list of urls, using provided number of connections
    and user credentials
    """

    # Make a queue with urls
    queue = [u for u in urls if validate_url(u)]

    # Check args
    num_urls = len(queue)
    num_conn = min(num_conn, num_urls)

    # Pre-allocate a list of curl objects
    mcurl = pycurl.CurlMulti()
    mcurl.handles = []
    for _ in range(num_conn):
        curl = pycurl.Curl()
        curl.fp = None
        curl.setopt(pycurl.FOLLOWLOCATION, 1)
        curl.setopt(pycurl.MAXREDIRS, 5)
        curl.setopt(pycurl.CONNECTTIMEOUT, 30)
        curl.setopt(pycurl.TIMEOUT, 300)
        curl.setopt(pycurl.NOSIGNAL, 1)
        curl.setopt(pycurl.SSLKEY, ckey)
        curl.setopt(pycurl.SSLCERT, cert)
        curl.setopt(pycurl.SSL_VERIFYPEER, False)
        mcurl.handles.append(curl)

    # Main loop
    freelist = mcurl.handles[:]
    num_processed = 0
    while num_processed < num_urls:
        # If there is an url to process and a free curl object,
        # add to multi-stack
        while queue and freelist:
            url = queue.pop(0)
            curl = freelist.pop()
            curl.setopt(pycurl.URL, url.encode('ascii', 'ignore'))
            bbuf = StringIO.StringIO()
            hbuf = StringIO.StringIO()
            curl.setopt(pycurl.WRITEFUNCTION, bbuf.write)
            curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
            mcurl.add_handle(curl)
            # store some info
            curl.hbuf = hbuf
            curl.bbuf = bbuf
            curl.url = url
        # Run the internal curl state machine for the multi stack
        while 1:
            ret, _num_handles = mcurl.perform()
            if  ret != pycurl.E_CALL_MULTI_PERFORM:
                break
        # Check for curl objects which have terminated, and add them to the
        # freelist
        while 1:
            num_q, ok_list, err_list = mcurl.info_read()
            for curl in ok_list:
                _hdrs  = curl.hbuf.getvalue()
                data   = curl.bbuf.getvalue()
                curl.bbuf.flush()
                curl.bbuf.close()
                curl.hbuf.close()
                curl.hbuf = None
                curl.bbuf = None
                mcurl.remove_handle(curl)
                freelist.append(curl)
                yield data
            for curl, errno, errmsg in err_list:
                _hdrs = curl.hbuf.getvalue()
                data = curl.bbuf.getvalue()
                curl.bbuf.flush()
                curl.bbuf.close()
                curl.hbuf.close()
                curl.hbuf = None
                curl.bbuf = None
                mcurl.remove_handle(curl)
                print "Failed: ", curl.url, errno, errmsg
                freelist.append(curl)
            num_processed = num_processed + len(ok_list) + len(err_list)
            if num_q == 0:
                break
        # Currently no more I/O is pending, could do something in the meantime
        # (display a progress bar, etc.).
        # We just call select() to sleep until some more data is available.
        mcurl.select(1.0)

    # Cleanup
    for curl in mcurl.handles:
        if  curl.hbuf is not None:
            curl.hbuf.close()
            curl.hbuf = None
        if  curl.bbuf is not None:
            curl.bbuf.close()
            curl.bbuf = None
        curl.close()
    mcurl.close()

def test():
    "Test function"
    ckey = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
    cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
    url1 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/help"
    url2 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatiers"
    urls = [url1, url2]
    data = getdata(urls, ckey, cert)
    for row in data:
        print row

if __name__ == '__main__':
    test()
