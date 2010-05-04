#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902
"""
CMS DAS specific implementation for ipython.
"""
__revision__ = "$Id: das_ipython.py,v 1.1 2009/06/18 17:56:13 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import IPython.ipapi
import inspect
from DAS.utils.utils import genkey, timestamp
from DAS.utils.iprint import PrintManager

### DAS MAGIC commands
def das_system(system, pretty_print=False):
    """
    Retrieve results from DAS cache for provided system, e.g. sitedb.
    Parameters: <das sub-system, e.g. sitedb> <pretty_print=False> 
    """
    host  = URI
    path  = '/das/_design/dasadmin/_view/system'
    kwds  = {'starkey': system, 'endkey': system}
    data  = httplib_request(host, path, kwds, 'GET', DEBUG)
    if  pretty_print:
        print_data(data)
    else:
        return json.loads(data)

def das_between(min_time, max_time=9999999999, pretty_print=False):
    """
    Retrieve results from provided time stamp range
    Parameters: <min_time, max_time> <pretty_print=False>
    Comments: times are seconds since epoch, max_time is optional. 
    """
    host  = URI
    path  = '/das/_design/dasadmin/_view/timer'
    kwds  = {'starkey': min_time, 'endkey': max_time}
    data  = httplib_request(host, path, kwds, 'GET', DEBUG)
    if  pretty_print:
        print_data(data)
    else:
        return json.loads(data)

def das_queries(pretty_print=False):
    """
    Get queries which present currently in couch DB.
    Parameters: <pretty_print=False>
    """
    host  = URI
    # pass group=true to the view to get the grouping done
    # the parameter should be in URL, rather passed in a kwds
    path  = '/das/_design/dasadmin/_view/all_queries?group=true'
    kwds  = {}
    data  = httplib_request(host, path, kwds, 'GET', DEBUG)
    if  pretty_print:
        print_data(data, lookup='key')
    else:
        return json.loads(data)

def das_get(query):
    """
    Get results from couch for provided DAS query
    Parameters: <das query, find dataset where dataset=/a/b/c> 
    """
    host  = URI
    path  = '/das/_design/dasviews/_view/query'
    key   = genkey(query)
    skey  = ["%s" % key, timestamp()]
    ekey  = ["%s" % key, 9999999999]
    kwds  = {'startkey': skey, 'endkey': ekey}
    data  = httplib_request(host, path, kwds, 'GET', DEBUG)
    return json.loads(data)

def das_incache(query):
    """
    Check if result for DAS query exists in couch db.
    Parameters: <das query, find dataset where dataset=/a/b/c> 
    """
    host  = URI
    path  = '/das/_design/dasviews/_view/query'
    key   = genkey(query)
    skey  = ["%s" % key, timestamp()]
    ekey  = ["%s" % key, 9999999999]
    kwds  = {'startkey': skey, 'endkey': ekey}
    data  = httplib_request(host, path, kwds, 'GET', DEBUG)
    jsondict = json.loads(data)
    res = len(jsondict['rows'])
    return res

magic_list = [
        ('das_system', das_system),
        ('das_between', das_between),
        ('das_queries', das_queries),
        ('das_incache', das_incache),
        ('das_get', das_get),
]

PM = PrintManager()

def das_help(self, arg):
    """
    Provide simple help about available DAS commands.
    """
    global magic_list
    msg  = "\nAvailable DAS commands:\n"
    for name, func in magic_list:
        msg += "%s\n%s\n" % (PM.msg_blue(name), PM.msg_green(func.__doc__))
    print msg

def das_load():
    "Load all magic commads we defined to DAS module"
    ip = IPython.ipapi.get()
    ip.expose_magic('das_help', das_help)
    for m in magic_list:
        ip.ex(inspect.getsource(m[1]))
