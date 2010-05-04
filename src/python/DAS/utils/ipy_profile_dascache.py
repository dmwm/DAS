#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902
"""
Couch DB command line admin tool
"""

# system modules
import os
import sys
import types
import inspect
import traceback

# ipython modules
from   IPython import Release
import IPython.ipapi
#try:
#    from   IPython.Extensions import ipipe
#except ImportError:
#    pass 
# import DAS modules
from DAS.utils.iprint import PrintManager
#from DAS.web.utils import httplib_request, urllib2_request

try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
from json import JSONDecoder, JSONEncoder

import __main__

#
# load managers
#
try:
    PM = PrintManager()
except:
    traceback.print_exc()

def load():
    msg = """
import traceback
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
from json import JSONDecoder, JSONEncoder
from DAS.web.utils import httplib_request, urllib2_request
from DAS.utils.utils import genkey, timestamp
from DAS.utils.iprint import PrintManager

# global variables
URI="http://localhost:5984"
DB="das"
DESIGN="dasadmin"
DEBUG=0
PM  = PrintManager()

def print_data(data, lookup="value"):
    jsondict = json.loads(data)
    #PM.print_blue("Total %s documents" % jsondict['total_rows'])
    PM.print_blue("Total %s documents" % len(jsondict['rows']))
    maxl = 0
    padding = ""
    for row in jsondict['rows']:
        values = row[lookup]
        if  type(values) is types.DictType:
            if  not padding:
                for key in values.keys():
                    if  len(key) > maxl:
                        maxl = len(key)
            for key, val in values.items():
                padding = " "*(maxl-len(key))
                print "%s%s: %s" % (padding, PM.msg_blue(key), val)
            print
        else:
            print values

"""
    return msg

def set_prompt(in1):
    """Define shell prompt"""
    if  in1.find('|\#>')!=-1:
        in1 = in1.replace('|\#>', '').strip()
    ip = __main__.__dict__['__IP'] 
    prompt = getattr(ip.outputcache, 'prompt1') 
    prompt.p_template = in1 + " |\#> "
    prompt.set_p_str() 

def das_help(self, arg):
    """
    Provide simple help about available commands
    """
    global magic_list
    msg  = "\nAvailable commands:\n"
    for name, func in magic_list:
        msg += "%s\n%s\n" % (PM.msg_blue(name), PM.msg_green(func.__doc__))
    msg += "List of pre-defined variables to control your interactions "
    msg += "with CouchDB:\n"
    msg += PM.msg_green("    URI, DB, DESIGN, DEBUG\n")
    print msg


### MAGIC COMMANDS ###
def db_info():
    """
    Provide information about Couch DB. Use DB parameter to setup
    your couch DB name.
    """
    host  = URI
    path  = '/%s' % DB
    data  = httplib_request(host, path, {}, 'GET', DEBUG)
    return data

def couch_views():
    """
    List registeted views in couch db.
    """
    qqq  = 'startkey=%22_design%2F%22&endkey=%22_design0%22'
    host = URI
    path = '/%s/_all_docs?%s' % (DB, qqq)
    results = httplib_request(host, path, {}, 'GET', DEBUG)
    designdocs = json.loads(results)
    results    = {}
    for item in designdocs['rows']:
        doc   = item['key']
        print PM.msg_blue("design: ") + doc
        path  = '/%s/%s' % (DB, doc)
        res   = httplib_request(host, path, {}, 'GET', DEBUG)
        rdict = json.loads(res)
        for view_name, view_dict in rdict['views'].items():
            print PM.msg_blue("view name: ") + view_name
            print PM.msg_blue("map:") 
            print PM.msg_green(view_dict['map'])
            if  view_dict.has_key('reduce'):
                print PM.msg_blue("reduce:") 
                print PM.msg_green(view_dict['reduce'])

def create_view(view_dict):
    """
    Create couch db view. The db and design names are controlled via
    DB and DESIGN shell parameters, respectively.
    Parameters: <view_dict>
    Example of the view:
    {"view_name": {"map" : "function(doc) { if(doc.hash) {emit(1, doc.hash);}}" }}
    """
    # get existing views
    host  = URI
    path  = '/%s/_design/%s' % (DB, DESIGN)
    data  = httplib_request(host, path, {}, 'GET', DEBUG)
    jsondict = json.loads(data)
    for view_name, view_def in view_dict.items():
        jsondict['views'][view_name] = view_def

    # update views
    encoder = JSONEncoder()
    params  = encoder.encode(jsondict)
    request = 'PUT'
    debug   = DEBUG
    data    = httplib_request(host, path, params, request, debug)
    return data

def delete_view(view_name):
    """
    Delete couch db view. The db and design names are controlled via
    DB and DESIGN shell parameters, respectively.
    Parameters: <view_name>
    """
    # get existing views
    host  = URI
    path  = '/%s/_design/%s' % (DB, DESIGN)
    data  = httplib_request(host, path, {}, 'GET', DEBUG)
    jsondict = json.loads(data)

    # delete requested view in view dict document
    try:
        del jsondict['views'][view_name]
        # update view dict document in a couch
        encoder = JSONEncoder()
        params  = encoder.encode(jsondict)
        request = 'PUT'
        debug   = DEBUG
        data    = httplib_request(host, path, params, request, debug)
    except:
        traceback.print_exc()

def delete_all_views(design):
    """
    Delete all views in particular design document. 
    The db and design names are controlled via
    DB and DESIGN shell parameters, respectively.
    Parameters: <design_name, e.g. dasadmin>
    """
    host  = URI
    path  = '/%s/_design/%s' % (DB, design)
    data  = httplib_request(host, path, {}, 'DELETE', DEBUG)
    return data

def create_db(db_name):
    """
    Create a new DB in couch.
    Parameters: <db_name, e.g. das>
    """
    host  = URI
    path  = '/%s' % db_name
    data  = httplib_request(host, path, {}, 'PUT', DEBUG)
    return data

def delete_db(db_name):
    """
    Delete DB in couch. By default DAS database called das.
    Parameters: <db_name, e.g. das>
    """
    host  = URI
    path  = '/%s' % db_name
    data  = httplib_request(host, path, {}, 'DELETE', DEBUG)
    return data

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
        return data

def das_between(min_time, max_time=9999999999, pretty_print=False):
    """
    Retrieve results from provided time stamp range
    Parameters: <min_time, max_time>
    Comments: times are seconds since epoch, max_time is optional. 
    """
    host  = URI
    path  = '/das/_design/dasadmin/_view/timer'
    kwds  = {'starkey': min_time, 'endkey': max_time}
    data  = httplib_request(host, path, kwds, 'GET', DEBUG)
    if  pretty_print:
        print_data(data)
    else:
        return data

def das_queries(pretty_print=False):
    """
    Get queries which present currently in couch DB.
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
        return data

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
    return data

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

# keep magic list as global since it's used in das_help
magic_list = [
        ('db_info', db_info),
        ('couch_views', couch_views),
        ('create_view', create_view),
        ('delete_view', delete_view),
        ('delete_all_views', delete_all_views),
        ('create_db', create_db),
        ('delete_db', delete_db),
        ('das_system', das_system),
        ('das_between', das_between),
        ('das_queries', das_queries),
        ('das_incache', das_incache),
        ('das_get', das_get),
]
def main():
    """
    Main function which defint ipython behavior
    """

    # global IP API
    ip = IPython.ipapi.get()

    o = ip.options
    # load cms modules and expose them to the shell
    ip.expose_magic('das_help', das_help)
    for m in magic_list:
#        ip.expose_magic(m[0], m[1])
        ip.ex(inspect.getsource(m[1]))
    ip.ex(load())
    
    # autocall to "full" mode (smart mode is default, I like full mode)
    o.autocall = 2
    
    # Jason Orendorff's path class is handy to have in user namespace
    # if you are doing shell-like stuff
    try:
        ip.ex("from path import path" )
    except ImportError:
        pass
    
    ip.ex('import os; import types; import time')
        
    # import ipipe support
    try:
        ip.ex("from ipipe import *")
    except ImportError:
        pass

    # import local logger
#    ip.ex("import cmssh_logger")

    # Set dbsh prompt
    o.prompt_in1 = 'das-sh |\#> '
    o.prompt_in2 = 'das-sh> '
    o.system_verbose = 0
    
    # define dbsh banner
    pyver  = sys.version.split('\n')[0]
    ipyver = Release.version
    msg    = "Welcome to das-sh \n[python %s, ipython %s]\n%s\n" \
            % (pyver, ipyver ,os.uname()[3])
    msg   += "For das-sh help use "
    msg   += PM.msg_blue("das_help")
    msg   += ", for python help use help commands\n"
    o.banner = msg
    o.prompts_pad_left = "1"
    # Remove all blank lines in between prompts, like a normal shell.
    o.separate_in = "0"
    o.separate_out = "0"
    o.separate_out2 = "0"

main()
