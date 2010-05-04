#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS cache RESTfull model, based on WMCore/WebTools
"""

__revision__ = "$Id: DASCacheModel.py,v 1.23 2009/12/08 15:18:15 valya Exp $"
__version__ = "$Revision: 1.23 $"
__author__ = "Valentin Kuznetsov"

# system modules
import re
import time
import types
import thread
import cherrypy
import traceback
import DAS.utils.jsonwrapper as json

# WMCore/WebTools modules
from WMCore.WebTools.RESTModel import RESTModel
#from WMCore.WebTools.Page import exposedasjson
from WMCore.WebTools.Page import exposejson

# DAS modules
from DAS.core.das_core import DASCore
from DAS.core.das_cache import DASCacheMgr
from DAS.utils.utils import getarg, genkey

# monogo db modules
from pymongo.connection import Connection
from pymongo.objectid import ObjectId
from pymongo import DESCENDING, ASCENDING

import sys
if  sys.version_info < (2, 5):
    raise Exception("DAS requires python 2.5 or greater")

def checkargs(func):
    """Decorator to check arguments to REST server"""
    def wrapper (self, *args, **kwds):
        """Wrapper for decorator"""
        # check request headers. For methods POST/PUT
        # we need to read request body to get parameters
        headers = cherrypy.request.headers
        if  cherrypy.request.method == 'POST' or\
            cherrypy.request.method == 'PUT':
            body = cherrypy.request.body.read()
            if  args and kwds:
                msg  = 'Misleading request.\n'
                msg += 'Request: %s\n' % cherrypy.request.method
                msg += 'Headers: %s\n' % headers
                msg += 'Parameters: args=%s, kwds=%s\n' % (args, kwds)
                return {'status':'fail', 'reason': msg}
            jsondict = json.loads(body, encoding='latin-1')
            for key, val in jsondict.items():
                kwds[str(key)] = str(val)

#        headers = cherrypy.request.headers
#        if  headers.has_key('Content-type'):
#            content = headers['Content-type']
#            cherrypy.response.headers['Content-type'] = content
#            if  content in ['application/json', 'text/json', 'text/x-json']:
#                body = cherrypy.request.body.read()
#                if  args and kwds:
#                    msg  = 'Misleading request.'
#                    msg += 'Headers: %s ' % headers
#                    msg += 'Parameters: %s, %s' % (args, kwds)
#                    return {'status':'fail', 'reason': msg}
#                jsondict = json.loads(body, encoding='latin-1')
#                for key, val in jsondict.items():
#                    kwds[str(key)] = str(val)
        pat  = re.compile('^[+]?\d*$')
        supported = ['query', 'idx', 'limit', 'expire', 'method', 
                     'skey', 'order']
        if  not kwds:
            if  args:
                kwds = args[-1]
        keys = []
        if  kwds:
            keys = [i for i in kwds.keys() if i not in supported]
        if  keys:
            msg  = 'Unsupported keys: %s' % keys
            return {'status':'fail', 'reason': msg}
        if  kwds.has_key('idx') and not pat.match(str(kwds['idx'])):
            msg  = 'Unsupported value idx=%s' % (kwds['idx'])
            return {'status':'fail', 'reason': msg}
        if  kwds.has_key('limit') and not pat.match(str(kwds['limit'])):
            msg  = 'Unsupported value limit=%s' % (kwds['limit'])
            return {'status':'fail', 'reason': msg}
        if  kwds.has_key('expire') and not pat.match(str(kwds['expire'])):
            msg  = 'Unsupported value expire=%s' % (kwds['expire'])
            return {'status':'fail', 'reason': msg}
        if  kwds.has_key('order'):
            if  kwds['order'] not in ['asc', 'desc']:
                msg  = 'Unsupported value order=%s' % (kwds['order'])
                return {'status':'fail', 'reason': msg}
        data = func (self, *args, **kwds)
        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def worker(query, expire):
    """
    Worker function which invoke DAS core to update cache for input query
    """
    dascore = DASCore()
#    status  = dascore.update_cache(query, expire)
    status  = dascore.call(query)
    return status

class DASCacheModel(RESTModel):
    """
    DASCacheModel class DAS cache interface. It is based on RESTfull model.
    It supports POST/GET/DELETE/UPDATE method who operates with
    DAS caching systems. The input queries are placed into DAS cache
    queue and served via FIFO. DAS cache retrieves results from 
    appropriate data-service and places them into DAS cache back-end.
    All requests are placed into separate thread.
    """
    def __init__(self, config):
        # keep this line, it defines self.config which is used in WMCore
        self.config   = config 

        RESTModel.__init__(self, config)
        self.version = __version__
        self.methods['GET']= {
            'request':
                {'args':['idx', 'limit', 'query', 'skey', 'order'],
                 'call': self.request, 'version':__version__},
            'nresults':
                {'args':['query'],
                 'call': self.nresults, 'version':__version__},
        }
        self.methods['POST']= {'create':
                {'args':['query', 'expire'],
                 'call': self.create, 'version':__version__}}
        self.methods['PUT']= {'replace':
                {'args':['query', 'expire'],
                 'call': self.replace, 'version':__version__}}
        self.methods['DELETE']= {'delete':
                {'args':['query'],
                 'call': self.delete, 'version':__version__}}

        # define config for DASCacheMgr
        cdict         = self.config.dictionary_()
        self.dascore  = DASCore()
        dbhost        = self.dascore.dasconfig['mongocache_dbhost']
        dbport        = self.dascore.dasconfig['mongocache_dbport']
        self.con      = Connection(dbhost, dbport)
        self.col      = self.con['logging']['db']
        sleep         = cdict.get('sleep', 2)
        verbose       = cdict.get('verbose', None)
        iconfig       = {'sleep':sleep, 'verbose':verbose}
        self.cachemgr = DASCacheMgr(iconfig)
        thread.start_new_thread(self.cachemgr.worker, (worker, ))

    def logdb(self):
        """
        Make entry in Logging DB
        """
        query = cherrypy.request.params.get("query", None)
        qhash = genkey(query)
        doc = dict(qhash=qhash, timestamp=time.time(),
                method=cherrypy.request.method,
                path=cherrypy.request.path_info,
                args=cherrypy.request.params,
                ip=cherrypy.request.remote.ip, 
                hostname=cherrypy.request.remote.name,
                port=cherrypy.request.remote.port)
        self.col.insert(doc)
        keys = ["qhash", "timestamp"] # can be adjusted later
        index_list = [(key, DESCENDING) for key in keys]
        self.col.ensure_index(index_list)

    @checkargs
    def nresults(self, *args, **kwargs):
        """
        HTTP GET request. Ask DAS for total number of records
        for provided query.
        """
        self.logdb()
        data = {'server_method':'nresults'}
        if  kwargs.has_key('query'):
            query = kwargs['query']
            query = self.dascore.mongoparser.requestquery(query)
            data.update({'status':'success'})
            if  hasattr(self.dascore, 'cache'):
                res = self.dascore.cache.nresults(query)
                if  res:
                    data['nresults'] = res
                else:
                    data['status'] = 'not found'
            else:
                res = self.dascore.in_raw_cache_nresults(query)
                if  res:
                    data['status'] = 'success'
                    data['nresults'] = res
                else:
                    data['status'] = 'not found'
        else:
            data.update({'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() })
        self.debug(str(data))
        return data

    @checkargs
    def request(self, *args, **kwargs):
        """
        HTTP GET request.
        Retrieve results from DAS cache.
        """
        self.logdb()
        data = {'server_method':'request'}
        if  kwargs.has_key('query'):
            query = kwargs['query']
            query = self.dascore.mongoparser.requestquery(query)
            idx   = getarg(kwargs, 'idx', 0)
            limit = getarg(kwargs, 'limit', 0)
            skey  = getarg(kwargs, 'skey', '')
            order = getarg(kwargs, 'order', 'asc')
            data.update({'status':'requested', 'idx':idx, 
                     'limit':limit, 'query':query,
                     'skey':skey, 'order':order})
            if  hasattr(self.dascore, 'cache'):
                if  self.dascore.cache.incache(query):
                    res = self.dascore.cache.\
                                get_from_cache(query, idx, limit, skey, order)
                    tot = self.dascore.cache.nresults(query)
                    if  type(res) is types.GeneratorType:
                        data['data'] = [i for i in res]
                    else:
                        data['data'] = res
                    data['status'] = 'success'
                    data['nresults'] = tot
                else:
                    data['status'] = 'not found'
            else:
                if  self.dascore.in_raw_cache(query):
                    res = self.dascore.result(query, idx, limit)
                    if  type(res) is types.GeneratorType:
                        result = []
                        for item in res:
                            if  item not in result:
                                result.append(item)
                        data['data'] = result
                        tot = len(data['data'])
                    else:
                        data['data'] = res
                        tot = 1
                    data['status'] = 'success'
                    data['nresults'] = tot
                else:
                    data['status'] = 'not found'
        else:
            data.update({'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() })
        self.debug(str(data))
        return data

    @checkargs
    def create(self, *args, **kwargs):
        """
        HTTP POST request. 
        Requests the server to create a new resource
        using the data enclosed in the request body.
        Creates new entry in DAS cache for provided query.
        """
        self.logdb()
        data = {'server_method':'create'}
        if  kwargs.has_key('query'):
            query  = kwargs['query']
            query  = self.dascore.mongoparser.requestquery(query)
            expire = getarg(kwargs, 'expire', 600)
            try:
                status = self.cachemgr.add(query, expire)
                data.update({'status':status, 'query':query, 'expire':expire})
            except:
                data.update({'exception':traceback.format_exc(), 
                             'status':'fail'})
        else:
            data.update({'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() })
        self.debug(str(data))
        return data

    @checkargs
    def replace(self, *args, **kwargs):
        """
        HTTP PUT request.
        Requests the server to replace an existing
        resource with the one enclosed in the request body.
        Replace existing query in DAS cache.
        """
        self.logdb()
        data = {'server_method':'replace'}
        if  kwargs.has_key('query'):
            query = kwargs['query']
            query = self.dascore.mongoparser.requestquery(query)
            try:
                self.dascore.remove_from_cache(query)
            except:
                msg  = traceback.format_exc()
                data.update({'status':'fail', 'query':query, 'exception':msg})
                self.debug(str(data))
                return data
            expire = getarg(kwargs, 'expire', 600)
            try:
                status = self.cachemgr.add(query, expire)
                data.update({'status':status, 'query':query, 'expire':expire})
            except:
                data.update({'status':'fail', 'query':query,
                        'exception':traceback.format_exc()})
        else:
            data.update({'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() })
        self.debug(str(data))
        return data

    @checkargs
    def delete(self, *args, **kwargs):
        """
        HTTP DELETE request.
        Delete input query in DAS cache
        """
        self.logdb()
        data = {'server_method':'delete'}
        if  kwargs.has_key('query'):
            query = kwargs['query']
            query = self.dascore.mongoparser.requestquery(query)
            data.update({'status':'requested', 'query':query})
            try:
                self.dascore.remove_from_cache(query)
                data.update({'status':'success'})
            except:
                msg  = traceback.format_exc()
                data.update({'status':'fail', 'exception':msg})
        else:
            data.update({'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() })
        self.debug(str(data))
        return data

