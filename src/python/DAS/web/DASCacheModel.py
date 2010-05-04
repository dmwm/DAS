#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS cache RESTfull model, based on WMCore/WebTools
"""

__revision__ = "$Id: DASCacheModel.py,v 1.10 2009/07/09 16:00:02 valya Exp $"
__version__ = "$Revision: 1.10 $"
__author__ = "Valentin Kuznetsov"

# system modules
import re
import types
import thread
import traceback

# WMCore/WebTools modules
from WMCore.WebTools.RESTModel import RESTModel
#from WMCore.WebTools.Page import exposedasjson
from WMCore.WebTools.Page import exposejson

# DAS modules
from DAS.core.das_core import DASCore
from DAS.core.das_cache import DASCacheMgr
from DAS.utils.utils import getarg

import sys
if  sys.version_info < (2, 5):
    raise Exception("DAS requires python 2.5 or greater")

def checkargs(func):
    """Decorator to check arguments to REST server"""
    def wrapper (self, *args, **kwds):
        """Wrapper for decorator"""
        data = func (self, *args, **kwds)
        pat  = re.compile('^[+]?\d*$')
        supported = ['query', 'idx', 'limit', 'expire', 'method', 
                     'skey', 'order']
        keys = [i for i in kwds.keys() if i not in supported]
        if  keys:
            msg  = 'Unsupported keys: %s' % keys
            return {'status':'fail', 'reason': msg}
        if  kwds.has_key('idx') and not pat.match(kwds['idx']):
            msg  = 'Unsupported value idx=%s' % (kwds['idx'])
            return {'status':'fail', 'reason': msg}
        if  kwds.has_key('limit') and not pat.match(kwds['limit']):
            msg  = 'Unsupported value limit=%s' % (kwds['limit'])
            return {'status':'fail', 'reason': msg}
        if  kwds.has_key('expire') and not pat.match(kwds['expire']):
            msg  = 'Unsupported value expire=%s' % (kwds['expire'])
            return {'status':'fail', 'reason': msg}
        if  kwds.has_key('order'):
            if  kwds['order'] not in ['asc', 'desc']:
                msg  = 'Unsupported value order=%s' % (kwds['order'])
                return {'status':'fail', 'reason': msg}
        pat  = re.compile('^find ')
        if  kwds.has_key('query') and not pat.match(kwds['query']):
            msg = 'Unsupported keyword query=%s' % kwds['query']
            return {'status':'fail', 'reason': msg}
        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def worker(item):
    """
    Worker function which invoke DAS core to update cache for input query
    """
    dascore = DASCore()
    query, expire = item
    status  = dascore.update_cache(query, expire)
    return status

class DASCacheModel(RESTModel):
    """
    Interface representing DAS cache. It is based on RESTfull model.
    It supports POST/GET/DELETE/UPDATE methods who operates with
    DAS caching systems. The input queries are placed into DAS cache
    queue and served via FIFO. DAS cache retrieve results from 
    appropriate data-service and place them into DAS cache back-end.
    All requests are placed into separate thread.
    """
    def __init__(self, config):
        # keep this line, it defines self.config which is used in WMCore
        self.config   = config 

        # define config for DASCacheMgr
        cdict         = self.config.dictionary_()
        self.dascore  = DASCore()
        sleep         = getarg(cdict, 'sleep', 2)
        verbose       = getarg(cdict, 'verbose', None)
        iconfig       = {'sleep':sleep, 'verbose':verbose}
        self.cachemgr = DASCacheMgr(iconfig)
        thread.start_new_thread(self.cachemgr.worker, (worker, ))

    @exposejson
    @checkargs
    def handle_get(self, *args, **kwargs):
        """
        HTTP GET request.
        Retrieve results from DAS cache.
        """
        data = {}
        if  kwargs.has_key('query'):
            query = kwargs['query']
            idx   = getarg(kwargs, 'idx', 0)
            limit = getarg(kwargs, 'limit', 0)
            skey  = getarg(kwargs, 'skey', '')
            order = getarg(kwargs, 'order', 'asc')
            data  = {'status':'requested', 'idx':idx, 
                     'limit':limit, 'query':query,
                     'skey':skey, 'order':order}
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
                elif self.dascore.in_raw_cache(query):
                    data['status'] = 'in raw cache'
                else:
                    data['status'] = 'not found'
            else:
                data['status'] = 'no cache'
        else:
            data = {'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() }
        self.debug(str(data))
        return data

    @exposejson
    @checkargs
    def handle_post(self, *args, **kwargs):
        """
        HTTP POST request. 
        Requests the server to create a new resource
        using the data enclosed in the request body.
        Creates new entry in DAS cache for provided query.
        """
        if  kwargs.has_key('query'):
            query  = kwargs['query']
            expire = getarg(kwargs, 'expire', 600)
            try:
                status = self.cachemgr.add(query, expire)
                data   = {'status':status, 'query':query, 'expire':expire}
            except:
                data['exception'] = traceback.format_exc()
                data['status'] = 'fail'
        else:
            data = {'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() }
        self.debug(str(data))
        return data

    @exposejson
    @checkargs
    def handle_put(self, *args, **kwargs):
        """
        HTTP PUT request.
        Requests the server to replace an existing
        resource with the one enclosed in the request body.
        Replace existing query in DAS cache.
        """
        data = self.handle_delete(*args, **kwargs)
        if  data['status'] == 'success':
            data = self.handle_post(*args, **kwargs)
        else:
            data = {'status':'fail'}
        self.debug(str(data))
        return data

    @exposejson
    @checkargs
    def handle_delete(self, *args, **kwargs):
        """
        HTTP DELETE request.
        Delete input query in DAS cache
        """
        data = {}
        if  kwargs.has_key('query'):
            query  = kwargs['query']
            data   = {'status':'requested', 'query':query}
            try:
                self.dascore.remove_from_cache(query)
                data = {'status':'success'}
            except:
                msg  = traceback.format_exc()
                data = {'status':'fail', 'exception':msg}
        else:
            data = {'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() }
        self.debug(str(data))
        return data

