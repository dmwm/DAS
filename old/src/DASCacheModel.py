#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS cache RESTfull model class.
"""
from __future__ import print_function

__revision__ = "$Id: DASCacheModel.py,v 1.1 2010/03/18 17:52:25 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

# system modules
import re
import sys
import time
import types
import thread
import cherrypy
import traceback
import DAS.utils.jsonwrapper as json

from cherrypy import expose

# DAS modules
from DAS.core.das_core import DASCore
from DAS.core.das_cache import DASCacheMgr
from DAS.utils.utils import getarg, genkey

#try:
    # WMCore/WebTools modules
#    from WMCore.WebTools.RESTModel import RESTModel
#    from WMCore.WebTools.Page import exposejson
#except:
    # stand-alone version
#    from DAS.web.tools import exposejson

from DAS.web.tools import exposejson
from DAS.web.das_webmanager import DASWebManager

# monogo db modules
from pymongo.connection import Connection
from pymongo.objectid import ObjectId
from pymongo import DESCENDING, ASCENDING

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
            if  body:
                jsondict = json.loads(body, encoding='latin-1')
            else:
                jsondict = kwds
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
                     'skey', 'order', 'collection']
        if  not kwds:
            if  args:
                kwds = args[-1]
        keys = []
        if  kwds:
            keys = [i for i in kwds.keys() if i not in supported]
        if  keys:
            msg  = 'Unsupported keys: %s' % keys
            return {'status':'fail', 'reason': msg}
        if  'idx' in kwds and not pat.match(str(kwds['idx'])):
            msg  = 'Unsupported value idx=%s' % (kwds['idx'])
            return {'status':'fail', 'reason': msg}
        if  'limit' in kwds and not pat.match(str(kwds['limit'])):
            msg  = 'Unsupported value limit=%s' % (kwds['limit'])
            return {'status':'fail', 'reason': msg}
        if  'expire' in kwds and not pat.match(str(kwds['expire'])):
            msg  = 'Unsupported value expire=%s' % (kwds['expire'])
            return {'status':'fail', 'reason': msg}
        if  'order' in kwds:
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
    status  = dascore.call(query)
    return status

class DASCacheModel(DASWebManager):
    """
    DASCacheModel represents DAS cache RESTful interface.
    It supports POST/GET/DELETE/UPDATE methods who communicate with
    DAS caching systems. The input queries are placed into DAS cache
    queue and served via FIFO mechanism. 
    """
    def __init__(self, config):
        self.config  = config 
        DASWebManager.__init__(self, config)
        self.version = __version__
        self.methods = {}
        self.methods['GET']= {
            'request':
                {'args':['idx', 'limit', 'query', 'skey', 'order'],
                 'call': self.request, 'version':__version__},
            'nresults':
                {'args':['query'],
                 'call': self.nresults, 'version':__version__},
            'records':
                {'args':['query', 'count', 'collection'],
                 'call': self.records, 'version':__version__},
            'status':
                {'args':['query'],
                 'call': self.status, 'version':__version__},
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

        try:
            # WMCore/WebTools
            rest  = RESTModel(config)
            rest.methods = self.methods # set RESTModel methods
            self.model = self # re-reference model to my class
            self.model.handler = rest.handler # reference handler to RESTModel
            cdict = self.config.dictionary_()
            self.base = '/rest'
        except:
            cdict = {}
            self.base = ''

        self.dascore  = DASCore()
        dbhost        = self.dascore.dasconfig['mongocache_dbhost']
        dbport        = self.dascore.dasconfig['mongocache_dbport']
        capped_size   = self.dascore.dasconfig['mongocache_capped_size']
        self.con      = Connection(dbhost, dbport)
        if  'logging' not in self.con.database_names():
            db = self.con['logging']
            options = {'capped':True, 'size': capped_size}
            db.create_collection('db', options)
            self.warning('Created logging.db, size=%s' % capped_size)
        self.col      = self.con['logging']['db']
        sleep         = cdict.get('sleep', 2)
        verbose       = cdict.get('verbose', None)
        iconfig       = {'sleep':sleep, 'verbose':verbose, 
                         'logger':self.dascore.logger}
        self.cachemgr = DASCacheMgr(iconfig)
        thread.start_new_thread(self.cachemgr.worker, (worker, ))
        msg = 'DASCacheMode::init, host=%s, port=%s, capped_size=%s' \
                % (dbhost, dbport, capped_size)
        self.dascore.logger.debug(msg)
        print(msg)

    def logdb(self, query):
        """
        Make entry in Logging DB
        """
        qhash = genkey(query)
        headers = cherrypy.request.headers
        doc = dict(qhash=qhash, timestamp=time.time(),
                headers=cherrypy.request.headers,
                method=cherrypy.request.method,
                path=cherrypy.request.path_info,
                args=cherrypy.request.params,
                ip=cherrypy.request.remote.ip, 
                hostname=cherrypy.request.remote.name,
                port=cherrypy.request.remote.port)
        self.col.insert(doc)

    @checkargs
    def records(self, *args, **kwargs):
        """
        HTTP GET request.
        Retrieve records from provided collection.
        """
        data  = {'server_method':'request'}
        if  'query' not in kwargs:
            data['status'] = 'fail'
            data['reason'] = 'no query is provided'
            return data
        # input query in JSON format, we should decode it using json.
        query = json.loads(kwargs.get('query'))
        coll  = kwargs.get('collection', 'merge')
        idx   = getarg(kwargs, 'idx', 0)
        limit = getarg(kwargs, 'limit', 10) # getarg perfrom type convertion
        count = kwargs.get('count', 0)
        data.update({'status':'requested', 'query':kwargs['query'], 
                 'collection':coll, 'count': count})
        if  '_id' in query['spec']:
            recid = query['spec']['_id']
            ids   = []
            if  type(recid) is bytes:
                ids = [ObjectId(recid)]
            elif type(recid) is list:
                ids = [ObjectId(r) for r in recid]
            spec = {'spec':{'_id':{'$in':ids}}}
        else: # look-up all records
            spec = {}
        self.logdb(query)
        try:
            gen = self.dascore.rawcache.get_from_cache\
                (spec, idx=idx, limit=limit, collection=coll, adjust=False)
            data['status'] = 'success'
            data['data']   = [r for r in gen]
        except:
            self.debug(traceback.format_exc())
            data['status'] = 'fail'
            data['reason'] =  sys.exc_info()[0]
        return data

    @checkargs
    def status(self, *args, **kwargs):
        """
        HTTP GET request. Check status of the input query in DAS.
        """
        data = {'server_method':'status'}
        if  'query' in kwargs:
            query  = kwargs['query']
            self.logdb(query)
            query  = self.dascore.mongoparser.parse(query)
            status = self.dascore.get_status(query)
            if  not status:
                status = 'no data' 
            data.update({'status':status})
        else:
            data.update({'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() })
        return data

    @checkargs
    def nresults(self, *args, **kwargs):
        """
        HTTP GET request. Ask DAS for total number of records
        for provided query.
        """
        data = {'server_method':'nresults'}
        if  'query' in kwargs:
            query = kwargs['query']
            self.logdb(query)
            query = self.dascore.mongoparser.parse(query)
            data.update({'status':'success'})
            res = self.dascore.in_raw_cache_nresults(query)
            data.update({'status':'success', 'nresults':res})
        else:
            data.update({'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() })
        return data

    @checkargs
    def request(self, *args, **kwargs):
        """
        HTTP GET request.
        Retrieve results from DAS cache.
        """
        data = {'server_method':'request'}
        if  'query' in kwargs:
            query = kwargs['query']
            self.logdb(query)
            query = self.dascore.mongoparser.parse(query)
            idx   = getarg(kwargs, 'idx', 0)
            limit = getarg(kwargs, 'limit', 0)
            skey  = getarg(kwargs, 'skey', '')
            order = getarg(kwargs, 'order', 'asc')
            data.update({'status':'requested', 'idx':idx, 
                     'limit':limit, 'query':query,
                     'skey':skey, 'order':order})
#            if  self.dascore.in_raw_cache(query):
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
#            else:
#                data['status'] = 'not found'
        else:
            data.update({'status': 'fail', 
                    'reason': 'Unsupported keys %s' % kwargs.keys() })
        return data

    @checkargs
    def create(self, *args, **kwargs):
        """
        HTTP POST request. 
        Requests the server to create a new resource
        using the data enclosed in the request body.
        Creates new entry in DAS cache for provided query.
        """
        data = {'server_method':'create'}
        if  'query' in kwargs:
            query  = kwargs['query']
            self.logdb(query)
            query  = self.dascore.mongoparser.parse(query)
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
        return data

    @checkargs
    def replace(self, *args, **kwargs):
        """
        HTTP PUT request.
        Requests the server to replace an existing
        resource with the one enclosed in the request body.
        Replace existing query in DAS cache.
        """
        data = {'server_method':'replace'}
        if  'query' in kwargs:
            query = kwargs['query']
            self.logdb(query)
            query = self.dascore.mongoparser.parse(query)
            try:
                self.dascore.remove_from_cache(query)
            except:
                msg  = traceback.format_exc()
                data.update({'status':'fail', 'query':query, 'exception':msg})
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
        return data

    @checkargs
    def delete(self, *args, **kwargs):
        """
        HTTP DELETE request.
        Delete input query in DAS cache
        """
        data = {'server_method':'delete'}
        if  'query' in kwargs:
            query = kwargs['query']
            self.logdb(query)
            query = self.dascore.mongoparser.parse(query)
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
        return data

    @exposejson
    def rest(self, *args, **kwargs):
        """
        RESTful interface. We use args tuple as access method(s), e.g.
        args = ('method',) and kwargs to represent input parameters.
        """
        request = cherrypy.request.method
        if  request not in self.methods.keys():
            msg = "Usupported request '%s'" % requset
            return {'error': msg}
        method  = args[0]
        if  method not in self.methods[request].keys():
            msg  = "Unsupported method '%s'" % method
            return {'error': msg}
        if  request == 'POST':
            if  cherrypy.request.body:
                body = cherrypy.request.body.read()
                try:
                    kwargs = json.loads(body)
                except:
                    msg = "Unable to load body request"
                    return {'error': msg}
        return getattr(self, method)(kwargs)
