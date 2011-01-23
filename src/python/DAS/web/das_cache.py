#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS cache RESTfull model class.
"""

__revision__ = "$Id: das_cache.py,v 1.11 2010/05/04 18:03:23 valya Exp $"
__version__ = "$Revision: 1.11 $"
__author__ = "Valentin Kuznetsov"

# system modules
import re
import sys
import time
from   types import GeneratorType
import thread
import cherrypy
from   cherrypy import HTTPError, expose
import traceback

# monogo db modules
from pymongo.objectid import ObjectId

# DAS modules
import DAS.utils.jsonwrapper as json
from DAS.core.das_core import DASCore
from DAS.core.das_cache import DASCacheMgr, thread_monitor
from DAS.utils.das_db import db_connection, connection_monitor
from DAS.utils.utils import getarg, genkey
from DAS.utils.logger import DASLogger, set_cherrypy_logger
from DAS.utils.das_config import das_readconfig
from DAS.utils.regex import web_arg_pattern
from DAS.web.tools import exposejson
from DAS.web.das_webmanager import DASWebManager
from DAS.web.das_codes import web_code
from DAS.web.utils import checkargs
from DAS.utils.das_db import db_gridfs

DAS_CACHE_INPUTS = ['query', 'idx', 'limit', 'expire', 'method', 
             'skey', 'order', 'collection', 'fid']

def cache_cleaner(dburi, collections, sleep):
    """
    Clean-up expired records in provided set of DAS caches
    """
    conn = db_connection(dburi)
    while True:
        time.sleep(sleep)
        for name in collections:
            dbname, collname = name.split('.')
            if  dbname == 'analytics':
                spec = {'apicall.expire': {'$lt' : int(time.time())}}
            else:
                spec = {'das.expire' : {'$lt' : int(time.time())}}
            conn[dbname][collname].remove(spec)
        
class DASCacheService(DASWebManager):
    """
    DASCacheService represents DAS cache RESTful interface.
    It supports POST/GET/DELETE/UPDATE methods who communicate with
    DAS caching systems. The input queries are placed into DAS cache
    queue and served via FIFO mechanism. 
    """
    def __init__(self, config):
        self.config  = config 
        DASWebManager.__init__(self, config)
        self.version = __version__

        # do not allow caching
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        cherrypy.response.headers['Pragma'] = 'no-cache'

        self.methods = {}
        self.methods['GET'] = {
            'request':
                {'args':['idx', 'limit', 'query', 'skey', 'order'],
                 'call': self.request, 'version':__version__},
            'test':
                {'args':['idx', 'limit', 'query', 'skey', 'order'],
                 'call': self.test, 'version':__version__},
            'testmongo':
                {'args':['idx', 'limit', 'query', 'skey', 'order', 'collection'],
                 'call': self.testmongo, 'version':__version__},
            'nresults':
                {'args':['query'],
                 'call': self.nresults, 'version':__version__},
            'records':
                {'args':['query', 'count', 'collection'],
                 'call': self.records, 'version':__version__},
            'gridfs':
                {'args':['fid'],
                 'call': self.gridfs, 'version':__version__},
            'status':
                {'args':['query'],
                 'call': self.status, 'version':__version__},
        }
        self.methods['POST'] = {'create':
                {'args':['query', 'expire'],
                 'call': self.create, 'version':__version__}}
        self.methods['PUT'] = {'replace':
                {'args':['query', 'expire'],
                 'call': self.replace, 'version':__version__}}
        self.methods['DELETE'] = {'delete':
                {'args':['query'],
                 'call': self.delete, 'version':__version__}}
        self.dasconfig   = das_readconfig()
        self.dburi       = self.dasconfig['mongodb']['dburi']
        self.qlimit      = self.dasconfig['cache_server']['queue_limit'] 

        sleep         = self.dasconfig.get('sleep', 2)
        verbose       = int(config.get('loglevel'))
        logfile       = config.get('logfile')
        logformat     = '%(levelname)s - %(message)s'
        nprocs        = self.dasconfig['cache_server']['n_worker_threads']
        logger        = DASLogger(logfile=logfile, verbose=verbose, 
                        name='DASCacheServer', format=logformat)
        self.logger   = logger
        set_cherrypy_logger(self.logger.handler, verbose)
        iconfig       = {'sleep':sleep, 'verbose':verbose, 'nprocs':nprocs,
                        'logfile':logfile}
        self.cachemgr = DASCacheMgr(iconfig)
        thread.start_new_thread(thread_monitor, (self.cachemgr, iconfig))

        msg  = 'DASCacheService::init'
        self.logger.info(msg)

        self.init()
        # Monitoring thread which performs auto-init of the server
        thread.start_new_thread(connection_monitor, (self.dburi, self.init, 5))

        # thread which performs clean-up of DAS caches
        sleep = self.dasconfig['cache_server'].get('clean_interval', 600) # seconds
        collections = ['das.cache', 'das.merge', 'analytics.db']
        thread.start_new_thread(cache_cleaner, (self.dburi, collections, sleep))

    def init(self):
        """
        Init DAS cache server, connect to DAS Core, etc.
        """
        capped_size = self.dasconfig['loggingdb']['capped_size']
        logdbname   = self.dasconfig['loggingdb']['dbname']
        logdbcoll   = self.dasconfig['loggingdb']['collname']
        try:
            self.con      = db_connection(self.dburi)
            if  logdbname not in self.con.database_names():
                dbname    = self.con[logdbname]
                options   = {'capped':True, 'size': capped_size}
                dbname.create_collection('db', **options)
                self.warning('Created %s.%s, size=%s' \
                % (logdbname, logdbcoll, capped_size))
            self.col      = self.con[logdbname][logdbcoll]
            self.dascore  = DASCore()
            self.gfs      = db_gridfs(self.dburi)
        except:
            self.con  = None
            self.dascore = None

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

    @expose
    @checkargs(DAS_CACHE_INPUTS)
    def records(self, *args, **kwargs):
        """
        HTTP GET request.
        Retrieve records from provided collection.
        """
        time0 = time.time()
        msg = 'records(%s, %s)' % (args, kwargs)
        self.logger.info(msg)
        data  = {'server_method':'request'}
        if  not kwargs.has_key('query'):
            code = web_code('Invalid query')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        # input query in JSON format, we should decode it using json.
        query = json.loads(kwargs.get('query'))
        coll  = kwargs.get('collection', 'merge')
        idx   = getarg(kwargs, 'idx', 0)
        limit = getarg(kwargs, 'limit', 10) # getarg perfrom type convertion
        count = kwargs.get('count', 0)
        data.update({'status':'requested', 'query':kwargs['query'], 
                 'collection':coll, 'count': count})
        if  query['spec'].has_key('_id'):
            recid = query['spec']['_id']
            ids   = []
            if  isinstance(recid, str):
                ids = [ObjectId(recid)]
            elif isinstance(recid, list):
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
            self.logger.error(traceback.format_exc())
            code = web_code('Exception')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        data['ctime'] = time.time() - time0
        return data

    @expose
    @checkargs(DAS_CACHE_INPUTS)
    def gridfs(self, *args, **kwargs):
        """
        HTTP GET request.
        Retrieve file from MongoDB GridFS for given file id.
        """
        time0 = time.time()
        msg = 'gridfs(%s, %s)' % (args, kwargs)
        self.logger.info(msg)
        data  = {'server_method':'request'}
        if  not kwargs.has_key('fid'):
            code = web_code('No file id')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        fid = kwargs.get('fid')
        data.update({'status':'requested', 'fid':fid}) 
        try:
            fds = self.gfs.get(ObjectId(fid))
            data['status'] = 'success'
            data['data']   = fds.read()
        except:
            self.logger.error(traceback.format_exc())
            code = web_code('Exception')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        data['ctime'] = time.time() - time0
        return data

    @expose
    @checkargs(DAS_CACHE_INPUTS)
    def status(self, *args, **kwargs):
        """
        HTTP GET request. Check status of the input query in DAS.
        """
        time0 = time.time()
        msg = 'status(%s, %s)' % (args, kwargs)
        self.logger.info(msg)
        data = {'server_method':'status'}
        if  kwargs.has_key('query'):
            query  = kwargs['query']
            self.logdb(query)
            query  = self.dascore.mongoparser.parse(query)
            status = self.dascore.get_status(query)
            if  not status:
                status = 'no data' 
            data.update({'status':status})
        else:
            code = web_code('Invalid query')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        data['ctime'] = time.time() - time0
        return data

    @expose
    @checkargs(DAS_CACHE_INPUTS)
    def nresults(self, *args, **kwargs):
        """
        HTTP GET request. Ask DAS for total number of records
        for provided query.
        """
        time0 = time.time()
        msg = 'nresults(%s, %s)' % (args, kwargs)
        self.logger.info(msg)
        data = {'server_method':'nresults'}
        if  kwargs.has_key('query'):
            query = kwargs['query']
            coll  = kwargs.get('collection', 'merge')
            self.logdb(query)
            query = self.dascore.mongoparser.parse(query)
            data.update({'status':'success'})
            res = self.dascore.in_raw_cache_nresults(query, coll)
            data.update({'status':'success', 'nresults':res})
        else:
            code = web_code('Invalid query')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        data['ctime'] = time.time() - time0
        return data

    @expose
    @checkargs(DAS_CACHE_INPUTS)
    def test(self, *args, **kwargs):
        """
        HTTP GET test method. Should be used by external tools for
        performance measurements. Return a dict with ctime.
        """
        time0 = time.time()
        data  = {'server_method':'request'}
        if  kwargs.has_key('query'):
            query = kwargs['query']
            idx   = getarg(kwargs, 'idx', 0)
            limit = getarg(kwargs, 'limit', 0)
            skey  = getarg(kwargs, 'skey', '')
            order = getarg(kwargs, 'order', 'asc')
            data.update({'status':'ok', 'idx':idx, 
                     'limit':limit, 'query':query,
                     'skey':skey, 'order':order})
            if  kwargs.has_key('mongo'):
                self.logdb(query)
        data['ctime'] = time.time() - time0
        return data

    @expose
    @checkargs(DAS_CACHE_INPUTS)
    def testmongo(self, *args, **kwargs):
        """
        HTTP GET testmongo method. Should be used by external tools for
        performance measurements. Return a dict with ctime.
        """
        time0 = time.time()
        data  = {'server_method':'testmongo'}
        if  kwargs.has_key('query'):
            query = kwargs.get('query', {})
            spec  = query
            if  isinstance(query, str) and query.find('=') != -1:
                key, val = query.split('=')
                if  val.find('*') != -1:
                    pat = re.compile("^%s" % val.replace('*', '.*'))
                    val = pat
                spec = {key: val}
            self.logdb(query)
            idx   = getarg(kwargs, 'idx', 0)
            limit = getarg(kwargs, 'limit', 1)
            skey  = getarg(kwargs, 'skey', '')
            order = getarg(kwargs, 'order', 'asc')
            collection = kwargs.get('collection', 'das.cache')
            dbname, colname = collection.split('.')
            coll = self.con[dbname][colname]
            res  = [r for r in coll.find(spec).skip(idx).limit(limit)]
            data.update({'status':'ok', 'idx':idx, 
                     'limit':limit, 'query':query, 'nresults': len(res),
                     'skey':skey, 'order':order, 'collection': collection})
        data['ctime'] = time.time() - time0
        return data

    @expose
    @checkargs(DAS_CACHE_INPUTS)
    def request(self, *args, **kwargs):
        """
        HTTP GET request.
        Retrieve results from DAS cache.
        """
        time0 = time.time()
        msg = 'request(%s, %s)' % (args, kwargs)
        self.logger.info(msg)
        data = {'server_method':'request'}
        if  kwargs.has_key('query'):
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
            res = self.dascore.result(query, idx, limit)
            if  isinstance(res, GeneratorType):
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
            code = web_code('Invalid query')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        data['ctime'] = time.time() - time0
        return data

    @expose
    @checkargs(DAS_CACHE_INPUTS)
    def create(self, *args, **kwargs):
        """
        HTTP POST request. 
        Requests the server to create a new resource
        using the data enclosed in the request body.
        Creates new entry in DAS cache for provided query.
        """
        time0 = time.time()
        msg = 'create(%s, %s)' % (args, kwargs)
        self.logger.info(msg)
        if  len(self.cachemgr.queue) > self.qlimit:
            msg = 'CacheMgr queue is full, current size %s. ' \
                % len(self.cachemgr.queue)
            msg += 'Please try in a few moments.'
            data.update({'status': 'fail', 'reason': msg})
            return data
            
        data = {'server_method':'create'}
        if  kwargs.has_key('query'):
            query  = kwargs['query']
            self.logdb(query)
            query  = self.dascore.mongoparser.parse(query)
            try: # this block implies usage of multiprocessing, see DASCacheMgr
                status = self.cachemgr.add(query)
                data.update({'status':status, 'query':query})
            except:
                self.logger.error(traceback.format_exc())
                code = web_code('Exception')
                raise HTTPError(500, 'DAS error, code=%s' % code)
        else:
            code = web_code('Invalid query')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        data['ctime'] = time.time() - time0
        return data

    @expose
    @checkargs(DAS_CACHE_INPUTS)
    def replace(self, *args, **kwargs):
        """
        HTTP PUT request.
        Requests the server to replace an existing
        resource with the one enclosed in the request body.
        Replace existing query in DAS cache.
        """
        time0 = time.time()
        msg = 'replace(%s, %s)' % (args, kwargs)
        self.logger.info(msg)
        data = {'server_method':'replace'}
        if  kwargs.has_key('query'):
            query = kwargs['query']
            self.logdb(query)
            query = self.dascore.mongoparser.parse(query)
            try:
                self.dascore.remove_from_cache(query)
            except:
                self.logger.error(traceback.format_exc())
                code = web_code('Exception')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            expire = getarg(kwargs, 'expire', 600)
            try:
                status = self.cachemgr.add(query, expire)
                data.update({'status':status, 'query':query, 'expire':expire})
            except:
                self.logger.error(traceback.format_exc())
                code = web_code('Excepion')
                raise HTTPError(500, 'DAS error, code=%s' % code)
        else:
            code = web_code('Invalid query')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        data['ctime'] = time.time() - time0
        return data

    @expose
    @checkargs(DAS_CACHE_INPUTS)
    def delete(self, *args, **kwargs):
        """
        HTTP DELETE request.
        Delete input query in DAS cache
        """
        time0 = time.time()
        msg = 'delete(%s, %s)' % (args, kwargs)
        self.logger.info(msg)
        data = {'server_method':'delete'}
        if  kwargs.has_key('query'):
            query = kwargs['query']
            self.logdb(query)
            query = self.dascore.mongoparser.parse(query)
            data.update({'status':'requested', 'query':query})
            try:
                self.dascore.remove_from_cache(query)
                data.update({'status':'success'})
            except:
                self.logger.error(traceback.format_exc())
                code = web_code('Exception')
                raise HTTPError(500, 'DAS error, code=%s' % code)
        else:
            code = web_code('Invalid query')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        data['ctime'] = time.time() - time0
        return data

    @expose
    @exposejson
    def rest(self, *args, **kwargs):
        """
        RESTful interface. We use args tuple as access method(s), e.g.
        args = ('method',) and kwargs to represent input parameters.
        """
        msg = 'rest(%s, %s)' % (args, kwargs)
        self.logger.info(msg)
        request = cherrypy.request.method
        if  request not in self.methods.keys():
            msg = "Unsupported request '%s'" % request
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
