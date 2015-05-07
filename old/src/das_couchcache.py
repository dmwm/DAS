#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS couchdb cache. Communitate with DAS core and couchdb server(s)
"""
from __future__ import print_function

__revision__ = "$Id: das_couchcache.py,v 1.1 2010/01/19 19:02:57 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import types
import traceback
import DAS.utils.jsonwrapper as json

from WMCore.Database.CMSCouch import CouchServer

# DAS modules
from DAS.utils.utils import genkey, timestamp, results2couch
from DAS.core.cache import Cache
from DAS.utils.logger import DummyLogger
from DAS.web.utils import urllib2_request, httplib_request

class DASCouchcache(Cache):
    """
    Base DAS couchdb cache class based on couchdb, see
    http://couchdb.apache.org/, The client API based on 
    http://wiki.apache.org/couchdb/Getting_started_with_Python
    in particular we use couchdb-python library
    http://couchdb-python.googlecode.com/
    """
    def __init__(self, config):
        Cache.__init__(self, config)
        uri = config['couch_servers'] # in a future I may have several
        self.logger = config['logger']
        if  not self.logger:
            self.logger = DummyLogger()
        self.limit  = config['couch_lifetime']
        self.uri    = uri.replace('http://', '')
        self.server = CouchServer(self.uri)
        self.dbname = "das"
        self.cdb    = None # cached couch DB handler
        self.future = 9999999999 # unreachable timestamp
        self.logger.info('Init couchcache %s' % self.uri)

        self.views = { 
            'query': {'map': """
function(doc) {
    if(doc.hash) {
        emit([doc.hash, doc.expire], doc.results);
    }
}"""
            },
#            'incache': {'map': """
#function(doc) {
#    if(doc.hash) {
#        emit([doc.hash, doc.expire], null);
#    }
#}"""
#            },
        }

        self.adminviews = { 

            'system' : {'map': """
function(doc) {
    if(doc.results.system) {
        emit(doc.results.system, doc);
    }
}"""
            },

            'cleaner' : {'map': """
function(doc) {
    if(doc.expire) {
        emit(doc.expire, doc);
    }
}"""
            },

            'timer' : {'map': """
function(doc) {
    if(doc.timestamp) {
        emit(doc.timestamp, doc);
    }
}"""
            },
            'all_queries' : {'map': """
function(doc) {
    if (doc.query) {
        emit(doc.query, null);
    }
}""",
                        'reduce' : """
function(keys, values) {
   return null;
}"""
            },

        }

    def connect(self, url):
        """
        Connect to different Couch DB URL
        """
        self.uri    = url.replace('http://', '')
        del self.server
        self.server = CouchServer(self.uri)

    def create_view(self, dbname, design, view_dict):
        """
        Create new view in couch db.
        """
        cdb  = self.couchdb(dbname)
        # check provided view_dict that it has all keys
        for view, definition in view_dict.items():
            if  type(definition) is not dict:
                msg = 'View "%s" has improper definition' % view
                raise Exception(msg)
            if  'map' not in definition:
                msg = 'View "%s" does not have map'
                raise Exception(msg)
        view = dict(_id='_design/%s' % design, language='javascript', 
                        doctype='view', views=view_dict)
        cdb.commit(view)

    def delete_view(self, dbname, design, view_name):
        """
        Delete given view in couch db
        """
        print("Delete view", dbname, design, view_name)

    def dbinfo(self, dbname='das'):
        """
        Provide couch db info
        """
        cdb = self.couchdb(dbname)
        if  cdb:
            self.logger.info(cdb.info())
        else:
            self.logger.warning("No '%s' found in couch db" % dbname)
        if  not cdb:
            return "Unable to connect to %s" % dbname
        return cdb.info()

    def delete_cache(self, dbname=None, system=None):
        """
        Delete either couchh db (dbname) or particular docs
        for provided system, e.g. all sitedb docs.
        """
        cdb = self.couchdb(dbname)
        if  cdb:
            if  system:
                key = '"%s"' % system
                options = {'key' : key}
                results = self.get_view('dasadmin', 'system', options)
                for doc in results:
                    cdb.queuedelete(doc)
                cdb.commit()
            else:
                self.server.deleteDatabase(dbname)
        return

    def couchdb(self, dbname):
        """
        look up db in couch db server, if found give it back to user
        """
        if  self.cdb:
            return self.cdb
        couch_db_list = []
        try:
            couch_db_list = self.server.listDatabases()
        except:
            return None
        if  dbname not in couch_db_list:
            self.logger.info("DASCouchcache::couchdb, create db %s" % dbname)
            cdb = self.server.createDatabase(dbname)
            self.create_view(self.dbname, 'dasviews', self.views)
            self.create_view(self.dbname, 'dasadmin', self.adminviews)
        else:
            self.logger.info("DASCouchcache::couchdb, connect db %s" % dbname)
            cdb = self.server.connectDatabase(dbname)
        self.cdb = cdb
        return cdb

    def incache(self, query):
        """
        Check if query exists in cache
        """
        dbname = self.dbname
        cdb = self.couchdb(dbname)
        if  not cdb:
            return
        key  = genkey(query)
        #TODO:check how to query 1 result, I copied the way from get_from_cache
        skey = ["%s" % key, timestamp()]
        ekey = ["%s" % key, self.future]
        options = {'startkey': skey, 'endkey': ekey}
#        results = cdb.loadView('dasviews', 'incache', options)
        results = cdb.loadView('dasviews', 'query', options)
        try:
            res = len(results['rows'])
        except:
            traceback.print_exc()
            return
        if  res:
            return True
        return False

    def get_from_cache(self, query, idx=0, limit=0, skey=None, order='asc'):
        """
        Retreieve results from cache, otherwise return null.
        """
        id      = 0
        idx     = int(idx)
        limit   = long(limit)
        stop    = idx + limit # get upper bound for range
        dbname  = self.dbname
        cdb     = self.couchdb(dbname)
        if  not cdb:
            return
        key     = genkey(query)

        skey    = ["%s" % key, timestamp()]
        ekey    = ["%s" % key, self.future]
        options = {'startkey': skey, 'endkey': ekey}
        results = cdb.loadView('dasviews', 'query', options)
        try:
            res = [row['value'] for row in results['rows']]
            for row in results['rows']:
                row['id'] = id
                if  limit:
                    if  id >= idx and id <= stop:
                        yield row
                else:
                    yield row
                id += 1
        except:
            traceback.print_exc()
            return
        if  res:
            self.logger.info("DASCouchcache::get_from_cache for %s" % query)
#        if  len(res) == 1:
#            return res[0]
#        return res

    def update_cache(self, query, results, expire):
        """
        Insert results into cache. We use bulk insert operation, 
        db.update over entire set, rather looping for every single 
        row and use db.create. The speed up is factor of 10
        """
        if  not expire:
            raise Exception('Expire parameter is null')
        self.logger.info("DASCouchcache::update_cache for %s" % query)
        if  not results:
            return
        dbname = self.dbname
        viewlist = []
        for key in self.views.keys():
            viewlist.append("/%s/_design/dasviews/_view/%s" % (dbname, key))
        cdb = self.couchdb(dbname)
        self.clean_cache()
        if  not cdb:
            if  type(results) is list or \
                type(results) is types.GeneratorType:
                for row in results:
                    yield row
            else:
                yield results
            return
        if  type(results) is list or \
            type(results) is types.GeneratorType:
            for row in results:
                res = results2couch(query, row, expire)
                cdb.queue(res, viewlist=viewlist)
                yield row
        else:
            res = results2couch(query, results, expire)
            yield results
            cdb.queue(res, viewlist=viewlist)
        cdb.commit(viewlist=viewlist)

    def remove_from_cache(self, query):
        """
        Delete query from cache
        """
        self.logger.debug('DASCouchcache::remove_from_cache(%s)' \
                % (query, ))
        return

    def get_view(self, design, view, options={}):
        """
        Retreieve results from cache based on provided Couchcache view
        """
        dbname = self.dbname
        cdb = self.couchdb(dbname)
        if  not cdb:
            return
        results = cdb.loadView(design, view, options)
        res = [row['value'] for row in results['rows']]
        if  len(res) == 1:
            return res[0]
        return res

    def list_views(self):
        """
        Return a list of Couchcache views
        """

    def clean_cache(self):
        """
        Clean expired docs in couch db.
        """
        dbname = self.dbname
        cdb = self.couchdb(dbname)
        if  not cdb:
            return
        skey = 0
        ekey = timestamp()
        options = {'startkey': skey, 'endkey': ekey}
        results = cdb.loadView('dasadmin', 'cleaner', options)

        ndocs = 0
        for doc in results['rows']:
            cdb.queueDelete(doc['value'])
            ndocs += 1

        self.logger.info("DASCouchcache::clean_couch, will remove %s doc's" \
            % ndocs )
        if  not ndocs:
            return
        cdb.commit()  # bulk delete
        cdb.compact() # remove them permanently
        
    def list_between(self, time_begin, time_end):
        """
        Retreieve results from cache for time range
        """
        dbname = self.dbname
        cdb = self.couchdb(dbname)
        if  not cdb:
            return
        skey = time_begin
        ekey = time_end
        options = {'startkey': skey, 'endkey': ekey}
        results = cdb.loadView('dasadmin', 'timer', options)
        try:
            res = [row['value'] for row in results['rows']]
        except:
            traceback.print_exc()
            return
        if  len(res) == 1:
            return res[0]
        return res

    def list_queries_in(self, system, idx=0, limit=0):
        """
        Retrieve results from cache for provided system, e.g. sitedb
        """
        idx = int(idx)
        limit = long(limit)
        dbname = self.dbname
        cdb = self.couchdb(dbname)
        if  not cdb:
            return
        skey = system
        ekey = system
        options = {'startkey': skey, 'endkey': ekey}
        results = cdb.loadView('dasadmin', 'system', options)
        try:
            res = [row['value'] for row in results['rows']]
        except:
            traceback.print_exc()
            return
        if  len(res) == 1:
            return res[0]
        return res

    def get_all_views(self, dbname=None):
        """
        Method to get all degined views in couch db. The couch db doesn't have
        a clear way to extract view documents. Instead we need to ask for
        _all_docs and provide proper start/end-keys. Once we retrieve
        _design docs, we loop over them and get the doc of particular view, e.g
        http://localhost:5984/das/_design/dasviews
        """
        if  not dbname:
            dbname = self.dbname
        qqq  = 'startkey=%22_design%2F%22&endkey=%22_design0%22'
        host = 'http://' + self.uri
        path = '/%s/_all_docs?%s' % (dbname, qqq)
        kwds = {}
        req  = 'GET'
        debug   = 0
        results = httplib_request(host, path, kwds, req, debug)
        designdocs = json.loads(results)
        results    = {}
        for item in designdocs['rows']:
            doc   = item['key']
#            print "design:", doc
            path  = '/%s/%s' % (dbname, doc)
            res   = httplib_request(host, path, kwds, req, debug)
            rdict = json.loads(res)
            views = []
            for view_name, view_dict in rdict['views'].items():
#                print "  view:", view_name
#                print "   map:", view_dict['map']
                if  'reduce' in view_dict:
#                    print "reduce:", view_dict['reduce']
                    rdef = view_dict['reduce']
                    defrow = dict(map=view_dict['map'], 
                                        reduce=view_dict['reduce'])
                else:
                    defrow = dict(map=view_dict['map'])
                row = {'%s' % view_name : defrow}
                views.append(row)
            results[doc] = views
        return results

    def get_all_queries(self, idx=0, limit=0):
        """
        Retreieve DAS queries from the cache.
        """
        idx = int(idx)
        limit = long(limit)
        dbname = self.dbname
        cdb = self.couchdb(dbname)
        if  not cdb:
            return

        options = {}
        results = cdb.loadView('dasadmin', 'all_queries', options)
        try:
            res = [row['value'] for row in results['rows']]
        except:
            traceback.print_exc()
            return
        if  len(res) == 1:
            return res[0]
        return res

#    def create_ft_index(self, db, name):
#        view = client.PermanentView(self.uri, name)
#        key = '_design/%s' % name
#        db[key] = { 'language': 'javascript',
#                    'ft_index': 
#"""function(doc) { 
#if(doc.body) index(doc.body); 
#if(doc.foo) property("foo", doc.foo);
#}"""
#                  }
