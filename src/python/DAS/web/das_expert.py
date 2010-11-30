#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0613,W0622,W0702

"""
DAS expert service class.
"""

__revision__ = "$Id: das_expert.py,v 1.9 2010/05/04 21:12:19 valya Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "Valentin Kuznetsov"

# system modules
import json
import thread
import urllib
import traceback
from pprint import pformat

# cherrypy modules
from cherrypy import expose, response, request, HTTPRedirect, HTTPError

# DAS modules
from DAS.utils.das_config import das_readconfig
from DAS.utils.utils import genkey
from DAS.utils.das_db import db_connection, connection_monitor
from DAS.utils.regex import web_arg_pattern
from DAS.core.das_core import DASCore
from DAS.core.das_mongocache import convert2pattern, encode_mongo_query
from DAS.web.das_webmanager import DASWebManager
from DAS.web.utils import json2html, ajax_response, checkargs, quote
from DAS.web.das_codes import web_code

DAS_EXPERT_INPUTS = ['idx', 'limit', 'collection', 'database', 'query',
             'dasquery', 'dbcoll', 'msg']

def error(msg):
    """Put message in red box"""
    err = '<div class="box_red">%s</div>' % msg
    return err

def check_dn(func):
    """CherryPy expose decorator which check user DN's"""
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        redirect = True
        headers = request.headers
        dasconfig = das_readconfig()
        dburi  = dasconfig['mongodb']['dburi']
        conn = db_connection(dburi)
        database = conn['admin']
        coll = database['dns']
        dn = headers.get('Ssl-Client-S-Dn', None)
        redirect = False
        if  dn:
            if  coll.find_one({'dn': dn}):
                redirect = False
        # check headers for passed DN, look-it up in MongoDB:dn collection
        # and make a decision to pass or refuse the request
        if  redirect:
            raise HTTPRedirect('/das/redirect')
        data = func (self, *args, **kwds)
        return data
    return wrapper

class DASExpertService(DASWebManager):
    """
    DAS expert service class.
    """
    def __init__(self, config):
        DASWebManager.__init__(self, config)
        self.base   = '/das'
        das_config  = das_readconfig()
        self.dburi  = das_config['mongodb']['dburi']
        self.dasconfig = das_config
        self.init()
        # Monitor thread which performs auto-reconnection
        thread.start_new_thread(connection_monitor, (self.dburi, self.init, 5))

    def init(self):
        """Connect to DASCore"""
        try:
            self.conn   = db_connection(self.dburi)
            self.das    = DASCore(debug=0, nores=True)
        except:
            self.conn = None
            self.das  = None

    @expose
    @checkargs(DAS_EXPERT_INPUTS)
    @check_dn
    def index(self, **kwargs):
        """Serve default index.html web page"""
        msg = kwargs.get('msg', '')
        databases = self.conn.database_names()
        server_info = dict(dburi=self.dburi)
        server_info.update(self.conn.server_info())
        ddict = {}
        for database in databases:
            collections = self.conn[database].collection_names()
            coll = self.conn[database]
            info_dict = {}
            for cname in collections:
                info_dict[cname] = (coll[cname].count(), 
                                    coll.validate_collection(cname),
                                    coll[cname].index_information())
            ddict[database] = info_dict
        info = self.templatepage('das_expert', mongo_info = server_info,
                ddict=ddict, base=self.base, msg=msg, 
                dasconfig=pformat(self.dasconfig))
        return self.page(info)

    @expose
    @checkargs(DAS_EXPERT_INPUTS)
    @check_dn
    def records(self, database, collection=None, query=None, idx=0, limit=10, 
                **kwargs):
        """Return records in given collection"""
        if  not collection:
            try:
                database, collection = database.split('.')
            except:
                msg = 'ERROR: no db collection is found in your request'
                return self.index(msg=error(msg))
        if  query:
            query = query.replace('\n', '')
            try:
                query = json.loads(query)
            except:
                traceback.print_exc()
                msg = 'ERROR: fail to validate input query="%s" as JSON document'\
                    % query
                return self.index(msg=error(msg))
        else:
            query = {}
        idx   = int(idx)
        limit = int(limit)
        recs  = self.conn[database][collection].find(query).\
                        skip(idx).limit(limit)
        pad   = ''
        page  = ''
        style = 'white'
        for row in recs:
            rec_id   = row['_id']
            page    += '<div class="%s"><hr class="line" />' % style
            jsoncode = {'jsoncode': json2html(row, pad)}
            jsonhtml = self.templatepage('das_json', **jsoncode)
            jsondict = dict(data=jsonhtml, id=rec_id, rec_id=rec_id)
            page += self.templatepage('das_row', **jsondict)
            page += '</div>'
        iquery = query
        if  not iquery:
            iquery = {}
        url = '%s/expert/records?database=%s&collection=%s&query=%s' \
        % (quote(self.base), quote(database), quote(collection), quote(iquery))
        nresults = self.conn[database][collection].find(query).count()
        idict = dict(nrows=nresults, idx=idx, 
                    limit=limit, url=url)
        content = page
        page  = self.templatepage('das_pagination', **idict)
        page += content
        return self.page(page)

    @expose
    @checkargs(DAS_EXPERT_INPUTS)
    @check_dn
    def query_info(self, dasquery, **kwargs):
        """
        Provide information about DAS query, similary to --hash option
        in DAS CLI interface
        """
        query = dasquery
        response.headers['Content-Type'] = 'text/xml'
        try:
            mongo_query = self.das.mongoparser.parse(query, add_to_analytics=False)
            service_map = self.das.mongoparser.service_apis_map(mongo_query)
            enc_query   = encode_mongo_query(mongo_query)
            loose_query_pat, loose_query = convert2pattern(mongo_query)
            idict = dict(query=query,
                    mongo_query=mongo_query,
                    loose_query=loose_query,
                    enc_query=enc_query,
                    enc_query_hash=genkey(enc_query),
                    service_map=pformat(service_map))
            page = self.templatepage('das_query_info', **idict)
        except:
            page = "<pre>" + traceback.format_exc() + "</pre>"
        page = ajax_response(page)
        return page

    @expose
    @checkargs(DAS_EXPERT_INPUTS)
    @check_dn
    def clean(self, dbcoll, **kwargs):
        """Clean DAS cache for provided dbcoll parameter"""
        response.headers['Content-Type'] = 'text/xml'
        try:
            db, coll = dbcoll.split('.')
            nrec1 = self.conn[db][coll].count()
            self.conn[db][coll].remove({})
            nrec2 = self.conn[db][coll].count()
            status = 'Clean %s.%s, # records: %s -> %s'\
                % (db, coll, nrec1, nrec2)
        except:
            status = 'ERROR: %s' % traceback.format_exc()
        page = ajax_response(status)
        return page

#    def mapping(self, **kwargs):
#        mappingdb = self.conn['mapping']['db']
#        return "mapping page"

#    @expose
#    def analytics(self, **kwargs):
#        return "analytics page"

