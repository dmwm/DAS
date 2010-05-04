#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0613,W0622

"""
DAS admin service class.
"""

__revision__ = "$Id: das_admin.py,v 1.1 2010/04/01 19:56:04 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os

# cherrypy modules
from cherrypy import expose

# monogo db modules
from pymongo.connection import Connection
from pymongo.objectid import ObjectId
from pymongo import DESCENDING, ASCENDING

# DAS modules
from DAS.utils.das_config import das_readconfig
from DAS.web.das_webmanager import DASWebManager
from DAS.web.utils import json2html

@expose
def error(msg):
    """Return error message"""
    return msg

class DASAdminService(DASWebManager):
    """
    DAS admin service class.
    """
    def __init__(self, config={}):
        DASWebManager.__init__(self, config)
        self.base   = '/das'
        das_config  = das_readconfig()
        self.dbhost = das_config.get('mongocache_dbhost')
        self.dbport = das_config.get('mongocache_dbport')
        self.conn = Connection(self.dbhost, self.dbport)

    @expose
    def index(self, *args, **kwargs):
        """Serve default index.html web page"""
        databases = self.conn.database_names()
        server_info = dict(host=self.dbhost, port=self.dbport)
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
        info = self.templatepage('das_admin', mongo_info = server_info,
                ddict=ddict, base=self.base)
        return self.page(info)

    @expose
    def records(self, database, collection, idx=0, limit=10):
        """Return records in given collection"""
        idx   = int(idx)
        limit = int(limit)
        recs  = self.conn[database][collection].find().skip(idx).limit(limit)
        pad   = ''
        page  = ''
        style = 'white'
        for row in recs:
            id    = row['_id']
            page += '<div class="%s"><hr class="line" />' % style
            jsoncode = {'jsoncode': json2html(row, pad)}
            jsonhtml = self.templatepage('das_json', **jsoncode)
            jsondict = dict(data=jsonhtml, id=id, rec_id=id)
            page += self.templatepage('das_row', **jsondict)
            page += '</div>'
        url = '%s/admin/records?database=%s&collection=%s' \
                % (self.base, database, collection)
        nresults = self.conn[database][collection].count()
        idict = dict(nrows=nresults, idx=idx, 
                    limit=limit, results=page, url=url)
        page  = self.templatepage('das_pagination', **idict)
        return self.page(page)
