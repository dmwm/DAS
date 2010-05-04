#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0613,W0622

"""
DAS admin service class.
"""

__revision__ = "$Id: das_admin.py,v 1.2 2010/04/02 20:01:55 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os
import json

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

def red(msg):
    """Put message in red box"""
    err = '<div class="box_red">%s</div>' % msg
    return err

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
    def index(self, **kwargs):
        """Serve default index.html web page"""
        msg = kwargs.get('msg', '')
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
                ddict=ddict, base=self.base, msg=msg)
        return self.page(info)

    @expose
    def records(self, database, collection=None, query={}, idx=0, limit=10, **kwargs):
        """Return records in given collection"""
        if  not collection:
            try:
                database, collection = database.split('.')
            except:
                msg = 'ERROR: no db collection is found in your request'
                return self.index(msg=red(msg))
        try:
            query = json.loads(query)
        except:
            msg = 'ERROR: fail to validate input query="%s" as JSON document'\
                % query
            return self.index(msg=red(msg))
        idx   = int(idx)
        limit = int(limit)
        recs  = self.conn[database][collection].find(query).\
                        skip(idx).limit(limit)
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
        nresults = self.conn[database][collection].find(query).count()
        idict = dict(nrows=nresults, idx=idx, 
                    limit=limit, results=page, url=url)
        page  = self.templatepage('das_pagination', **idict)
        return self.page(page)
