#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: request_manager.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: Persistent request manager for DAS web server
"""

import time
import DAS.utils.jsonwrapper as json
from DAS.utils.das_db import db_connection

class RequestManager(object):
    """
    RequestManager holds information about DAS requests. It stores
    pid/kwds pairs into MongoDB, where we assign MongoDB _id to be
    equal to pid (to avoid creating a new index). Since MongoDB
    does not support storage of 'key.attr' as a dict key, we use
    json dumps/loads method to serialize kwds.
    """
    def __init__(self, dburi, dbname='das', dbcoll='requests'):
        self.con = db_connection(dburi)
        self.col = self.con[dbname][dbcoll]

    def get(self, pid):
        """Get params for a given pid"""
        doc = self.col.find_one(dict(_id=pid))
        if  doc and isinstance(doc, dict):
            return json.loads(doc['kwds'])
        
    def add(self, pid, kwds):
        """Add new pid/kwds"""
        tstamp = time.strftime("%Y%m%d %H:%M:%S", time.localtime())
        doc = dict(_id=pid, kwds=json.dumps(kwds),
                ts=time.time(), timestamp=tstamp)
        self.col.insert(doc)
        
    def remove(self, pid):
        """Remove given pid"""
        self.col.remove(dict(_id=pid))
        
    def items(self):
        """Return list of current requests"""
        for row in self.col.find():
            row['_id'] = str(row['_id'])
            yield row

    def size(self):
        """Return size of the request cache"""
        return self.col.count()
