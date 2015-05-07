#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS Parser DB manager
"""

__author__ = "Gordon Ball"

from DAS.utils.utils import genkey
from DAS.utils.das_db import db_connection, create_indexes, find_one
from DAS.utils.query_utils import encode_mongo_query, decode_mongo_query
from DAS.utils.logger import PrintManager

from pymongo import DESCENDING

PARSERCACHE_NOTFOUND = 5
PARSERCACHE_INVALID = 17
PARSERCACHE_VALID = 23

class DASParserDB(object):
    """
    Caching layer for the PLY parser.
    """
    def __init__(self, config):
        self.verbose  = config['verbose']
        self.logger   = PrintManager('DASParserDB', self.verbose)
        self.dburi    = config['mongodb']['dburi']
        self.dbname   = config['parserdb']['dbname']
        self.sizecap  = config['parserdb'].get('sizecap', 5*1024*1024)
        self.colname  = config['parserdb']['collname']
        msg = "DASParserCache::__init__ %s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)
        self.create_db()

    def create_db(self):
        """
        Create db collection
        """
        conn = db_connection(self.dburi)
        dbn  = conn[self.dbname]
        if  self.colname not in dbn.collection_names():
            dbn.create_collection(self.colname, capped=True, size=self.sizecap)
        col = dbn[self.colname]
        index_list = [('qhash', DESCENDING)]
        create_indexes(col, index_list)

    @property
    def col(self):
        "Collection object to MongoDB"
        conn = db_connection(self.dburi)
        dbn  = conn[self.dbname]
        col  = dbn[self.colname]
        return col

    def lookup_query(self, rawtext):
        """
        Check the parser cache for a given rawtext query.
        Search is done with the qhash of this string.
        Returns a tuple (status, value) for the cases
        (PARSERCACHE_VALID, mongo_query) - valid query found
        (PARSERCACHE_INVALID, error) - error message for invalid query
        (PARSERCACHE_NOTFOUND, None) - not in the cache
        """
        result = find_one(self.col, {'qhash':genkey(rawtext)}, \
                        fields=['query', 'error'])

        if result and result['query']:
            if self.verbose:
                self.logger.debug("DASParserCache: found valid %s->%s" %\
                                  (rawtext, result['query']))
            query = decode_mongo_query(result['query'])
            return (PARSERCACHE_VALID, query)
        elif result and result['error']:
            if self.verbose:
                self.logger.debug("DASParserCache: found invalid %s->%s" %\
                                  (rawtext, result['error']))
            return (PARSERCACHE_INVALID, result['error'])
        else:
            if self.verbose:
                self.logger.debug("DASParserCache: not found %s" %\
                                  (rawtext))
            return (PARSERCACHE_NOTFOUND, None)

    def insert_valid_query(self, rawtext, query):
        "Insert a query that was successfully transformed"	
        self._insert_query(rawtext, query, None)

    def insert_invalid_query(self, rawtext, error):
        "Insert the error message for an invalid query"
        self._insert_query(rawtext, None, error)

    def _insert_query(self, rawtext, query, error):
        """Internal method to insert a query"""
        if  self.verbose:
            self.logger.debug("DASParserCache: insert %s->%s/%s" %\
	                          (rawtext, query, error))
        # since MongoDB does not support insertion of $ sign in queries
        # we need to encode inserted query
        if  query:
            encquery = encode_mongo_query(query)
        else:
            encquery = ""
        self.col.insert({'raw':rawtext, 'qhash':genkey(rawtext),
                         'query':encquery, 'error':str(error)})
