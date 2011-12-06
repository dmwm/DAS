#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS Parser DB manager
"""

__author__ = "Gordon Ball"

from DAS.utils.utils import genkey
from DAS.utils.das_db import db_connection
from DAS.core.das_mongocache import encode_mongo_query, decode_mongo_query
from DAS.utils.logger import PrintManager

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
        self.sizecap  = config['parserdb']['sizecap']
        self.colname  = config['parserdb']['collname']
        
        msg = "DASParserCache::__init__ %s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)
        
        self.col = None
        self.create_db()

    def create_db(self):
        """
        Create db collection
        """
        conn = db_connection(self.dburi)
        dbn  = conn[self.dbname]
        if self.colname not in dbn.collection_names() and self.sizecap > 0:
            dbn.create_collection(self.colname, capped=True, size=self.sizecap)
        self.col = dbn[self.colname]

    def lookup_query(self, rawtext):
        """
        Check the parser cache for a given rawtext query.
        Search is done with the hash of this string.
        
        Returns a tuple (status, value) for the cases
        (PARSERCACHE_VALID, mongo_query) - valid query found
        (PARSERCACHE_INVALID, error) - error message for invalid query
        (PARSERCACHE_NOTFOUND, None) - not in the cache
        """
        result = self.col.find_one({'hash':genkey(rawtext)},
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
        self.col.insert({'raw':rawtext, 'hash':genkey(rawtext),
                         'query':encquery, 'error':str(error)})
        self.col.ensure_index('hash', unique=True, drop_dups=True)
