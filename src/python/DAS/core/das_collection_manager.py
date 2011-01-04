#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS collection manager class handles access to appropriate DAS cache/merge
collection for various DAS queries.
"""
__author__ = "Valentin Kuznetsov"

# DAS modules
from DAS.core.das_son_manipulator import DAS_SONManipulator
from DAS.utils.das_db import db_connection

# monogo db modules
from pymongo import DESCENDING, ASCENDING

def make_dbname(query):
    """Construct dbname for given query"""
    keys = [k.split('.')[0] for k in query['spec'].keys()]
    if  query['fields']:
        keys += query['fields']
    return '_'.join(set(keys))

class DASCollectionManager(object):
    """DASCollectionManager class manages all DAS connections to MongoDB"""
    def __init__(self, config):
        self.dburi   = config['mongodb']['dburi']
        self.logger  = config['logger']
        self.verbose = config['verbose']
        self.conn    = db_connection(self.dburi)

        msg = "DASCollectionManager::__init__ %s" % self.dburi
        self.logger.info(msg)

        # mandatory indexes for cache/merge collections
        self.cache_index_list = \
                [('das.expire', ASCENDING), ('query.spec.key', ASCENDING),
                 ('das.qhash', DESCENDING), ('das.empty_record', ASCENDING),
                 ('query', DESCENDING), ('query.spec', DESCENDING)]
        self.merge_index_list = \
                [('das.expire', ASCENDING), ('das_id', ASCENDING),
                 ('das.empty_record', ASCENDING)]
        # to be filled at run time
        self.cachemgr = {'cache':{}, 'merge':{}} 
        
    def add_manipulator(self, mdb):
        """
        Add DAS-specific MongoDB SON manipulator to perform
        conversion of inserted data into DAS cache.
        """
        das_son_manipulator = DAS_SONManipulator()
        mdb.add_son_manipulator(das_son_manipulator)
        msg = "DASCollectionManager::add_manipulator %s" % das_son_manipulator
        self.logger.debug(msg)

    def collection(self, colname, query):
        """Return DAS collection pointer for given query and collection name"""
        dbname = make_dbname(query)
        if  self.cachemgr[colname].has_key(dbname):
            return self.cachemgr[colname][dbname]
        mongodb = self.conn[dbname]
        self.add_manipulator(mongodb)
        collection = mongodb[colname]
        index_list = getattr(self, '%s_index_list' % colname)
        for pair in index_list:
            collection.ensure_index([pair])
        self.cachemgr[colname][dbname] = collection
        return collection

    def cache(self, query):
        """Return DAS cache collection pointer for given query"""
        return self.collection('cache', query)

    def merge(self, query):
        """Return DAS merge collection pointer for given query"""
        return self.collection('merge', query)

    def cache_collections(self):
        """Return all pointers for DAS cache collections"""
        return self.cachemgr['cache']

    def merge_collections(self):
        """Return all pointers for DAS merge collections"""
        return self.cachemgr['merge']

