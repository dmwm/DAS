#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0702,E1101,R0903
"""
Base classes to serve CMSSW config service.
"""

__revision__ = "$Id: das_server.py,v 1.6 2010/03/19 02:22:21 valya Exp $"
__version__ = "$Revision: 1.6 $"
__author__ = "Valentin Kuznetsov"

# system modules
import time

from operator import itemgetter
from heapq import nlargest

# mongosearch/mongoengine
from DAS.utils import mongosearch
import mongoengine

class CMSSWConfig(mongoengine.Document):
    """
    CMSSW configuration object.
    """
    name      = mongoengine.fields.StringField()
    content   = mongoengine.fields.StringField()
    system    = mongoengine.fields.StringField()
    subsystem = mongoengine.fields.StringField()
    hash      = mongoengine.fields.StringField()

class MongoQuery(object):
    """
    Class which query configdb collections.
    """
    def __init__(self, release, logger=None):
        self.release = release
        mongoengine.connect('configdb')
        config_obj  = CMSSWConfig
        config_obj._meta['collection'] = release
        self.index  = mongosearch.SearchIndex(config_obj, use_term_index=False)
        self.logger = logger

    def query(self, query):
        """
        Execute query over results in collection.
        """
        # Query the collection
        time0 = time.time()
        results = self.index.search(query)
        top_matches = nlargest(10, results.items(), itemgetter(1))
        time_taken = time.time() - time0
        if  self.logger:
            msg = 'MongoQuery: querying took %s seconds' % time_taken
            self.logger.debug(msg)

        # Write the results to results.htm as HTML
        for doc_id, score in top_matches:
            doc = CMSSWConfig.objects(id=doc_id).first()
            yield doc_id, score, doc.name

        if  self.logger:
            msg = 'MongoQuery: processed %s items' % CMSSWConfig.objects.count()
            self.logger.debug(msg)

