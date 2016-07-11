#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
CMSSWConfigs service
"""
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import time
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator
from DAS.services.cmsswconfigs.base import MongoQuery
from DAS.utils.das_db import db_connection

from bson.objectid import ObjectId

class CMSSWConfigsService(DASAbstractService):
    """
    Helper class to provide CMSSWConfigs service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'cmsswconfigs', config)
        self.headers = {'Accept': 'text/json;application/json'}
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

        # specify access to DB
        dburi = config.get('dburi')
        self.conn = db_connection(dburi)
        database  = self.conn['configdb']
        self.managers = {}
        for release in database.collection_names():
            if  release.find('index') == -1:
                self.managers[release] = MongoQuery(release)
        self.releases = list(self.managers.keys())

    def api(self, dasquery):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        api     = list(self.map.keys())[0] # we have only one key
        url     = self.map[api]['url']
        expire  = self.map[api]['expire']
        args    = dict(self.map[api]['params'])

        search  = dasquery.mongo_query['spec'].get('search', None)
        release = dasquery.mongo_query['spec'].get('release.name', None)
        msg = "CMSSWConfigsService::api(%s), release=%s, search=%s" \
                % (dasquery, release, search)
        self.logger.info(msg)
        time0   = time.time()
        genrows = self.worker(release, search)
        ctime   = time.time() - time0
        self.write_to_cache(dasquery, expire, url, api, args, genrows, ctime)

    def worker(self, release, query):
        """
        Helper function which perform all the work to find release documents for
        given query and release.
        """
        if  not query:
            error = "CMSSWConfigsService::worker, Query is not provided"
            raise Exception(error)
        if  not release:
            error = "CMSSWConfigsService::worker, CMSSW release is not provided"
            raise Exception(error)
        if  release.lower() == 'all':
            releases = list(self.managers.keys())
        else:
            releases = [release]
        doc = ''
        for release in releases:
            mgr = self.managers.get(release, MongoQuery(release, self.logger))
            for res in mgr.query(query):
                doc_id = res[0]
                score  = res[1]
                name   = res[2]
                coll   = self.conn['configdb'][release]
                doc    = coll.find_one({'_id':ObjectId(doc_id)})
                del doc['_id'] # don't ship ObjectId
                yield dict(config=doc, search=query, release=dict(name=release))

