#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for CMS data-services
"""

import os
from cherrypy import engine, tree
from pymongo.connection import Connection
import unittest

from DAS.core.das_core import DASCore
from DAS.core.das_query import DASQuery
from DAS.utils.ddict import DotDict
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import PrintManager
from DAS.utils.utils import deepcopy
from DAS.core.das_parser import ql_manager
from DAS.core.das_mapping_db import DASMapping
from DAS.web.das_test_datasvc import DASTestDataService, Root
from DAS.services.map_reader import read_service_map

import DAS
DASPATH = '/'.join(DAS.__file__.split('/')[:-1])

class testCMSFakeDataServices(unittest.TestCase):
    """
    A test class for the DAS core module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug = 0

        # read DAS config and make fake Mapping DB entry
        collname      = 'test_collection'
        self.dasmerge = 'test_merge'
        self.dascache = 'test_cache'
        self.dasmr    = 'test_mapreduce'
        self.collname = collname
#        config        = deepcopy(das_readconfig())
        config        = das_readconfig()
        dburi         = config['mongodb']['dburi']
        self.dburi    = dburi
        logger        = PrintManager('TestCMSFakeDataServices', verbose=debug)
        self.base     = 'http://localhost:8080' # URL of DASTestDataService
        self.expire   = 100
        config['logger']    = logger
        config['loglevel']  = debug
        config['verbose']   = debug
        config['mappingdb'] = dict(dburi=dburi, dbname='mapping', collname=collname)
        config['analyticsdb'] = dict(dbname='analytics', collname=collname, history=100)
        config['dasdb'] = {'dbname': 'das',
                           'cachecollection': self.dascache,
                           'mrcollection': self.dasmr,
                           'mergecollection': self.dasmerge}
        config['keylearningdb'] = {'collname': collname, 'dbname': 'keylearning'}
        config['parserdb'] = {'collname': collname, 'dbname': 'parser', 
                                'enable': True, 'sizecap': 10000}
        config['services'] = ['dbs', 'phedex', 'sitedb', 'google_maps', 'ip']

        # mongo parser
        self.mongoparser = ql_manager(config)
        config['mongoparser'] = self.mongoparser

        # setup DAS mapper
        self.mgr = DASMapping(config)

        # create fresh DB
        self.clear_collections()
        self.mgr.delete_db_collection()
        self.mgr.create_db()

        # Add fake mapping records
        self.add_service('ip', 'ip.yml')
        self.add_service('google_maps', 'google_maps.yml')
        self.add_service('dbs', 'dbs.yml')
        self.add_service('phedex', 'phedex.yml')
        self.add_service('sitedb', 'sitedb.yml')

        # create DAS handler
        self.das = DASCore(config)

        # start TestDataService
        self.server = Root(config)
        self.server.start()

    def add_service(self, system, ymlfile):
        """
        Add Fake data service mapping records. We provide system name
        which match corresponding name in DASTestDataService and
        associated with this system YML map file.
        """
        fname  = os.path.join(DASPATH, 'services/maps/%s' % ymlfile)
        url    = self.base + '/%s' % system
        for record in read_service_map(fname):
            record['url'] = url
            record['system'] = system
            self.mgr.add(record)
        for record in read_service_map(fname, 'notations'):
            record['system'] = system
            self.mgr.add(record)

    def clear_collections(self):
        """clean-up test collections"""
        conn = Connection(host=self.dburi)
        for dbname in ['mapping', 'analytics', 'das', 'parser', 'keylearning']:
            db = conn[dbname]
            if  dbname != 'das':
                db.drop_collection(self.collname)
            else:
                db.drop_collection(self.dascache)
                db.drop_collection(self.dasmerge)
                db.drop_collection(self.dasmr)
            

    def tearDown(self):
        """Invoke after each test"""
        self.server.stop()
#        self.mgr.delete_db_collection()
#        self.clear_collections()

    def testDBSService(self):
        """test DASCore with test DBS service"""
        query  = "primary_dataset=abc" # invoke query to fill DAS cache
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.call(dquery)
        expect = "ok"
        self.assertEqual(expect, result)

        query  = "primary_dataset=abc" # invoke query to get results from DAS cache
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.get_from_cache(dquery, collection=self.dasmerge)
        result = [r for r in result]
        result = DotDict(result[0]).get('primary_dataset.name')
        expect = 'abc'
        self.assertEqual(expect, result)

    def testPhedexAndSiteDBServices(self):
        """test DASCore with test PhEDEx and SiteDB services"""
        query  = "site=T3_US_Cornell" # invoke query to fill DAS cache
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.call(dquery)
        expect = "ok"
        self.assertEqual(expect, result)

        query  = "site=T3_US_Cornell | grep site.name" # invoke query to get results from DAS cache
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.get_from_cache(dquery, collection=self.dasmerge)
        result = [r for r in result]
        expect = 'T3_US_Cornell'
        self.assertEqual(expect, DotDict(result[0]).get('site.name'))
        expect = ['_id', 'das_id', 'site', 'cache_id', 'das', 'qhash']
        expect.sort()
        rkeys = result[0].keys()
        rkeys.sort()
        self.assertEqual(expect, rkeys)

    def testAggregators(self):
        """test DASCore aggregators via zip service"""
        query  = "zip=1000"
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.call(dquery)
        expect = "ok"
        self.assertEqual(expect, result)

        query  = "zip=1000 | count(zip.place.city)"
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.get_from_cache(dquery, collection=self.dasmerge)
        result = [r for r in result]
        expect = {"function": "count", "result": {"value": 2}, 
                  "key": "zip.place.city", "_id":0}
        self.assertEqual(expect, result[0])

    def testIPService(self):
        """test DASCore with IP service"""
        query  = "ip=137.138.141.145"
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.call(dquery)
        expect = "ok"
        self.assertEqual(expect, result)

        query  = "ip=137.138.141.145 | grep ip.address"
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.get_from_cache(dquery, collection=self.dasmerge)
        result = [r for r in result]
        result = DotDict(result[0]).get('ip.address')
        expect = '137.138.141.145'
        self.assertEqual(expect, result)

    def testRecords(self):
        """test records DAS keyword with all services"""
        query  = "ip=137.138.141.145"
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.call(dquery)
        expect = "ok"
        self.assertEqual(expect, result)

        query  = "site=T3_US_Cornell"
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.call(dquery)
        expect = "ok"
        self.assertEqual(expect, result)

        query  = "records | grep ip.address"
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.get_from_cache(dquery, collection=self.dasmerge)
        result = [r for r in result]
        result = DotDict(result[0]).get('ip.address')
        expect = '137.138.141.145'
        self.assertEqual(expect, result)

        query  = "records | grep site.name"
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.get_from_cache(dquery, collection=self.dasmerge)
        result = [r for r in result]
        expect = 'T3_US_Cornell'
        self.assertEqual(expect, DotDict(result[0]).get('site.name'))

        query  = "records"
        dquery = DASQuery(query, mongoparser=self.mongoparser)
        result = self.das.get_from_cache(dquery, collection=self.dasmerge)
        res    = []
        for row in result:
            if  row.has_key('das') and row['das'].has_key('empty_record'):
                if  row['das'].get('empty_record'):
                    continue
            if  row.has_key('ip'):
                res.append(DotDict(row).get('ip.address'))
            if  row.has_key('site'):
                for item in row['site']:
                    if  item.has_key('name') and item['name'] not in res:
                        res.append(item['name'])
        res.sort()
        expect = ['137.138.141.145', 'T3_US_Cornell']
        self.assertEqual(expect, res)
#
# main
#
if __name__ == '__main__':
    unittest.main()



