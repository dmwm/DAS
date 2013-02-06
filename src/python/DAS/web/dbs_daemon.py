#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=R0913,W0703,W0702
"""
File: dbs_daemon.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: DBS daemon, which update DAS cache with DBS datasets
"""

# system modules
import re
import time
import thread
import urllib
import urllib2
import itertools

# MongoDB modules
from pymongo.errors import InvalidOperation
from pymongo import ASCENDING

# DAS modules
import DAS.utils.jsonwrapper as json
from DAS.utils.utils import qlxml_parser, dastimestamp, print_exc
from DAS.utils.das_db import db_connection, is_db_alive, create_indexes
from DAS.utils.das_db import db_monitor
from DAS.utils.utils import get_key_cert, genkey
from DAS.utils.thread import start_new_thread
from DAS.utils.url_utils import HTTPSClientAuthHandler

def dbs_instance(dbsurl):
    """Parse dbs instance from provided DBS url"""
    if  dbsurl[-1] == '/':
        dbsurl = dbsurl[:-1]
    if  dbsurl.find('DBSServlet') != -1:
        # http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet
        dbsinst = dbsurl.split('/')[-3]
    elif dbsurl.find('DBSReader') != -1:
        # http://cmsweb.cern.ch/dbs/prod/global/DBSReader
        dbsinst = dbsurl.split('/')[-3]
    else:
        msg = 'Unable to parse dbs instance from provided url %s' % dbsurl
        raise Exception(msg)
    return dbsinst

class DBSDaemon(object):
    """
    DBSDaemon fetch list of known datasets from DBS2/DBS3
    and store them in separate collection to be used by
    DAS autocomplete web interface.
    """
    def __init__(self, dbs_url, dburi, config=None):
        if  not config:
            config = {}
        self.dburi      = dburi
        self.dbcoll     = dbs_instance(dbs_url)
        self.dbs_url    = dbs_url
        self.dbname     = config.get('dbname', 'dbs')
        self.cache_size = config.get('cache_size', 1000)
        self.expire     = config.get('expire', 3600)
        self.write_hash = config.get('write_hash', False)
        self.col = None # to be defined in self.init
        self.init()
        # Monitoring thread which performs auto-reconnection to MongoDB
        thname = 'dbs_monitor:%s' % dbs_url
        start_new_thread(thname, db_monitor, (dburi, self.init))

    def init(self):
        """
        Init db connection and check that it is alive
        """
        try:
            conn = db_connection(self.dburi)
            self.col = conn[self.dbname][self.dbcoll]
            indexes = [('dataset', ASCENDING), ('ts', ASCENDING)]
            create_indexes(self.col, indexes)
        except Exception as _exp:
            self.col = None
        if  not is_db_alive(self.dburi):
            self.col = None

    def update(self):
        """
        Update DBS collection with a fresh copy of datasets
        """
        if  self.col:
            time0 = time.time()
            gen = self.datasets()
            if  not self.col.count():
                try: # perform bulk insert operation
                    while True:
                        if  not self.col.insert(\
                                itertools.islice(gen, self.cache_size)):
                            break
                except InvalidOperation:
                    pass
            else: # we already have records, update their ts
                for row in gen:
                    spec = dict(dataset=row['dataset'])
                    self.col.update(spec, {'$set':{'ts':time0}}, upsert=True)
            # remove records with old ts
            self.col.remove({'ts':{'$lt':time0-self.expire}})
            print "%s DBSDaemon updated %s collection in %s sec, nrec=%s" \
            % (dastimestamp(), self.dbcoll, time.time()-time0, self.col.count())

    def find(self, pattern, idx=0, limit=10):
        """
        Find datasets for a given pattern. The idx/limit parameters
        control number of retrieved records (aka pagination). The
        limit=-1 means no pagination (get all records).
        """
        if  self.col:
            try:
                if  len(pattern) > 0 and pattern[0] == '/':
                    pattern = '^%s' % pattern
                if  pattern.find('*') != -1:
                    pattern = pattern.replace('*', '.*')
                pat  = re.compile('%s' % pattern, re.I)
                spec = {'dataset':pat}
                if  limit == -1:
                    for row in self.col.find(spec):
                        yield row['dataset']
                else:
                    for row in self.col.find(spec).skip(idx).limit(limit):
                        yield row['dataset']
            except:
                pass

    def datasets(self):
        """
        Retrieve a list of DBS datasets (DBS2)
        """
        time0 = time.time()
        if  self.dbs_url.find('DBSServlet') != -1: # DBS2
            for rec in self.datasets_dbs():
                rec.update({'ts':time0})
                yield rec
        else: # DBS3
            for rec in self.datasets_dbs3():
                rec.update({'ts':time0})
                yield rec

    def datasets_dbs(self):
        """
        Retrieve a list of DBS datasets (DBS2)
        """
        query = 'find dataset,dataset.status'
        params = {'api': 'executeQuery', 'apiversion': 'DBS_2_0_9',
                  'query':query}
        encoded_data = urllib.urlencode(params, doseq=True)
        url = self.dbs_url + '?' + encoded_data
        req = urllib2.Request(url)
        try:
            stream = urllib2.urlopen(req)
        except Exception as exc:
            print_exc(exc)
            msg = 'Fail to contact %s' % url
            raise Exception(msg)
        gen = qlxml_parser(stream, 'dataset')
        for row in gen:
            dataset = row['dataset']['dataset']
            rec = {'dataset': dataset}
            if  self.write_hash:
                storage_query = {"fields": ["dataset"],
                     "spec": [{"key": "dataset.name",
                               "value": "\"%s\"" % dataset}],
                     "instance": self.dbcoll}
                rec.update({'qhash': genkey(storage_query)})
            if  row['dataset']['dataset.status'] == 'VALID':
                yield rec
        stream.close()

    def datasets_dbs3(self):
        """
        Retrieve a list of DBS datasets (DBS3)
        """
        params = {'dataset_access_type':'VALID'}
        encoded_data = urllib.urlencode(params, doseq=True)
        url = self.dbs_url + '/datasets?' + encoded_data
        req = urllib2.Request(url)
        ckey, cert = get_key_cert()
        handler = HTTPSClientAuthHandler(ckey, cert)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
        stream = urllib2.urlopen(req)
        gen = json.load(stream)
        for row in gen:
            dataset = row['dataset']
            rec = {'dataset': dataset}
            if  self.write_hash:
                storage_query = {"fields": ["dataset"],
                     "spec": [{"key": "dataset.name",
                               "value": "\"%s\"" % dataset}],
                     "instance": self.dbcoll}
                rec.update({'qhash': genkey(storage_query)})
            yield rec
        stream.close()

def test(dbs_url):
    "Test function"
    uri = 'mongodb://localhost:8230'
    mgr = DBSDaemon(dbs_url, uri)
    mgr.update()
    idx = 0
    limit = 10
    for row in mgr.find('zee*summer', idx, limit):
        print row

def test_dbs2():
    "Test dbs2 service"
    url = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
    test(url)

def test_dbs3():
    "Test dbs3 service"
    url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datasets/'
    test(url)

if __name__ == '__main__':
    test_dbs2()
