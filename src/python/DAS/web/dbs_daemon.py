#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: dbs_daemon.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: DBS daemon, which update DAS cache with DBS datasets
"""

# system modules
import re
import urllib
import urllib2
import itertools

# MongoDB modules
from pymongo.errors import InvalidOperation
from pymongo import ASCENDING

# DAS modules
import DAS.utils.jsonwrapper as json
from DAS.utils.utils import xml_parser
from DAS.utils.das_db import db_connection
from DAS.utils.das_db import create_indexes

class DBSDaemon(object):
    """docstring for DBSDaemon"""
    def __init__(self, dburi, dbname='dbs', dbcoll='datasets', cache_size=10000):
        self.con = db_connection(dburi)
        self.col = self.con[dbname][dbcoll]
        self.cache_size = cache_size
        create_indexes(self.col, [('dataset', ASCENDING)])

    def update(self):
        """
        Update DBS collection with a fresh copy of datasets
        """
        self.col.remove()
        gen = self.datasets()
        try: # perform bulk insert operation
            while True:
                if  not self.col.insert(itertools.islice(gen, self.cache_size)):
                    break
        except InvalidOperation:
            pass

    def find(self, pattern, idx=0, limit=10):
        """
        Find datasets for a given pattern. The idx/limit parameters 
        control number of retrieved records (aka pagination). The
        limit=-1 means no pagination (get all records).
        """
        if  pattern[0] == '/':
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

    def datasets(self):
        """
        Retrieve a list of DBS datasets (DBS2)
        """
        url = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
        params = {'api': 'listDatasetPaths', 'user_type': 'NORMAL', 'apiversion': 'DBS_2_0_9'}
        encoded_data = urllib.urlencode(params, doseq=True)
        url = url + '?' + encoded_data
        req = urllib2.Request(url)
        stream = urllib2.urlopen(req)
        gen = xml_parser(stream, 'processed_dataset')
        for row in gen:
            yield dict(dataset=row['processed_dataset']['path'])
        stream.close()

    def dbs3_datasets(self):
        """
        Retrieve a list of DBS datasets (DBS3)
        """
        url = 'http://localhost:8989/dbs/DBSReader/datasets/'
        params = {'dataset_access_type':'PRODUCTION'}
        encoded_data = urllib.urlencode(params, doseq=True)
        url = url + '?' + encoded_data
        req = urllib2.Request(url)
        stream = urllib2.urlopen(req)
        gen = json.load(stream)
        for row in gen:
            yield row
        stream.close()
        
def main():
    uri = 'mongodb://localhost:8230'
    mgr = DBSDaemon(uri)
    mgr.update()
    idx = 0
    limit = 10
    for row in mgr.find('zee*summer', idx, limit):
        print row

if __name__ == '__main__':
    main()        
