#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=R0913,W0703,W0702
"""
File: dbs_daemon.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: DBS daemon, which update DAS cache with DBS datasets
"""
from __future__ import print_function

# system modules
import re
import json
import time
# import thread
import urllib
try:
    import urllib2
except ImportError:
    import urllib.request as urllib2
import itertools

# MongoDB modules
import pymongo
from pymongo.errors import InvalidOperation
from pymongo import ASCENDING

# DAS modules
from DAS.utils.utils import qlxml_parser, dastimestamp, print_exc
from DAS.utils.utils import get_dbs_instance
from DAS.utils.das_db import db_connection, is_db_alive, create_indexes
from DAS.utils.das_db import find_one
from DAS.utils.utils import get_key_cert, genkey
from DAS.utils.thread import start_new_thread
from DAS.utils.url_utils import HTTPSClientAuthHandler
from DAS.utils.das_config import das_readconfig
from DAS.utils.das_db import query_db


SKIP_UPDATES = 0

class DBSDaemon(object):
    """
    DBSDaemon fetch list of known datasets from DBS
    and store them in separate collection to be used by
    DAS autocomplete web interface.
    """
    def __init__(self, dbs_url, dburi, config=None):
        if  not config:
            config = {}
        self.dburi      = dburi
        self.dbcoll     = get_dbs_instance(dbs_url)
        self.dbs_url    = dbs_url
        self.dbname     = config.get('dbname', 'dbs')
        self.cache_size = config.get('cache_size', 1000)
        self.expire     = config.get('expire', 3600)
        self.write_hash = config.get('write_hash', False)
        # Shall we keep existing Datasets on server restart
        self.preserve_on_restart = config.get('preserve_on_restart', False)
        self.init()

    @property
    def col(self):
        "Return MongoDB collection object"
        conn = db_connection(self.dburi)
        dbc  = conn[self.dbname]
        col  = dbc[self.dbcoll]
        return col

    def init(self):
        """
        Init db connection and check that it is alive
        """
        try:
            indexes = [('dataset', ASCENDING), ('ts', ASCENDING)]
            create_indexes(self.col, indexes)

            if not self.preserve_on_restart:
                if  pymongo.version.startswith('3.'): # pymongo 3.X
                    self.col.delete_many({})
                else:
                    self.col.remove()
        except Exception as _exp:
            pass

    def update(self):
        """
        Update DBS collection with a fresh copy of datasets. Upon first insert
        of datasets we add dataset:__POPULATED__ record to be used as a flag
        that cache was populated in this cache.
        """
        if SKIP_UPDATES:
            return None

        dbc = self.col
        if  not dbc:
            print("%s DBSDaemon %s, no connection to DB" \
                % (dastimestamp(), self.dbcoll))
            return

        try:
            time0 = round(time.time())
            udict = {'$set':{'ts':time0}}
            cdict = {'dataset':'__POPULATED__'}
            gen = self.datasets()
            msg = ''
            if  not dbc.count():
                try: # perform bulk insert operation
                    if  pymongo.version.startswith('3.'): # pymongo 3.X
                        res = dbc.insert_many(gen)
                    else:
                        while True:
                            if  not dbc.insert(\
                                    itertools.islice(gen, self.cache_size)):
                                break
                except InvalidOperation as err:
                    # please note we need to inspect error message to
                    # distinguish InvalidOperation from generate exhastion
                    if  str(err) == 'cannot do an empty bulk insert':
                        dbc.insert(cdict)
                    pass
                except Exception as err:
                    pass
                # remove records with old ts
                spec = {'ts':{'$lt':time0-self.expire}}
                if  pymongo.version.startswith('3.'): # pymongo 3.X
                    dbc.delete_many(spec)
                else:
                    dbc.remove(spec)
#                 dbc.remove({'ts':{'$lt':time0-self.expire}})
                msg = 'inserted'
            else: # we already have records, update their ts
                for row in gen:
                    spec = dict(dataset=row['dataset'])
                    dbc.update(spec, udict, upsert=True)
                msg = 'updated'

            if  find_one(dbc, cdict):
                dbc.update(cdict, udict)
            print("%s DBSDaemon %s, %s %s records in %s sec" \
            % (dastimestamp(), self.dbcoll, msg, dbc.count(),
                    round(time.time()-time0)))
        except Exception as exc:
            print("%s DBSDaemon %s, fail to update, reason %s" \
                % (dastimestamp(), self.dbcoll, str(exc)))

    def find(self, pattern, idx=0, limit=10):
        """
        Find datasets for a given pattern. The idx/limit parameters
        control number of retrieved records (aka pagination). The
        limit=-1 means no pagination (get all records).
        """
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
        Retrieve a list of DBS datasets
        """
        time0 = time.time()
        for rec in self.datasets_dbs():
            rec.update({'ts':time0})
            yield rec

    def datasets_dbs(self):
        """
        Retrieve a list of DBS datasets
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

def find_datasets(pattern, dbs_instance, dbname='dbs', idx=0, limit=10,
                  ignorecase=True):
    """
    Find datasets for a given pattern. The idx/limit parameters
    control number of retrieved records (aka pagination). The
    limit=-1 means no pagination (get all records).
    """
    if len(pattern) > 0 and pattern[0] == '/':
        pattern = '^%s' % pattern
    if pattern.find('*') != -1:
        pattern = pattern.replace('*', '.*')
    try:
        re_flags = re.I if ignorecase else 0
        pat = re.compile('%s' % pattern, re_flags)
    except re.error:
        return

    # TODO: dbname is not set in any config...
    # TODO: validate DBS instance name, but it's in dasmapping.dbs_instances()
    # so probably it must be validated from outside
    dbcol = dbs_instance
    query = {'dataset': pat}
    for row in query_db(dbname, dbcol, query, idx, limit):
        yield row['dataset']


def test(dbs_url):
    "Test function"
    uri = das_readconfig()['mongodb']['dburi'][0]
    config = {'preserve_on_restart': True}
    mgr = DBSDaemon(dbs_url, uri, config)
    mgr.update()
    idx = 0
    limit = 10
    for row in mgr.find('zee*summer', idx, limit):
        print(row)

def test_find_static():
    """ Test the standalone find() """
    for row in find_datasets('*Zmm*', dbs_instance='prod/global'):
        print(row)

def test_dbs():
    "Test dbs3 service"
    url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/'
    test(url)

def initialize_global_dbs_mngr(update_required=False):
    """
    Gets a DBSDaemon for global DBS and fetches the data if needed.
    *Used for testing purposes only*.
    """
    from DAS.core.das_mapping_db import DASMapping

    dasconfig = das_readconfig()
    dasmapping = DASMapping(dasconfig)

    dburi = dasconfig['mongodb']['dburi']
    dbsexpire = dasconfig.get('dbs_daemon_expire', 3600)
    main_dbs_url = dasmapping.dbs_url()
    dbsmgr = DBSDaemon(main_dbs_url, dburi, {'expire': dbsexpire,
                                             'preserve_on_restart': True})

    # if we have no datasets (fresh DB, fetch them)
    if update_required or not next(dbsmgr.find('*Zmm*'), False):
        print('fetching datasets from global DBS...')
        dbsmgr.update()
    return dbsmgr


def get_global_dbs_inst():
    """
    gets the name of global dbs instance
    """
    from DAS.core.das_mapping_db import DASMapping
    dasconfig = das_readconfig()
    dasmapping = DASMapping(dasconfig)
    return dasmapping.dbs_global_instance()


def list_dbs_instances():
    """ list all DBS instances """
    from DAS.core.das_mapping_db import DASMapping
    dasconfig = das_readconfig()
    dasmapping = DASMapping(dasconfig)
    return dasmapping.dbs_instances()


if __name__ == '__main__':
    test_dbs()
    test_find_static()
