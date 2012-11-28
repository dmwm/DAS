#!/usr/bin/python
#-*- coding: ISO-8859-1 -*-
"""
DAS DBS/Phedex combined service to fetch dataset/site information.
"""

# system modules
import re
import time
import thread
import cherrypy

# pymongo modules
from   pymongo import MongoClient, DESCENDING
from   pymongo.errors import AutoReconnect, ConnectionFailure
from   bson.code import Code

# DAS modules
from   DAS.utils.das_db import db_connection, create_indexes
from   DAS.utils.url_utils import getdata
from   DAS.web.tools import exposejson
from   DAS.web.utils import db_monitor
from   DAS.utils.utils import qlxml_parser, dastimestamp, print_exc
from   DAS.utils.utils import get_key_cert
from   DAS.utils.thread import set_thread_name, start_new_thread
from   DAS.core.das_mapping_db import DASMapping
from   DAS.utils.das_config import das_readconfig
import DAS.utils.jsonwrapper as json

PAT = re.compile("^T[0-3]_")
        
def datasets(urls, verbose=0):
    """
    Retrieve list of datasets from DBS and compare each of them
    wrt list in MongoDB.
    """
    url     = urls.get('dbs')
    if  url.find('servlet') != -1: # DBS2 url
        gen = datasets_dbs2(urls, verbose)
    elif url.find('cmsweb') != -1 and url.find('DBSReader') != -1:
        gen = datasets_dbs3(urls, verbose)
    else:
        raise Exception('Unsupport DBS URL, url=%s' % url)
    for row in gen:
        yield row

def datasets_dbs3(urls, verbose=0):
    """DBS3 implementation of datasets function"""
    headers = {'Accept':'application/json;text/json'}
    records = []
    url     = urls.get('dbs')
    params  = {'detail':'True', 'dataset_access_type':'VALID'}
    ckey, cert = get_key_cert()
    data, _ = getdata(url, params, headers, verbose=verbose,
                ckey=ckey, cert=cert, doseq=False)
    records = json.load(data)
    data.close()
    data = {}
    size = 10 # size for POST request to Phedex
    for row in records:
        if  not data.has_key(row['dataset']):
            data[row['dataset']] = \
                dict(era=row['acquisition_era_name'],
                        tier=row['data_tier_name'], status='VALID')
        if  len(data.keys()) > size:
            for rec in dataset_info(urls, data):
                yield rec
            data = {}
    if  data:
        for rec in dataset_info(urls, data):
            yield rec

def datasets_dbs2(urls, verbose=0):
    """DBS2 implementation of datasets function"""
    headers = {'Accept':'application/xml;text/xml'}
    records = []
    url     = urls.get('dbs')
    query   = \
        'find dataset,dataset.tier,dataset.era where dataset.status like VALID*'
    params  = {'api':'executeQuery', 'apiversion':'DBS_2_0_9', 'query':query}
    stream, _ = getdata(url, params, headers, verbose=verbose)
    records = [r for r in qlxml_parser(stream, 'dataset')]
    stream.close()
    data = {}
    size = 10 # size for POST request to Phedex
    for row in records:
        dataset = row['dataset']
        if  not data.has_key(dataset['dataset']):
            data[dataset['dataset']] = \
                dict(era=dataset['dataset.era'],
                        tier=dataset['dataset.tier'], status='VALID')
        if  len(data.keys()) > size:
            for rec in dataset_info(urls, data):
                yield rec
            data = {}
    if  data:
        for rec in dataset_info(urls, data):
            yield rec
    del records

def dataset_info(urls, datasetdict, verbose=0):
    """
    Request blockReplicas information from Phedex for a given
    dataset or a list of dataset (use POST request in later case).
    Update MongoDB with aggregated information about dataset:
    site, size, nfiles, nblocks.
    """
    url      = urls.get('phedex') + '/blockReplicas'
    params   = {'dataset': [d for d in datasetdict.keys()]}
    headers  = {'Accept':'application/json;text/json'}
    data, _  = getdata(url, params, headers, post=True, verbose=verbose)
    jsondict = json.load(data)
    data.close()
    for row in jsondict['phedex']['block']:
        name = row['name'].split('#')[0]
        for rep in row['replica']:
            rec = dict(name=name, 
                        nfiles=row['files'],
                        size=row['bytes'],
                        site=rep['node'], 
                        se=rep['se'],
                        custodial=rep['custodial'])
            rec.update(datasetdict[name])
            yield rec
    data.close()

def collection(uri):
    """
    Return collection cursor
    """
    conn = MongoClient(uri)
    coll = conn['db']['datasets']
    return coll

def update_db(urls, uri, db_name, coll_name):
    """
    Update DB info with dataset info.
    """
    tst  = time.time()
    conn = db_connection(uri)
    if  conn:
        coll = conn[db_name][coll_name]
        for dataset in datasets(urls):
            dataset.update({'ts':tst})
            spec = dict(name=dataset['name'])
            coll.update(spec, dataset, upsert=True)
        coll.remove({'ts': {'$lt': tst}})
    else:
        raise ConnectionFailure('could not establish connection')

def find_dataset(coll, site, operation="mapreduce"):
    """
    Return dataset info using provided operation.
    """
    if  operation == 'group':
        for row in find_dataset_group(coll, site):
            yield row
    else:
        for row in find_dataset_mp(coll, site):
            yield row
        
def find_dataset_group(coll, site):
    """
    Find dataset info in MongoDB using group operation.
    """
    # implementation via group function
    initial = {'count':0, 'nfiles':0, 'size':0}
    redfunc = Code("""function(doc, out) {
        out.size += parseFloat(doc.size);
        out.site = doc.site;
        out.se = doc.se;
        out.era = doc.era;
        out.tier = doc.tier;
        out.custodial = doc.custodial;
        out.nfiles += parseInt(doc.nfiles);
        out.count += 1;}""")
    finalize = Code("""function(out) {
        out.nfiles = parseInt(out.nfiles); }""")
    if  PAT.match(site):
        key = 'site'
    else:
        key = 'se'
    spec = {key:site}
    key  = {'name':True}
    for row in coll.group(key, spec, initial, redfunc, finalize):
        yield row

def find_dataset_mp(coll, site):
    """
    Find dataset info in MongoDB using map/reduce operation.
    """
    # implementation via group function
    mapfunc = Code("""function() {
        emit(this.name, {count:1, size:this.size, nfiles:this.nfiles,
            site:this.site, se:this.se, era:this.era, tier:this.tier,
            custodial:this.custodial, name:this.name});
        }""")
    redfunc = Code("""function(key, values) {
        var result = {count:0, name:"", size:0, nfiles:0, 
            site:"",se:"",era:"",tier:"",custodial:""};
        values.forEach(function(value) {
            result.name = value.name;
            result.count += parseInt(value.count);
            result.size += parseFloat(value.size);
            result.nfiles += parseInt(value.nfiles);
            result.site = value.site;
            result.se = value.se;
            result.era = value.era;
            result.tier = value.tier;
            result.custodial = value.custodial;
        });
        return result;}""")
    if  PAT.match(site):
        key = 'site'
    else:
        key = 'se'
    spec = {key:site}
    result = coll.map_reduce(mapfunc, redfunc, "results", query=spec)
    for row in result.find():
        yield row['value']

def worker(urls, uri, db_name, coll_name, interval=3600):
    """
    Daemon which updates DBS/Phedex DB
    """
    conn_interval = 3 # default connection failure sleep interval
    threshold = 60 # 1 minute is threashold to check connections
    time0 = time.time()
    print "\n### Start dbs_phedex worker"
    while True:
        if  conn_interval > threshold:
            conn_interval = threshold
        try:
            update_db(urls, uri, db_name, coll_name)
            print "%s update dbs_phedex DB %s sec" \
                % (dastimestamp(), time.time()-time0)
            time.sleep(interval)
        except AutoReconnect as _err:
            time.sleep(conn_interval) # handles broken connection
            conn_interval *= 2
        except ConnectionFailure as _err:
            time.sleep(conn_interval) # handles broken connection
            conn_interval *= 2
        except Exception as exp:
            print_exc(exp)
            time.sleep(conn_interval)
            conn_interval *= 2

class DBSPhedexService(object):
    """DBSPhedexService"""
    def __init__(self, config=None):
        if  not config:
            config = {}
        super(DBSPhedexService, self).__init__()
        self.config     = config
        self.dbname     = 'dbs_phedex'
        self.collname   = 'datasets'
        self.dasconfig  = das_readconfig()
        self.uri        = self.dasconfig['mongodb']['dburi']
        self.urls       = None # defined at run-time via self.init()
        self.expire     = None # defined at run-time via self.init()
        self.coll       = None # defined at run-time via self.init()
        self.worker_thr = None # defined at run-time via self.init()
        self.init()

        # Monitoring thread which performs auto-reconnection
        name = 'dbs_phedex_monitor'
        start_new_thread(name, db_monitor, (self.uri, self.init, 5))

    def init(self):
        """Takes care of MongoDB connection"""
        try:
            conn = db_connection(self.uri)
            self.coll = conn[self.dbname][self.collname]
            indexes = [('name', DESCENDING), ('site', DESCENDING), 
                       ('ts', DESCENDING)]
            for index in indexes:
                create_indexes(self.coll, [index])
            dasmapping   = DASMapping(self.dasconfig)
            service_name = self.config.get('name', 'combined')
            service_api  = \
                    self.config.get('api', 'combined_dataset4site_release')
            mapping      = dasmapping.servicemap(service_name)
            self.urls    = mapping[service_api]['services']
            self.expire  = mapping[service_api]['expire']
            if  not self.worker_thr:
                # Worker thread which update dbs/phedex DB
                self.worker_thr = thread.start_new_thread(worker, \
                (self.urls, self.uri, self.dbname, self.collname, self.expire))
                set_thread_name(self.worker_thr, 'dbs_phedex_worker')
        except Exception as exc:
            self.urls       = None
            self.expire     = None
            self.coll       = None
            self.worker_thr = None

    def isexpired(self):
        """
        Check if data is expired in DB.
        """
        spec = {'ts': {'$lt': time.time() + self.expire}}
        if  self.coll and self.coll.find_one(spec):
            return False
        return True

    def find_dataset(self, site, operation):
        """Find dataset info for a given site"""
        reason = 'waiting for DBS3/Phedex info'
        if  PAT.match(site):
            key = 'name'
        else:
            key = 'se'
        rec = dict(dataset={'name':'N/A', 'reason':reason}, site={key:site})
        if  self.isexpired():
            yield rec
        else:
            if  self.coll:
                for item in find_dataset(self.coll, site, operation):
                    yield {'dataset':item}
            else:
                reason = 'lost connection to internal DB'
                rec.update({'dataset':{'name':'N/A', 'reason':reason}})
                yield rec

    @exposejson
    def dataset(self, site, operation="mapreduce"):
        """
        Service exposed method to get dataset/site info
        """
        records = [r for r in self.find_dataset(site, operation)]
        if  len(records) == 1 and records[0]['dataset'].has_key('reason'):
            expires = 60 # in seconds
        else:
            expires = self.expire
        cherrypy.lib.caching.expires(secs=expires, force = True)
        return records
        
def test():
    """Test main function"""
    cherrypy.quickstart(DBSPhedexService({}), '/')

if __name__ == '__main__':
    test()
