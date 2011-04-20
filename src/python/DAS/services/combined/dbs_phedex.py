#!/usr/bin/python
#-*- coding: ISO-8859-1 -*-
"""
DAS DBS/Phedex combined service to fetch dataset/site information.
"""

import time
import thread
import urllib
import urllib2
import cherrypy
import traceback

from pymongo import Connection, DESCENDING
from bson.code import Code
from cherrypy import expose

import DAS.utils.jsonwrapper as json
from DAS.utils.das_db import db_connection
from DAS.web.tools import exposejson

def getdata(url, params, headers=None, post=None, verbose=0):
    """
    Invoke URL call and retrieve data from data-service based
    on provided URL and set of parameters. Use post=True to
    invoke POST request.
    """
    encoded_data = urllib.urlencode(params, doseq=True)
    if  not post:
        url = url + '?' + encoded_data
    if  not headers:
        headers = {}
    req = urllib2.Request(url)
    for key, val in headers.items():
        req.add_header(key, val)
    if  verbose > 1:
        handler = urllib2.HTTPHandler(debuglevel=1)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    if  post:
        data = urllib2.urlopen(req, encoded_data)
    else:
        data = urllib2.urlopen(req)
    return data

def datasets(urls, verbose=0):
    """
    Retrieve list of datasets from DBS and compare each of them
    wrt list in MongoDB.
    """
    headers = {'Accept':'application/json;text/json'}
    records = []
    url = urls.get('dbs')
    params = {'detail':'True', 'dataset_access_type':'PRODUCTION'}
    data = getdata(url, params, headers, verbose=verbose)
    records = json.load(data)
    data.close()
    data = {}
    size = 10 # size for POST request to Phedex
    for row in records:
        if  not data.has_key(row['dataset']):
            data[row['dataset']] = \
            dict(era=row['acquisition_era_name'], tier=row['data_tier_name'])
        if  len(data.keys()) > size:
            for rec in dataset_info(urls, data):
                yield rec
            data = {}
    if  data:
        for rec in dataset_info(urls, data):
            yield rec

def dataset_info(urls, datasetdict, verbose=0):
    """
    Request blockReplicas information from Phedex for a given
    dataset or a list of dataset (use POST request in later case).
    Update MongoDB with aggregated information about dataset:
    site, size, nfiles, nblocks.
    """
    url = urls.get('phedex')
    params = {'dataset': [d for d in datasetdict.keys()]}
    headers = {'Accept':'application/json;text/json'}
    data = getdata(url, params, headers, post=True, verbose=verbose)
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

def collection(uri):
    """
    Return collection cursor
    """
    conn = Connection(uri)
    coll = conn['db']['datasets']
    return coll

def update_db(urls, uri, db_name, coll_name):
    """
    Update DB info with dataset info.
    """
    conn = db_connection(uri)
    coll = conn[db_name][coll_name]
    coll.drop()
    for dataset in datasets(urls):
        coll.insert(dataset)
    record = {'timestamp':time.time()}
    coll.insert(record)

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
    spec = {'site':site}
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
    spec = {'site':site}
    result = coll.map_reduce(mapfunc, redfunc, "results", query=spec)
    for row in result.find():
        yield row['value']

def worker(urls, uri, db_name, coll_name, interval=3600):
    """
    Daemon which update DBS/Phedex DB
    """
    while True:
        try:
            update_db(urls, uri, db_name, coll_name)
        except Exception, _exp:
            traceback.print_exc()
        time.sleep(interval)

class DBSPhedexService(object):
    """DBSPhedexService"""
    def __init__(self, uri, urls):
        super(DBSPhedexService, self).__init__()
        self.conn     = db_connection(uri)
        self.dbname   = 'db'
        self.collname = 'datasets'
        self.coll = self.conn[self.dbname][self.collname]
        indexes = [('name', DESCENDING), ('site', DESCENDING), 
                   ('timestamp', DESCENDING)]
        for index in indexes:
            self.coll.create_index([index])
        self._expired = 1*60*60 # 1 hour
#        thread.start_new_thread(worker, \
#            (urls, uri, self.dbname, self.collname, self._expired))

    def is_expired(self):
        """
        Check if data is expired in DB.
        """
        spec = {'timestamp': {'$lt': time.time() + self._expired}}
        if  self.coll.find_one(spec):
            return False
        return True

    def find_dataset(self, site, operation):
        """Find dataset info for a given site"""
        if  self.is_expired():
            rec = \
            dict(dataset={'name':'N/A', 'reason':'waiting for DBS3/Phedex info'}, \
                 site={'name':site})
            yield rec
        else:
            for item in find_dataset(self.coll, site, operation):
                yield {'dataset':item}

    @exposejson
    def dataset(self, site, operation="mapreduce"):
        """
        Service exposed method to get dataset/site info
        """
        records = [r for r in self.find_dataset(site, operation)]
        if  len(records) == 1 and records[0]['dataset'] == 'N/A':
            expires = 60 # in seconds
        else:
            expires = self._expired
        cherrypy.lib.caching.expires(secs=expires, force = True)
        return records
        
def main():
    """Test main function"""
    uri = 'mongodb://localhost:8230'
    urls = {'dbs':'http://localhost:8989/dbs/DBSReader/datasets',
    'phedex':'https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockReplicas'}
    cherrypy.quickstart(DBSPhedexService(uri, urls), '/')

if __name__ == '__main__':
    main()
