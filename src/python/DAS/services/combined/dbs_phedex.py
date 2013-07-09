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
from   DAS.utils.das_db import db_connection, create_indexes, db_monitor
from   DAS.utils.url_utils import getdata
from   DAS.utils.urlfetch_pycurl import getdata as urlfetch_getdata
from   DAS.web.tools import exposejson
from   DAS.utils.utils import qlxml_parser, dastimestamp, print_exc
from   DAS.utils.utils import get_key_cert
from   DAS.utils.thread import start_new_thread
from   DAS.core.das_mapping_db import DASMapping
from   DAS.utils.das_config import das_readconfig
import DAS.utils.jsonwrapper as json

PAT = re.compile("^T[0-3]_")
CKEY, CERT = get_key_cert()

def datasets(urls, which_dbs, verbose=0):
    """
    Retrieve list of datasets from DBS and compare each of them
    wrt list in MongoDB.
    """
    url     = urls.get('dbs')
    if  which_dbs == 'dbs': # DBS2 url
        gen = datasets_dbs2(urls, verbose)
    elif which_dbs == 'dbs3':
        gen = datasets_dbs3(urls, verbose)
    else:
        raise Exception('Unsupport DBS URL, url=%s' % url)
    for row in gen:
        yield row

def datasets_dbs3(urls, verbose=0):
    """DBS3 implementation of datasets function"""
    headers = {'Accept':'application/json;text/json'}
    records = []
    url     = urls.get('dbs3') + '/datasets'
    params  = {'detail':'True', 'dataset_access_type':'VALID'}
    data, _ = getdata(url, params, headers, post=False, verbose=verbose,
                ckey=CKEY, cert=CERT, doseq=False, system='dbs3')
    records = json.load(data)
    data.close()
    dbsdata = {}
    for row in records:
        if  not dbsdata.has_key(row['dataset']):
            dbsdata[row['dataset']] = \
                dict(era=row['acquisition_era_name'],
                        tier=row['data_tier_name'], status='VALID')
    for row in phedex_info(urls, dbsdata):
        yield row

def phedex_info(urls, dbsdata):
    "Get phedex info for given set of dbs data"
    # create list of URLs for urlfetch
    url  = urls.get('phedex') + '/blockReplicas'
    urls = ('%s?dataset=%s' % (url, d) for d in dbsdata.keys())
    headers  = {'Accept':'application/json;text/json'}
    gen  = urlfetch_getdata(urls, CKEY, CERT, headers)
    for ddict in gen:
        try:
            jsondict = json.loads(ddict['data'])
        except Exception as _exc:
            continue
        rec = {}
        for blk in jsondict['phedex']['block']:
            dataset = blk['name'].split('#')[0]
            if  not rec.has_key('nfiles'):
                nfiles = blk['files']
                size = blk['bytes']
            else:
                nfiles = rec['nfiles'] + blk['files']
                size = rec['size'] + blk['bytes']
            rec.update({'nfiles':nfiles, 'size':size})
            for rep in blk['replica']:
                if  not rec.has_key('site'):
                    rec = dict(dataset=dataset, nfiles=nfiles, size=size,
                                site=[rep['node']], se=[rep['se']],
                                custodial=[rep['custodial']])
                    rec.update(dbsdata[dataset])
                else:
                    sites = rec['site']
                    ses = rec['se']
                    custodial = rec['custodial']
                    if  rep['node'] not in sites:
                        sites.append(rep['node'])
                        ses.append(rep['se'])
                        custodial.append(rep['custodial'])
                    rec.update({'site':sites, 'se':ses, 'custodial':custodial})
        if  rec:
            # unwrap the site/se/custodial lists and yield records w/ their
            # individual values
            for idx in range(0, len(rec['site'])):
                sename = rec['se'][idx]
                site = rec['site'][idx]
                custodial = rec['custodial'][idx]
                newrec = dict(rec)
                newrec['se'] = sename
                newrec['site'] = site
                newrec['custodial'] = custodial
                yield newrec

def datasets_dbs2(urls, verbose=0):
    """DBS2 implementation of datasets function"""
    headers = {'Accept':'application/xml;text/xml'}
    records = []
    url     = urls.get('dbs')
    query   = \
        'find dataset,dataset.tier,dataset.era where dataset.status like VALID*'
    params  = {'api':'executeQuery', 'apiversion':'DBS_2_0_9', 'query':query}
    stream, _ = getdata(url, params, headers, post=False, \
            ckey=CKEY, cert=CERT, verbose=verbose, system='dbs')
    records = [r for r in qlxml_parser(stream, 'dataset')]
    stream.close()
    dbsdata = {}
    for row in records:
        dataset = row['dataset']
        if  not dbsdata.has_key(dataset['dataset']):
            dbsdata[dataset['dataset']] = \
                dict(era=dataset['dataset.era'],
                        tier=dataset['dataset.tier'], status='VALID')
    for row in phedex_info(urls, dbsdata):
        yield row

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
    data, _  = getdata(url, params, headers, post=True, \
            ckey=CKEY, cert=CERT, verbose=verbose, system='dbs_phedex')
    if  isinstance(data, basestring): # no response
        dastimestamp('DBS_PHEDEX ERROR: %s' % data)
        return
    jsondict = json.load(data)
    data.close()
    for row in jsondict['phedex']['block']:
        dataset = row['name'].split('#')[0]
        for rep in row['replica']:
            rec = dict(dataset=dataset,
                        nfiles=row['files'],
                        size=row['bytes'],
                        site=rep['node'],
                        se=rep['se'],
                        custodial=rep['custodial'])
            rec.update(datasetdict[dataset])
            yield rec
    data.close()

def collection(uri):
    """
    Return collection cursor
    """
    conn = MongoClient(uri)
    coll = conn['db']['datasets']
    return coll

def update_db(urls, which_dbs, uri, db_name, coll_name):
    """
    Update DB info with dataset info.
    """
    tst  = time.time()
    conn = db_connection(uri)
    if  conn:
        coll = conn[db_name][coll_name]
        for row in datasets(urls, which_dbs):
            dataset = dict(row)
            dataset.update({'ts':tst})
            spec = dict(dataset=dataset['dataset'], site=dataset['site'])
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
        out.dataset = doc.dataset;
        out.site = doc.site;
        out.se = doc.se;
        out.era = doc.era;
        out.tier = doc.tier;
        out.custodial = doc.custodial;
        out.nfiles += parseInt(doc.nfiles);
        out.count += 1;}""")
    finalize = Code("""function(out) {
        out.nfiles = parseInt(out.nfiles);
        out.size = parseFloat(out.size);
        }""")
    if  PAT.match(site):
        key = 'site'
    else:
        key = 'se'
    pat = re.compile('%s.*' % site.replace('*', ''))
#    spec = {key:site}
    spec = {key:pat}
    key  = {'dataset':True}
    for row in coll.group(key, spec, initial, redfunc, finalize):
        yield row

def find_dataset_mp(coll, site):
    """
    Find dataset info in MongoDB using map/reduce operation.
    """
    # implementation via group function
    mapfunc = Code("""function() {
        emit(this.dataset, {count:1, size:this.size, nfiles:this.nfiles,
            site:this.site, se:this.se, era:this.era, tier:this.tier,
            custodial:this.custodial, dataset:this.dataset});
        }""")
    redfunc = Code("""function(key, values) {
        var result = {count:0, dataset:"", size:0, nfiles:0,
            site:"",se:"",era:"",tier:"",custodial:""};
        values.forEach(function(value) {
            result.dataset = value.dataset;
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
    pat = re.compile('%s.*' % site.replace('*', ''))
#    spec = {key:site}
    spec = {key:pat}
    result = coll.map_reduce(mapfunc, redfunc, "results", query=spec)
    for row in result.find():
        yield row['value']

def worker(urls, which_dbs, uri, db_name, coll_name, interval=3600):
    """
    Daemon which updates DBS/Phedex DB
    """
    conn_interval = 3 # default connection failure sleep interval
    threshold = 60 # 1 minute is threashold to check connections
    time0 = time.time()
    print "### Start dbs_phedex worker"
    while True:
        if  conn_interval > threshold:
            conn_interval = threshold
        try:
            update_db(urls, which_dbs, uri, db_name, coll_name)
            print "%s update dbs_phedex DB %s sec" \
                % (dastimestamp(), time.time()-time0)
            time0 = time.time()
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
        self.expire     = 60   # defined at run-time via self.init()
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
            service_api  = self.config.get('api', 'dataset4site_release')
            mapping      = dasmapping.servicemap(service_name)
            self.urls    = mapping[service_api]['services']
            self.expire  = mapping[service_api]['expire']
            services     = self.dasconfig['services']
            which_dbs    = [d for d in services if d.find('dbs') != -1][0]
            if  not self.worker_thr:
                # Worker thread which update dbs/phedex DB
                self.worker_thr = start_new_thread('dbs_phedex_worker', worker, \
                (self.urls, which_dbs,
                    self.uri, self.dbname, self.collname, self.expire))
            msg = "### DBSPhedexService:init started with %s service" \
                    % which_dbs
            print msg
        except Exception as exc:
            print "### Fail DBSPhedexService:init\n", str(exc)
            self.urls       = None
            self.expire     = 60
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

    def find(self, site, operation, rkey='summary'):
        """Find dataset info for a given site"""
        reason = 'waiting for DBS/Phedex info'
        if  PAT.match(site):
            key = 'name'
        else:
            key = 'se'
        rec = {rkey:{'name':'N/A', 'reason':reason}, site:{key:site}}
        if  self.isexpired():
            yield rec
        else:
            if  self.coll:
                for item in find_dataset(self.coll, site, operation):
                    yield {rkey:item}
            else:
                reason = 'lost connection to internal DB'
                rec.update({rkey:{'name':'N/A', 'reason':reason}})
                yield rec

    @exposejson
    def dataset_site(self, site, operation="mapreduce"):
        """
        Service exposed method to get dataset/site info
        """
        operation = 'group'
        rkey = 'summary' # record key
        records = [r for r in self.find(site, operation, rkey)]
        if  len(records) == 1 and records[0][rkey].has_key('reason'):
            expires = 60 # in seconds
        else:
            expires = self.expire
        cherrypy.lib.caching.expires(secs=expires, force = True)
        return records

def test():
    """Test main function"""
    cherrypy.quickstart(DBSPhedexService({}), '/')

def test_dbs(which_dbs):
    urls = {
        "dbs": "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet",
        "dbs3": "https://cmsweb.cern.ch/dbs/prod/global/DBSReader",
        "phedex": "https://cmsweb.cern.ch/phedex/datasvc/json/prod",
        "conddb": "https://cms-conddb.cern.ch",
    }
    if  which_dbs == 'dbs':
        gen = datasets_dbs2(urls)
    else:
        gen = datasets_dbs3(urls)
    for row in gen:
        print "\n### row", row

if __name__ == '__main__':
#    test()
    test_dbs('dbs')
