#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703,R0912,R0913,R0914,R0915,W0702,R0902

"""
Core class for Data Aggregation Service (DAS) framework.
It performs the following tasks:

- registers data-services found in DAS configuration file (das.cfg).
- invoke data-service APIs
- merge results based on common keys
- pass results to presentation layer (CLI or WEB)
"""
from __future__ import print_function

__author__ = "Valentin Kuznetsov"

# system modules
import os
import time
import itertools

# DAS modules
from DAS.core.das_ql import das_operators, das_special_keys
from DAS.core.das_query import DASQuery
from DAS.core.das_mapping_db import DASMapping
from DAS.core.das_keylearning import DASKeyLearning
from DAS.core.das_mongocache import DASMongocache
from DAS.utils.query_utils import compare_specs, decode_mongo_query
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import PrintManager
from DAS.utils.utils import expire_timestamp, print_exc, fix_times
from DAS.utils.utils import api_rows, das_sinfo, regen, dastimestamp
from DAS.utils.task_manager import TaskManager, PluginTaskManager
from DAS.utils.das_timer import das_timer, get_das_timer
from DAS.utils.global_scope import SERVICES

# DAS imports
import DAS.core.das_aggregators as das_aggregator

def dasheader(system, dasquery, expire, api=None, url=None, ctime=None,
        services=None):
    """
    Return DAS header (dict) wrt DAS specifications:

         - system represents DAS services, e.g. combined
         - dasquery is DASQuery representation
         - expire is expire timestamp of the record
         - api is data-service API name
         - url is data-service URL
         - ctime is current timestamp
         - services is a dict (or list of dicts) of CMS services contributed
           to data record, e.g. combined service uses dbs and phedex
    """
    # tstamp must be integer in order for json encoder/decoder to
    # work properly, see utils/jsonwrapper/__init__.py
    tstamp = round(time.time())
    if  isinstance(system, basestring):
        system = [system]
    if  not api:
        dasdict = dict(system=system, ts=tstamp,
                    expire=expire_timestamp(expire),
                    status="requested")
    else:
        dasdict = dict(system=system, ts=tstamp,
                    url=[url], ctime=[ctime],
                    expire=expire_timestamp(expire), urn=[api],
                    api=[api], status="requested")
    if  services:
        if  isinstance(services, dict):
            services = [services]
        dasdict.update({"services": services})
    return dict(das=dasdict)

class DASCore(object):
    """
    DAS core class.
    """
    def __init__(self, config=None, debug=0,
                nores=False, logger=None, engine=None, multitask=True):
        if  config:
            dasconfig = config
        else:
            dasconfig = das_readconfig()
        verbose       = dasconfig['verbose']
        self.stdout   = debug
        if  isinstance(debug, int):
            self.verbose = debug
            dasconfig['verbose'] = debug
        else:
            self.verbose = verbose
        das_timer('DASCore::init', self.verbose)
        self.operators = das_operators()

        # set noresults option
        self.noresults = False
        if  nores:
            dasconfig['write_cache'] = True
            self.noresults = nores

        self.multitask = dasconfig['das'].get('multitask', True)
        if  debug or self.verbose:
            self.multitask = False # in verbose mode do not use multitask
            dasconfig['das']['multitask'] = False
        if  not multitask: # explicitly call DASCore ctor
            self.multitask = False
            dasconfig['das']['multitask'] = False
        dasconfig['engine'] = engine
        if  self.multitask:
            nworkers = dasconfig['das'].get('core_workers', 5)
            if  engine:
                thr_name = 'DASCore:PluginTaskManager'
                self.taskmgr = PluginTaskManager(\
                        engine, nworkers=nworkers, name=thr_name)
                self.taskmgr.subscribe()
            else:
                thr_name = 'DASCore:TaskManager'
                self.taskmgr = TaskManager(nworkers=nworkers, name=thr_name)
        else:
            self.taskmgr = None

        if  logger:
            self.logger = logger
        else:
            self.logger = PrintManager('DASCore', self.verbose)

        # define Mapping/Analytics/Parser in this order since Parser depends
        # on first two
        dasmapping = DASMapping(dasconfig)
        dasconfig['dasmapping'] = dasmapping
        self.mapping = dasmapping

        self.keylearning = DASKeyLearning(dasconfig)
        dasconfig['keylearning'] = self.keylearning

        # init DAS cache
        self.rawcache = DASMongocache(dasconfig)
        dasconfig['rawcache'] = self.rawcache

        # plug-in architecture: loop over registered data-services in
        # dasconfig; load appropriate module/class; register data
        # service with DASCore.
        self.systems = dasmapping.list_systems()
        # pointer to the DAS top level directory
        dasroot = '/'.join(__file__.split('/')[:-3])
        for name in self.systems:
            try:
                klass  = 'DAS/services/%s/%s_service.py' \
                    % (name, name)
                srvfile = os.path.join(dasroot, klass)
                with file(srvfile) as srvclass:
                    for line in srvclass:
                        if  line.find('(DASAbstractService)') != -1:
                            klass = line.split('(DASAbstractService)')[0]
                            klass = klass.split('class ')[-1] 
                            break
                mname  = 'DAS.services.%s.%s_service' % (name, name)
                module = __import__(mname, fromlist=[klass])
                obj = getattr(module, klass)(dasconfig)
                setattr(self, name, obj)
                SERVICES[name] = obj
            except IOError as err:
                if  debug > 1:
                    # we have virtual services, so IOError can be correct
                    print_exc(err)
                try:
                    mname  = 'DAS.services.generic_service'
                    module = __import__(mname, fromlist=['GenericService'])
                    obj    = module.GenericService(name, dasconfig)
                    setattr(self, name, obj)
                except Exception as exc:
                    print_exc(exc)
                    msg = "Unable to load %s data-service plugin" % name
                    raise Exception(msg)
            except Exception as exc:
                print_exc(exc)
                msg = "Unable to load %s data-service plugin" % name
                raise Exception(msg)

        # loop over systems and get system keys, add mapping keys to final list
        self.service_keys = {}
        self.service_parameters = {}
        for name in self.systems: 
            skeys = getattr(self, name).keys()
            self.service_keys[getattr(self, name).name] = skeys
            sparams = getattr(self, name).parameters()
            self.service_parameters[getattr(self, name).name] = sparams

        self.service_keys['special'] = das_special_keys()
        self.dasconfig = dasconfig
        das_timer('DASCore::init', self.verbose)

    def keys(self):
        """
        Return map of data service keys
        """
        return self.service_keys

    def das_keys(self):
        """
        Return map of data service keys
        """
        _keys = ['records']
        for values in self.service_keys.values():
            for key in values:
                if  key not in _keys:
                    _keys.append(key)
        return _keys

    def result(self, query, idx=0, limit=None):
        """
        Get results either from cache or from explicit call
        """
        self.logger.info('input query=%s' % query)
        results = []
        dasquery = DASQuery(query)
        query    = dasquery.mongo_query
        # check if we have any service which cover the query
        # otherwise decompose it into list of queries
        service_map = dasquery.service_apis_map()
        if  not service_map:
            msg  = 'no APIs found to answer input query, will decompose it'
            self.logger.info(msg)
            skeys = query['fields']
            if  not skeys:
                skeys = []
            for key in skeys:
                newquery = DASQuery(dict(fields=[key], spec=query['spec']))
                self.call(newquery) # process query
        else:
            self.call(dasquery) # process query

        # lookup provided query in a cache
        if  not self.noresults:
            results = self.get_from_cache(dasquery, idx, limit)
        return results

    def remove_from_cache(self, dasquery):
        """
        Delete in cache entries about input query
        """
        self.rawcache.remove_from_cache(dasquery)

    def get_status(self, dasquery):
        """
        Look-up status of provided query in a cache.
        Return status of the query request and its hash.
        """
        status = None
        error  = None
        reason = None
        if  dasquery and 'fields' in dasquery.mongo_query:
            fields = dasquery.mongo_query['fields']
            if  fields and isinstance(fields, list) and 'queries' in fields:
                return 'ok', error, reason
        record = self.rawcache.find(dasquery)
        error, reason = self.rawcache.is_error_in_records(dasquery)
        try:
            if  record and 'das' in record and 'status' in record['das']:
                status = record['das']['status']
                if  not error:
                    error = record['das'].get('error', error)
                if  not reason:
                    reason = record['das'].get('reason', reason)
                return status, error, reason
        except Exception as exc:
            print_exc(exc)
            status = error = reason = None
            self.rawcache.remove_from_cache(dasquery)
        return status, error, reason

    def worker(self, srv, dasquery):
        """Main worker function which calls data-srv call function"""
        self.logger.info('##### %s ######\n' % srv)
        das_timer(srv, self.verbose)
        getattr(getattr(self, srv), 'call')(dasquery)
        das_timer(srv, self.verbose)

    def insert_query_records(self, dasquery):
        """
        Insert DAS query records into DAS cache and return list of services
        which will answer this query
        """
        services = dasquery.services
        self.logger.info('Potential services = %s' % services)
        if  not services:
            msg  = 'No data-services for query %s' % dasquery
            msg += 'mongo_query: %s' % dasquery.mongo_query
            msg += 'params: %s' % dasquery.params()
            print(dastimestamp('DAS WARNING '), msg)

        # get list of URI which can answer this query
        ack_services = []
        for srv in services:
            gen = [t for t in getattr(getattr(self, srv), 'apimap')(dasquery)]
            for url, api, args, iformat, expire in gen:
                header = dasheader(srv, dasquery, expire, api, url, ctime=0)
                self.rawcache.insert_query_record(dasquery, header)
                if  srv not in ack_services:
                    ack_services.append(srv)
        if  not ack_services:
            ack_services = services
        if  dasquery.query.find('records ') != -1:
            srv_status = True # skip DAS queries w/ records request
        # create das record with initial expire tstamp 2 min in a future
        # it should be sufficient for processing data-srv records
        expire = time.time()+2*60
        header = dasheader("das", dasquery, expire, api='das_core',
                services=dict(das=ack_services))
        header['lookup_keys'] = []
        self.rawcache.insert_query_record(dasquery, header)
        das_timer('das_record', self.verbose)
        return ack_services

    def call(self, query, **kwds):
        """
        Top level DAS api which execute a given query using underlying
        data-services. It follows the following steps:

            - parse input query
            - identify data-sercices based on selection keys
              and where clause conditions
            - construct DAS workflow and execute data-service 
              API calls. At this step individual 
              data-services store results into DAS cache.

        Return status 0/1 depending on success of the calls, can be
        used by workers on cache server.

        kwds is provided for compatibility with web layer, e.g. it
        may invoke this method with additional pid parameter.
        """
        def update_das_query(dasquery, status, reason=None):
            "Update DAS query record with given status and reason"
            self.rawcache.update_query_record(dasquery, status, reason=reason)
            self.rawcache.add_to_record(\
                    dasquery, {'das.timer': get_das_timer()}, system='das')
            # make sure that das record is updated, we use 7 iteration which
            # sum up into 1 minute to cover default syncdelay value of mongo
            # server (in a future it would be better to find programatically
            # this syncdelay value, but it seems pymongo driver does not
            # provide any API for it.
            for idx in xrange(1, 7):
                spec = {'qhash':dasquery.qhash, 'das.system':['das']}
                res = self.rawcache.col.find_one(spec)
                if  res:
                    dbstatus = res.get('das', {}).get('status', None)
                    if  dbstatus == status:
                        break
                    msg = 'qhash %s, das.status=%s, status=%s, wait for update' \
                            % (dasquery.qhash, dbstatus, status)
                    print(dastimestamp('DAS WARNING'), msg)
                time.sleep(idx*idx)
                self.rawcache.update_query_record(dasquery, status, reason=reason)

        self.logger.info('input query=%s' % query)
        das_timer('DASCore::call', self.verbose)
        if  isinstance(query, object) and hasattr(query, '__class__')\
            and query.__class__.__name__ == 'DASQuery':
            dasquery = query
        else:
            dasquery = DASQuery(query)
        for col in ['merge', 'cache']:
            self.rawcache.remove_expired(dasquery, col)
        query  = dasquery.mongo_query
        spec   = query.get('spec')
        fields = query.get('fields')
        if  fields == ['records']:
            msg = 'look-up all records in cache'
            self.logger.info(msg)
            return 'in cache'
        if  spec == dict(records='*'):
            self.logger.info("look-up everything in cache")
            return 'in cache'
        for record in self.rawcache.find_specs(dasquery):
            status = record['das']['status']
            msg = 'found query %s in cache, status=%s\n' \
                        % (record['query'], status)
            self.logger.info(msg)
            print(dastimestamp('DAS INFO'), msg)
            return status

        self.logger.info(dasquery)
        das_timer('das_record', self.verbose)
        services = self.insert_query_records(dasquery)
        if  not services:
            msg = 'unable to locate data-services to fulfill this request'
            msg += ', will iterate over all registered services'
            print(dastimestamp('DAS WARNING '), dasquery, msg)
            services = dasquery.services if dasquery.services else self.systems
        try:
            if  self.multitask:
                jobs = []
                for srv in sorted(services):
                    jobs.append(self.taskmgr.spawn(self.worker, srv, dasquery))
                self.taskmgr.joinall(jobs)
            else:
                for srv in services:
                    self.worker(srv, dasquery)
        except Exception as exc:
            print_exc(exc)
            return 'fail'
        self.logger.info('\n##### merging ######\n')
        update_das_query(dasquery, 'merging')
        das_timer('merge', self.verbose)
        self.rawcache.merge_records(dasquery)
        das_timer('merge', self.verbose)
        # check if we have service records and properly setup status
        self.logger.info('\n##### check services ######\n')
        das_services = self.rawcache.check_services(dasquery)
        reason = ''
        status = 'ok'
        if  not das_services:
            if  'records' in dasquery.query:
                status = 'ok' # keep status ok for 'records' queries
            else:
                reason = 'no data records found in DAS cache'
                status = 'fail'
                print(dastimestamp('DAS ERROR '), dasquery, reason)
        update_das_query(dasquery, status, reason)
        das_timer('DASCore::call', self.verbose)
        return status

    def processing_time(self, dasquery):
        "Look-up and return DAS query processing time"
        query_record = self.rawcache.find(dasquery)
        if  query_record:
            das = query_record.get('das', None)
            if  isinstance(das, dict):
                ctime = das.get('ctime', [])
                if  ctime:
                    return ctime[-1]-ctime[0]
        return None

    def nresults(self, dasquery, coll='merge'):
        """
        Return total number of results (count) for provided query
        Code should match body of get_from_cache method.
        """
        fields = dasquery.mongo_query.get('fields', None)
        if  dasquery.mapreduce:
            result = self.rawcache.map_reduce(dasquery.mapreduce, dasquery)
            return len([1 for _ in result])
        elif dasquery.aggregators:
            return len(dasquery.aggregators)
        return self.rawcache.nresults(dasquery, coll)

    def apilist(self, dasquery):
        "Return list of APIs answer given das query"
        return self.rawcache.apilist(dasquery)

    def incache(self, dasquery, coll='merge'):
        """
        Answer the question if given query in DAS cache or not
        """
        return self.rawcache.incache(dasquery, collection=coll)

    def get_from_cache(self, dasquery, idx=0, limit=0, collection='merge'):
        """
        Look-up results from the merge cache and yield them for
        further processing.
        """
        das_timer('DASCore::get_from_cache', self.verbose)
        msg = 'col=%s, query=%s, idx=%s, limit=%s'\
                % (collection, dasquery, idx, limit)
        self.logger.info(msg)

        fields  = dasquery.mongo_query.get('fields', None)

        if  dasquery.mapreduce:
            res = self.rawcache.map_reduce(dasquery.mapreduce, dasquery)
        elif dasquery.aggregators:
            # extract das information from rawcache
            rows  = self.rawcache.get_from_cache(\
                    dasquery, collection=collection)
            first = next(rows)
            sinfo = das_sinfo(first)
            # to perform aggregation we need:
            # - loop over all aggregator functions
            # - loop over all data-services
            # - loop over all APIs within a data-services
            # the code below does that, it applies aggregator
            # to selected (based on key/srv/api) records
            res = []
            _id = 0
            time0  = time.time()
            expire = 300 # min expire
            for func, key in dasquery.aggregators:
                afunc = getattr(das_aggregator, 'das_%s' % func)
                found = False
                for srv, apis, in sinfo.items():
                    for api in apis:
                        rows  = self.rawcache.get_from_cache(\
                                dasquery, collection=collection)
                        gen   = api_rows(rows, api)
                        data  = afunc(key, gen)
                        ctime = time.time() - time0
                        das   = dasheader(srv, dasquery, expire, api=api,
                                ctime=ctime)
                        if  isinstance(data, dict) and data['value'] != 'N/A':
                            aggr = {'_id':_id, 'function': func,
                                    'key': key, 'result': data}
                            aggr.update(das)
                            res.append(aggr)
                            _id += 1
                            found = True
                if  not found: # when we got nothing add empty result record
                    empty = {'value':'N/A'}
                    ctime = time.time() - time0
                    das = dasheader('das', dasquery, expire, api='das_core',
                            ctime=ctime)
                    rec = {'_id':0, 'function':func, 'key':key, 'result':empty}
                    rec.update(das)
                    res.append(rec)
        else:
            res = self.rawcache.get_from_cache(dasquery, idx, limit, \
                    collection=collection)
        # we assume that all records from single query will have
        # identical structure, therefore it will be sufficient to update
        # keylearning DB only with first record
        count = 0
        for row in res:
            if  not count:
                self.keylearning.add_record(dasquery, row)
            fix_times(row)
            yield row
            count += 1
        das_timer('DASCore::get_from_cache', self.verbose)
