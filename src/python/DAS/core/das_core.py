#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0703,R0912,R0913,R0914,R0915,W0702,R0902

"""
Core class for Data Aggregation Service (DAS) framework.
It performs the following tasks:

- registers data-services found in DAS configuration file (das.cfg).
- invoke data-service APIs
- merge results based on common keys
- pass results to presentation layer (CLI or WEB)
"""

__author__ = "Valentin Kuznetsov"

# system modules
import os
import time
import itertools

# DAS modules
from DAS.core.das_ql import das_operators, das_special_keys
from DAS.core.das_query import DASQuery
from DAS.core.das_parser import ql_manager
from DAS.core.das_mapping_db import DASMapping
from DAS.core.das_analytics_db import DASAnalytics
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
        services=[]):
    """
    Return DAS header (dict) wrt DAS specifications, see
    https://twiki.cern.ch/twiki/bin/view/CMS/DMWMDataAggregationService#DAS_data_service_compliance
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
    if  system == ['das']:
        dasdict.update({"services": services})
    return dict(das=dasdict)

class DASCore(object):
    """
    DAS core class.
    """
    def __init__(self, config=None, debug=None, 
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
        if  not multitask: # explicitly call DASCore ctor, e.g. in analytics
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

        self.analytics = DASAnalytics(dasconfig)
        dasconfig['dasanalytics'] = self.analytics

        self.mongoparser = ql_manager(dasconfig)
        dasconfig['mongoparser'] = self.mongoparser

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
        dasquery = DASQuery(query, mongoparser=self.mongoparser)
        dasquery.add_to_analytics()
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
                newquery = DASQuery(dict(fields=[key], spec=query['spec']),
                                        mongoparser=self.mongoparser)
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
        reason = None
        for col in ['merge', 'cache']:
            self.rawcache.remove_expired(col)
        if  dasquery and dasquery.mongo_query.has_key('fields'):
            fields = dasquery.mongo_query['fields']
            if  fields and isinstance(fields, list) and 'queries' in fields:
                return 'ok', reason
        record = self.rawcache.find(dasquery)
        try:
            if  record and record.has_key('das') and \
                record['das'].has_key('status'):
                status = record['das']['status']
                return status, record['das'].get('reason', reason)
        except:
            pass
        return status, reason

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
        services = []
        if  dasquery.mongo_query.has_key('system'):
            system = query['system']
            if  isinstance(system, str) or isinstance(system, unicode):
                services = [system]
            elif isinstance(system, list):
                services = system
            else:
                msg = 'Unsupported system=%s type=%s in DAS query' \
                        % (system, type(system))
                raise Exception(msg)
        if  not services:
            services = dasquery.params()['services']
        self.logger.info('Potential services = %s' % services)
        expire = 7*24*60*60 # 7 days, long enough to be overwriten by data-srv
        header = dasheader("das", dasquery, expire, api='das_core',
                services=services)
        header['lookup_keys'] = []
        self.rawcache.insert_query_record(dasquery, header)
        das_timer('das_record', self.verbose)
        # get list of URI which can answer this query
        if  services:
            ack_services = []
            for srv in services:
                gen = getattr(getattr(self, srv), 'apimap')(dasquery)
                for url, api, args, iformat, expire in gen:
                    header = dasheader(srv, dasquery, expire, api, url, ctime=0)
                    self.rawcache.insert_query_record(dasquery, header)
                    ack_services.append(srv)
        if  services and not ack_services:
            srv_status = False
        else:
            srv_status = set(services) & set(ack_services) == set(ack_services)
        if  dasquery.query.find('records ') != -1:
            srv_status = True # skip DAS queries w/ records request
        return ack_services, srv_status

    def call(self, query, add_to_analytics=True, **kwds):
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
        for col in ['merge', 'cache']:
            self.rawcache.remove_expired(col)
        self.logger.info('input query=%s' % query)
        das_timer('DASCore::call', self.verbose)
        if  isinstance(query, object) and hasattr(query, '__class__')\
            and query.__class__.__name__ == 'DASQuery':
            dasquery = query
        else:
            dasquery = DASQuery(query, mongoparser=self.mongoparser)
        if  add_to_analytics:
            dasquery.add_to_analytics()
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
            return status

        self.logger.info(dasquery)
        das_timer('das_record', self.verbose)
        services, srv_status = self.insert_query_records(dasquery)
        if  not srv_status:
            if  services:
                msg = 'fail to acknowledge services %s' % services
            else:
                msg = 'unable to locate data-services to fulfill this request'
            print dastimestamp('DAS ERROR '), dasquery, msg
            self.rawcache.update_query_record(dasquery, 'fail', reason=msg)
            self.rawcache.add_to_record(\
                    dasquery, {'das.timer': get_das_timer()}, system='das')
            return 'fail'
        self.logger.info('Acknowledged services = %s' % services)
        try:
            if  self.multitask:
                jobs = []
                for srv in services:
                    jobs.append(self.taskmgr.spawn(self.worker, srv, dasquery))
                self.taskmgr.joinall(jobs)
            else:
                for srv in services:
                    self.worker(srv, dasquery)
        except Exception as exc:
            print_exc(exc)
            return 'fail'
        self.logger.info('\n##### merging ######\n')
        self.rawcache.update_query_record(dasquery, 'merging')
        das_timer('merge', self.verbose)
        self.rawcache.merge_records(dasquery)
        das_timer('merge', self.verbose)
        self.rawcache.update_query_record(dasquery, 'ok')
        self.rawcache.add_to_record(\
                dasquery, {'das.timer': get_das_timer()}, system='das')
        das_timer('DASCore::call', self.verbose)
        return 'ok'

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
        elif isinstance(fields, list) and 'queries' in fields:
            return len([1 for _ in self.get_queries(dasquery)])
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
            first = rows.next()
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
        elif isinstance(fields, list) and 'queries' in fields:
            res = itertools.islice(self.get_queries(dasquery), idx, idx+limit)
        else:
            res = self.rawcache.get_from_cache(dasquery, idx, limit, \
                    collection=collection)
        for row in res:
            fix_times(row)
            yield row
        das_timer('DASCore::get_from_cache', self.verbose)

    def get_queries(self, dasquery):
        """
        Look-up (popular) queries in DAS analytics/logging db
        """
        das_timer('DASCore::get_queries', self.verbose)
        fields = dasquery.mongo_query.get('fields')
        spec   = dasquery.mongo_query.get('spec')
        if  'popular' in fields:
            res = self.analytics.get_popular_queries(spec)
        else:
            datestamp = spec.get('date')
            if  isinstance(datestamp, dict):
                value = datestamp.get('$in')
                res = \
                self.analytics.list_queries(after=value[0], before=value[1])
            elif isinstance(datestamp, int):
                res = self.analytics.list_queries(after=datestamp)
            elif not datestamp:
                res = self.analytics.list_queries()
            else:
                msg = 'Unsupported date value: %s' % datestamp
                raise Exception(msg)
        for row in res:
            rid = row.pop('_id')
            yield dict(das_query=row, _id=rid)
        das_timer('DASCore::get_queries', self.verbose)

