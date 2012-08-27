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
from DAS.utils.task_manager import TaskManager, PluginTaskManager
from DAS.utils.das_timer import das_timer, get_das_timer
from DAS.utils.global_scope import SERVICES

# DAS imports
import DAS.core.das_aggregators as das_aggregator

def dasheader(system, dasquery, expire, api=None, url=None, ctime=None):
    """
    Return DAS header (dict) wrt DAS specifications, see
    https://twiki.cern.ch/twiki/bin/view/CMS/DMWMDataAggregationService#DAS_data_service_compliance
    """
    if  not api:
        dasdict = dict(system=[system], timestamp=time.time(),
                    expire=expire_timestamp(expire),
                    status="requested")
    else:
        dasdict = dict(system=[system], timestamp=time.time(),
                    url=[url], ctime=[ctime],
                    expire=expire_timestamp(expire), urn=[api],
                    api=[api], status="requested")
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
        if  dasquery and dasquery.mongo_query.has_key('fields'):
            fields = dasquery.mongo_query['fields']
            if  fields and isinstance(fields, list) and 'queries' in fields:
                return 'ok', dasquery.qhash
        status = 0
        record = self.rawcache.find(dasquery)
        try:
            if  record and record.has_key('das') and \
                record['das'].has_key('status'):
                status = record['das']['status']
                return status, record['qhash']
        except:
            pass

        similar_dasquery = self.rawcache.similar_queries(dasquery)
        if  similar_dasquery:
            record = self.rawcache.find(similar_dasquery)
            if  record and record.has_key('das') and \
                record['das'].has_key('status'):
                similar_query_status = record['das']['status']
                return similar_query_status, record['qhash']
        return status, 0

    def worker(self, srv, dasquery):
        """Main worker function which calls data-srv call function"""
        self.logger.info('##### %s ######\n' % srv)
        das_timer(srv, self.verbose)
        getattr(getattr(self, srv), 'call')(dasquery)
        das_timer(srv, self.verbose)

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
        services = []
        if  isinstance(query, object) and hasattr(query, '__class__')\
            and query.__class__.__name__ == 'DASQuery':
            dasquery = query
        else:
            dasquery = DASQuery(query, mongoparser=self.mongoparser)
        if  add_to_analytics:
            dasquery.add_to_analytics()
        query = dasquery.mongo_query
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
        similar_dasquery = self.rawcache.similar_queries(dasquery)
        if  similar_dasquery:
            for record in self.rawcache.find_specs(similar_dasquery):
                if  record:
                    try:
                        status = record['das']['status']
                    except:
                        status = 'N/A'
                        msg = 'Fail to look-up das.status, record=%s' % record
                        self.logger.info(msg)
                msg  = 'found SIMILAR query in cache,'
                msg += 'query=%s, status=%s\n' % (record['query'], status)
                self.logger.info(msg)
                return status

        self.logger.info(dasquery)
        params = dasquery.params()
        if  not services:
            services = params['services']
        self.logger.info('services = %s' % services)
        das_timer('das_record', self.verbose)
        # initial expire tstamp 1 day (long enough to be overwriten by data-srv)
        expire = expire_timestamp(time.time()+1*24*60*60)
        header = dasheader("das", dasquery, expire)
        header['lookup_keys'] = []
        self.rawcache.insert_query_record(dasquery, header)
        das_timer('das_record', self.verbose)
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
            res = []
            _id = 0
            for func, key in dasquery.aggregators:
                rows = self.rawcache.get_from_cache(\
                        dasquery, collection=collection)
                data = getattr(das_aggregator, 'das_%s' % func)(key, rows)
                res += \
                [{'_id':_id, 'function': func, 'key': key, 'result': data}]
                _id += 1
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

