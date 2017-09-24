#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703,R0912,R0913,R0914,R0902,R0915

"""
Abstract interface for DAS service
"""
from __future__ import print_function
__revision__ = "$Id: abstract_service.py,v 1.94 2010/04/30 16:39:50 valya Exp $"
__version__ = "$Revision: 1.94 $"
__author__ = "Valentin Kuznetsov"

# system modules
import sys
import json
import time
import itertools

# python 3
if  sys.version.startswith('3.'):
    unicode = str

# DAS modules
from DAS.utils.ddict import DotDict
from DAS.utils.utils import getarg, print_exc
from DAS.utils.utils import row2das, make_headers, get_key_cert
from DAS.utils.utils import xml_parser, json_parser
from DAS.utils.utils import yield_rows, delete_keys
from DAS.utils.query_utils import compare_specs
from DAS.utils.url_utils import getdata, close
from DAS.utils.das_db import db_gridfs, parse2gridfs
from DAS.core.das_ql import das_special_keys
from DAS.core.das_core import dasheader
from DAS.core.das_query import DASQuery
from DAS.utils.task_manager import TaskManager, PluginTaskManager
from DAS.utils.logger import PrintManager

class DASAbstractService(object):
    """
    Abstract class describing DAS service. It initialized with a name which
    is used to identify service parameters from DAS configuration file.
    Those parameters are keys, verbosity level, URL of the data-service.
    """
    def __init__(self, name, config):
        self.name = name
        try:
            self.verbose      = config['verbose']
            title             = 'DASAbstactService_%s' % self.name
            self.logger       = PrintManager(title, self.verbose)
            self.dasmapping   = config['dasmapping']
            self.write2cache  = config.get('write_cache', True)
            self.multitask    = config['das'].get('multitask', True)
            self.error_expire = config['das'].get('error_expire', 300) 
            self.dbs_global   = None # to be configured at run time
            self.dburi        = config['mongodb']['dburi']
            engine            = config.get('engine', None)
            self.gfs          = db_gridfs(self.dburi)
        except Exception as exc:
            print_exc(exc)
            raise Exception('fail to parse DAS config')

        # read key/cert info
        try:
            self.ckey, self.cert = get_key_cert()
        except Exception as exc:
            print_exc(exc)
            self.ckey = None
            self.cert = None

        if  self.multitask:
            nworkers = config['das'].get('api_workers', 3)
            thr_weights = config['das'].get('thread_weights', [])
            for system_weight in thr_weights:
                system, weight = system_weight.split(':')
                if  system == self.name:
                    nworkers *= int(weight)
            if  engine:
                thr_name = 'DASAbstractService:%s:PluginTaskManager' % self.name
                self.taskmgr = PluginTaskManager(\
                        engine, nworkers=nworkers, name=thr_name)
                self.taskmgr.subscribe()
            else:
                thr_name = 'DASAbstractService:%s:TaskManager' % self.name
                self.taskmgr = TaskManager(nworkers=nworkers, name=thr_name)
        else:
            self.taskmgr = None

        self.map        = {}   # to be defined by data-service implementation
        self._keys      = None # to be defined at run-time in self.keys
        self._params    = None # to be defined at run-time in self.parameters
        self._notations = {}   # to be defined at run-time in self.notations

        self.logger.info('initialized')
        # define internal cache manager to put 'raw' results into cache
        if  'rawcache' in config and config['rawcache']:
            self.localcache   = config['rawcache']
        else:
            msg = 'Undefined rawcache, please check your configuration'
            raise Exception(msg)

    def services(self):
        """
        Return sub-subsystems used to retrieve data records. It is used
        in dasheader call to setup das.services field. This method can be
        overwritten in sub-classes, otherwise returns dict of service name
        and CMS systems used to retrieve data records.
        """
        return {self.name:[self.name]}

    def version(self):
        """Return data-services version, should be implemented in sub-classes"""
        return ''

    def keys(self):
        """
        Return service keys
        """
        if  self._keys:
            return self._keys
        srv_keys = []
        for _api, params in self.map.items():
            for key in params['keys']:
                if  not key in srv_keys:
                    srv_keys.append(key)
        self._keys = srv_keys
        return srv_keys

    def parameters(self):
        """
        Return mapped service parameters
        """
        if  self._params:
            return self._params
        srv_params = []
        for _api, params in self.map.items():
            for key in params['params']:
                param_list = self.dasmapping.api2das(self.name, key)
                for par in param_list:
                    if  not par in srv_params:
                        srv_params.append(par)
        self._params = srv_params
        return srv_params

    def notations(self):
        """
        Return a map of system notations.
        """
        if  self._notations:
            return self._notations
        for _, rows in self.dasmapping.notations(self.name).items():
            for row in rows:
                api  = row['api']
                nmap = row['rec_key']
                notation = row['api_output']
                if  api in self._notations:
                    self._notations[api].update({notation:nmap})
                else:
                    self._notations[api] = {notation:nmap}
        return self._notations

    def getdata(self, url, params, expire, headers=None, post=None):
        """URL call wrapper"""
        if  url.find('https:') != -1:
            return getdata(url, params, headers, expire, post,
                self.error_expire, self.verbose, self.ckey, self.cert,
                system=self.name)
        else:
            return getdata(url, params, headers, expire, post,
                self.error_expire, self.verbose, system=self.name)

    def call(self, dasquery):
        """
        Invoke service API to execute given query.
        Return results as a collect list set.
        """
        self.logger.info(dasquery)
        # check the cache for records with given query/system
        res = self.localcache.incache(dasquery,
                                      collection='cache',
                                      system=self.name)
        if  res:
            msg  = "found records in local cache"
            self.logger.info(msg)
            return
        # ask data-service api to get results, they'll be store them in
        # cache, so return at the end what we have in cache.
        self.api(dasquery)

    def write_to_cache(self, dasquery, expire, url, api, args, gen, ctime):
        """
        Write provided result set into DAS cache.
        """
        if  not self.write2cache:
            return

        # before going to cache we should check/set possible misses, e.g.
        # primary key when error is thrown
        result = self.set_misses(dasquery, api, gen)

        # update the cache
        header = dasheader(self.name, dasquery, expire, api, url,
                services=self.services())
        header['lookup_keys'] = self.lookup_keys(api)
        header['prim_key'] = self.dasmapping.primary_mapkey(self.name, api)
        header['ctime'] = ctime
        system = self.name
        self.localcache.update_cache(dasquery, result, header, system)

        msg  = 'cache has been updated,\n'
        self.logger.debug(msg)

    def adjust_params(self, api, kwds, instance=None):
        """
        Data-service specific parser to adjust parameters according to
        its specifications. For example, DQ service accepts a string
        of parameters, rather parameter set, while DBS2 can reuse
        some parameters for different API, e.g. I can use dataset path
        to pass to listPrimaryDatasets as primary_dataset pattern.
        """
        pass

    def lookup_keys(self, api):
        """
        Return look-up keys of data output for given data-service API.
        """
        lkeys = self.dasmapping.lookup_keys(self.name, api)
        return [{api:lkeys}]

    def inspect_params(self, api, args):
        """
        Perform API parameter inspection. Check if API accept a range
        of parameters, etc.
        """
        for key, value in args.items():
            if  isinstance(value, dict):
                minval = None
                maxval = None
                for oper, val in value.items():
                    if  oper == '$in':
                        minval = int(val[0])
                        maxval = int(val[-1])
                        args[key] = range(minval, maxval)
                    elif oper == '$lt':
                        maxval = int(val)
                        args[key] = maxval
                    elif oper == '$lte':
                        maxval = int(val)
                        args[key] = maxval
                    elif oper == '$gt':
                        minval = int(val)
                        args[key] = minval
                    elif oper == '$gte':
                        minval = int(val)
                        args[key] = minval
                    else:
                        msg = '%s does not support operator %s' % (api, oper)
                        raise Exception(msg)
        return args

    def get_notations(self, api):
        """Return notations used for given API"""
        notationmap = self.notations()
        if  not notationmap:
            return {}
        notations = {}
        if  '' in notationmap:
            notations = dict(notationmap['']) # notations applied to all APIs
            if  api in notationmap: # overwrite the one for provided API
                notations.update(notationmap[api])
        return notations

    def parser(self, dasquery, dformat, data, api):
        """
        DAS data parser. Input parameters:

        - *query* input DAS query
        - *dformat* is a data format, e.g. XML, JSON
        - *data* is a data source, either file-like object or
          actual data
        - *api* is API name
        """
        prim_key  = self.dasmapping.primary_key(self.name, api)
        counter   = 0
        if  dformat.lower() == 'xml':
            tags = self.dasmapping.api2daskey(self.name, api)
            gen  = xml_parser(data, prim_key, tags)
            for row in gen:
                counter += 1
                yield row
        elif dformat.lower() == 'json' or dformat.lower() == 'dasjson':
            gen  = json_parser(data, self.logger)
            das_dict = {}
            for row in gen:
                if  dformat.lower() == 'dasjson':
                    for key, val in row.items():
                        if  key != 'results':
                            das_dict[key] = val
                    row = row['results']
                if  isinstance(row, list):
                    for item in row:
                        if  item:
                            if  prim_key in item:
                                counter += 1
                                yield item
                            else:
                                counter += 1
                                yield {prim_key:item}
                else:
                    if  prim_key in row:
                        counter += 1
                        yield row
                    else:
                        counter += 1
                        yield {prim_key:row}
        else:
            msg = 'Unsupported data format="%s", API="%s"' % (dformat, api)
            raise Exception(msg)
        msg  = "api=%s, format=%s " % (api, dformat)
        msg += "prim_key=%s yield %s rows" % (prim_key, counter)
        self.logger.info(msg)

    def translator(self, api, genrows):
        """
        Convert raw results into DAS records. 
        """
        prim_key  = self.dasmapping.primary_key(self.name, api)
        count = 0
        for row in genrows:
            row2das(self.dasmapping.notation2das, self.name, api, row)
            count += 1
            # check for primary key existance, since it can be overriden
            # by row2das. For example DBS3 uses flat namespace, so we
            # override dataset=>name, while dataset still is a primary key
            if  isinstance(row, list):
                yield {prim_key:row}
            elif  prim_key in row:
                if  prim_key in row[prim_key]:
                    yield row[prim_key] # remapping may create nested dict
                else:
                    yield row
            else:
                yield {prim_key:row}
        msg = "yield %s rows" % count
        self.logger.debug(msg)

    def set_misses(self, dasquery, api, genrows):
        """
        Check and adjust DAS records wrt input query. If some of the DAS
        keys are missing, add it with its value to the DAS record.
        """
        # look-up primary key
        prim_key  = self.dasmapping.primary_key(self.name, api)

        # Scan all docs and store those whose size above MongoDB limit into
        # GridFS
        map_key = self.dasmapping.primary_mapkey(self.name, api)
        genrows = parse2gridfs(self.gfs, map_key, genrows, self.logger)

        spec  = dasquery.mongo_query['spec']
        row   = next(genrows)
        ddict = DotDict(row)
        keys2adjust = []
        for key in spec.keys():
            val = ddict.get(key)
            if  spec[key] != val and key not in keys2adjust:
                keys2adjust.append(key)
        msg   = "adjust keys %s" % keys2adjust
        self.logger.debug(msg)
        count = 0
        if  keys2adjust:
            # adjust of the rows
            for row in yield_rows(row, genrows):
                ddict = DotDict(row)
                pval  = ddict.get(map_key)
                if  isinstance(pval, dict) and 'error' in pval:
                    ddict[map_key] = ''
                    ddict.update({prim_key: pval})
                for key in keys2adjust:
                    value = spec[key]
                    existing_value = ddict.get(key)
                    # the way to deal with proximity/patern/condition results
                    if  (isinstance(value, str) or isinstance(value, unicode))\
                        and value.find('*') != -1: # we got pattern
                        if  existing_value:
                            value = existing_value
                    elif isinstance(value, dict) or \
                        isinstance(value, list): # we got condition
                        if  existing_value:
                            value = existing_value
                        elif isinstance(value, dict) and \
                        '$in' in value: # we got a range {'$in': []}
                            value = value['$in']
                        elif isinstance(value, dict) and \
                        '$lte' in value and '$gte' in value:
                            # we got a between range
                            value = [value['$gte'], value['$lte']]
                        else: 
                            value = json.dumps(value) 
                    elif existing_value and value != existing_value:
                        # we got proximity results
                        if  'proximity' in ddict:
                            proximity = DotDict({key:existing_value})
                            ddict['proximity'].update(proximity)
                        else:
                            proximity = DotDict({})
                            proximity[key] = existing_value
                            ddict['proximity'] = proximity
                    else:
                        if  existing_value:
                            value = existing_value
                    ddict[key] = value
                yield ddict
                count += 1
        else:
            yield row
            for row in genrows:
                yield row
                count += 1
        msg   = "yield %s rows" % count
        self.logger.debug(msg)
            
    def api(self, dasquery):
        """
        Data service api method, can be defined by data-service class.
        It parse input query and invoke appropriate data-service API
        call. All results are stored into the DAS cache along with
        api call inserted into Analytics DB.
        """
        self.logger.info(dasquery)
        genrows = self.apimap(dasquery)
        if  not genrows:
            return
        jobs = []
        for url, api, args, dformat, expire in genrows:
            # insert DAS query record for given API
            header = dasheader(self.name, dasquery, expire, api, url)
            self.localcache.insert_query_record(dasquery, header)
            # fetch DAS data records
            if  self.multitask:
                jobs.append(self.taskmgr.spawn(self.apicall, \
                            dasquery, url, api, args, dformat, expire))
            else:
                self.apicall(dasquery, url, api, args, dformat, expire)
        if  self.multitask:
            self.taskmgr.joinall(jobs)

    def apicall(self, dasquery, url, api, args, dformat, expire):
        """
        Data service api method, can be defined by data-service class.
        It parse input query and invoke appropriate data-service API
        call. All results are stored into the DAS cache along with
        api call inserted into Analytics DB.

        We invoke explicitly close call for our datastream instead
        of using context manager since this method as well as
        getdata/parser can be overwritten by child classes.
        """
        datastream  = None
        try:
            args    = self.inspect_params(api, args)
            time0   = time.time()
            headers = make_headers(dformat)
            datastream, expire = self.getdata(url, args, expire, headers)
            self.logger.info("%s expire %s" % (api, expire))
            rawrows = self.parser(dasquery, dformat, datastream, api)
            dasrows = self.translator(api, rawrows)
            ctime   = time.time() - time0
            self.write_to_cache(dasquery, expire, url, api, args,
                    dasrows, ctime)
        except Exception as exc:
            msg  = 'Fail to process: url=%s, api=%s, args=%s' \
                    % (url, api, args)
            print(msg)
            print_exc(exc)
        close(datastream)

    def url_instance(self, url, _instance):
        """
        Virtual method to adjust URL for a given instance,
        must be implemented in service classes
        """
        return url

    def adjust_url(self, url, instance):
        """
        Adjust data-service URL wrt provided instance, e.g.
        DBS carry several instances
        """
        if  instance:
            url = self.url_instance(url, instance)
        return url

    def apimap(self, dasquery):
        """
        Analyze input query and yield url, api, args, format, expire
        for further processing.
        """
        srv   = self.name # get local copy to avoid threading issues
        cond  = getarg(dasquery.mongo_query, 'spec', {})
        instance = dasquery.mongo_query.get('instance', self.dbs_global)
        skeys = getarg(dasquery.mongo_query, 'fields', [])
        if  not skeys:
            skeys = []
        self.logger.info("\n")
        for api, value in self.map.items():
            expire = value['expire']
            iformat = value['format']
            url    = self.adjust_url(value['url'], instance)
            if  not url:
                msg = '--- rejects API %s, no URL' % api
                self.logger.info(msg)
                continue
            args   = dict(value['params']) # make new copy, since we'll adjust
            wild   = value.get('wild_card', '*')
            found  = 0
            # check if input parameters are covered by API
            if  not self.dasmapping.check_api_match(srv, api, cond):
                msg = '--- rejects API %s, does not cover input condition keys' \
                        % api
                self.logger.info(msg)
                continue
            # once we now that API covers input set of parameters we check
            # every input parameter for pattern matching
            for key, val in cond.items():
                # check if keys from conditions are accepted by API
                # need to convert key (which is daskeys.map) into
                # input api parameter
                for apiparam in self.dasmapping.das2api(srv, api, key, val):
                    if  apiparam in args:
                        args[apiparam] = val
                        found += 1
            # VK 20160708, wrong statement, it caused to pass
            # datasets API for query dataset in [path1, path2]
            # I'll leave block here until I test and verify that
            # commented out block will not cause other issues
            #
            # check the case when we only have single condition key
            # and it is the key we look-up
#             if  not found and skeys == [k.split('.')[0] for k in cond.keys()]:
#                 found = 1
            # check if number of keys on cond and args are the same
            if  len(cond.keys()) != found:
                msg = "--- reject API %s, not all condition keys are covered" \
                        % api
                self.logger.info(msg)
                msg = 'args=%s' % args
                self.logger.debug(msg)
                continue
            if  not found:
                msg = "--- rejects API %s, parameters don't match" % api
                self.logger.info(msg)
                msg = 'args=%s' % args
                self.logger.debug(msg)
                continue
            self.adjust_params(api, args, instance)
            # delete args keys whose value is optional
            delete_keys(args, 'optional')
            # check that there is no "required" parameter left in args,
            # since such api will not work
            if 'required' in args.values():
                msg = '--- rejects API %s, parameter is required' % api
                self.logger.info(msg)
                msg = 'args=%s' % args
                self.logger.debug(msg)
                continue
            # adjust pattern symbols in arguments
            if  wild != '*':
                for key, val in args.items():
                    if  isinstance(val, str) or isinstance(val, unicode):
                        val   = val.replace('*', wild)
                    args[key] = val

            # compare query selection keys with API look-up keys
            api_lkeys = self.dasmapping.api_lkeys(srv, api)
            if  set(api_lkeys) != set(skeys):
                msg = "--- rejects API %s, api_lkeys(%s)!=skeys(%s)"\
                        % (api, api_lkeys, skeys)
                self.logger.info(msg)
                continue

            msg = '+++ %s passes API %s' % (srv, api)
            self.logger.info(msg)
            msg = 'args=%s' % args
            self.logger.debug(msg)

            msg  = "yield "
            msg += "system ***%s***, url=%s, api=%s, args=%s, format=%s, " \
                % (srv, url, api, args, iformat)
            msg += "expire=%s, wild_card=%s" \
                % (expire, wild)
            self.logger.debug(msg)

            yield url, api, args, iformat, expire
