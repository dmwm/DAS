#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0703

"""
Abstract interface for DAS service
"""
__revision__ = "$Id: abstract_service.py,v 1.94 2010/04/30 16:39:50 valya Exp $"
__version__ = "$Revision: 1.94 $"
__author__ = "Valentin Kuznetsov"

# system modules
import time
import DAS.utils.jsonwrapper as json

# DAS modules
from DAS.utils.ddict import DotDict
from DAS.utils.utils import getarg, genkey, print_exc
from DAS.utils.utils import row2das, make_headers, get_key_cert
from DAS.utils.utils import xml_parser, json_parser
from DAS.utils.utils import yield_rows
from DAS.core.das_mongocache import compare_specs, encode_mongo_query
from DAS.utils.url_utils import getdata
from DAS.utils.das_db import db_gridfs, parse2gridfs
from DAS.core.das_ql import das_special_keys
from DAS.core.das_core import dasheader
from DAS.utils.task_manager import TaskManager, PluginTaskManager

class DASAbstractService(object):
    """
    Abstract class describing DAS service. It initialized with a name who
    is used to identify service parameters from DAS configuration file.
    Those parameters are keys, verbosity level, URL of the data-service.
    """
    def __init__(self, name, config):
        self.name = name
        try:
            self.verbose      = config['verbose']
            self.logger       = config['logger']
            self.dasmapping   = config['dasmapping']
            self.analytics    = config['dasanalytics']
            self.write2cache  = config.get('write_cache', True)
            self.multitask    = config['das'].get('multitask', True)
            self.error_expire = config['das'].get('error_expire', 300) 
            if  config.has_key('dbs'):
                self.dbs_global = config['dbs'].get('dbs_global_instance', None)
            else:
                self.dbs_global = None
            dburi             = config['mongodb']['dburi']
            engine            = config.get('engine', None)
            self.gfs          = db_gridfs(dburi)
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
            if  engine:
                self.taskmgr      = PluginTaskManager(engine)
                self.taskmgr.subscribe()
            else:
                self.taskmgr      = TaskManager()
        else:
            self.taskmgr = None

        self.map        = {}   # to be defined by data-service implementation
        self._keys      = None # to be defined at run-time in self.keys
        self._params    = None # to be defined at run-time in self.parameters
        self._notations = {}   # to be defined at run-time in self.notations

        msg = 'DASAbstractService::__init__ %s' % self.name
        self.logger.info(msg)
        # define internal cache manager to put 'raw' results into cache
        if  config.has_key('rawcache') and config['rawcache']:
            self.localcache   = config['rawcache']
        else:
            msg = 'Undefined rawcache, please check your configuration'
            raise Exception(msg)

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
        for api, params in self.map.items():
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
        for api, params in self.map.items():
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
                api = row['api']
                map = row['map']
                notation = row['notation']
                if  self._notations.has_key(api):
                    self._notations[api].update({notation:map})
                else:
                    self._notations[api] = {notation:map}
        return self._notations

    def getdata(self, url, params, expire, headers=None, post=None):
        """URL call wrapper"""
        if  url.find('https:') != -1:
            return getdata(url, params, headers, expire, post,
                            self.error_expire, self.verbose, self.ckey, self.cert)
        else:
            return getdata(url, params, headers, expire, post,
                            self.error_expire, self.verbose)

    def call(self, query):
        """
        Invoke service API to execute given query.
        Return results as a collect list set.
        """
        msg = 'DASAbstractService::%s::call(%s)' % (self.name, query)
        self.logger.info(msg)

        # check the cache for records with given query/system
        enc_query = encode_mongo_query(query)
        qhash = genkey(enc_query)
        dasquery = {'spec': {'das.qhash': qhash, 'das.system': self.name}, 
                    'fields': None}
        if  self.localcache.incache(query=dasquery, collection='cache'):
            msg  = "DASAbstractService::%s found records in local cache."\
                  % self.name
            msg += "Update analytics."
            self.logger.info(msg)
            self.analytics.update(self.name, query)
            return
        # ask data-service api to get results, they'll be store them in
        # cache, so return at the end what we have in cache.
        result = self.api(query)

    def write_to_cache(self, query, expire, url, api, args, result, ctime):
        """
        Write provided result set into DAS cache. Update analytics
        db appropriately.
        """
        if  not self.write2cache:
            return
        self.analytics.add_api(self.name, query, api, args)
        msg  = 'DASAbstractService::%s::add_api added to Analytics DB' \
                % self.name
        msg += ' query=%s, api=%s, args=%s' % (query, api, args)
        self.logger.debug(msg)
        header  = dasheader(self.name, query, expire, api, url, ctime)
        header['lookup_keys'] = self.lookup_keys(api)

        # check that apicall record is present in analytics DB
        qhash = genkey(encode_mongo_query(query))
        self.analytics.insert_apicall(self.name, query, 
                                      url, api, args, expire)

        # update the cache
        self.localcache.update_cache(query, result, header)

        msg  = 'DASAbstractService::%s cache has been updated,\n' \
                % self.name
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

    def pass_apicall(self, query, url, api, api_params):
        """
        Filter provided apicall wrt existing apicall records in Analytics DB.
        """
        self.analytics.remove_expired()
        msg  = 'DBSAbstractService::pass_apicall, %s, API=%s, args=%s'\
        % (self.name, api, api_params)
        for row in self.analytics.list_apicalls(url=url, api=api):
            input_query = {'spec':api_params}
            exist_query = {'spec':row['apicall']['api_params']}
            if  compare_specs(input_query, exist_query):
                msg += '\nwill re-use existing api call with args=%s, query=%s'\
                % (row['apicall']['api_params'], exist_query)
                self.logger.info(msg)
                try:
                    # update DAS cache with empty result set
                    args = self.inspect_params(api, api_params)
                    cond   = {'das.qhash': row['apicall']['qhash']}
                    record = self.localcache.col.find_one(cond)
                    if  record and record.has_key('das') and \
                        record['das'].has_key('expire'):
                        expire = record['das']['expire']
                        self.write_to_cache(\
                                query, expire, url, api, args, [], 0)
                except Exception as exc:
                    print_exc(exc)
                    msg  = 'DASAbstractService::pass_apicall\n'
                    msg += 'failed api %s\n' % api
                    msg += 'input query %s\n' % input_query
                    msg += 'existing query %s\n' % exist_query
                    msg += 'Unable to look-up existing query and extract '
                    msg += 'expire timestamp'
                    raise Exception(msg)
                return False
        return True

    def lookup_keys(self, api):
        """
        Return look-up keys of data output for given data-service API.
        """
        lkeys = []
        for key in self.map[api]['keys']:
            for lkey in self.dasmapping.lookup_keys(self.name, key, api=api):
                if  lkey not in lkeys:
                    lkeys.append(lkey)
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
#                        maxval = int(val) - 1
                        maxval = int(val)
                        args[key] = maxval
                    elif oper == '$lte':
                        maxval = int(val)
                        args[key] = maxval
                    elif oper == '$gt':
#                        minval = int(val) + 1
                        minval = int(val)
                        args[key] = minval
                    elif oper == '$gte':
                        minval = int(val)
                        args[key] = minval
                    else:
                        msg  = 'DASAbstractService::inspect_params, API=%s'\
                                % api
                        msg += ' does not support operator %s' % oper
                        raise Exception(msg)
        return args

    def get_notations(self, api):
        """Return notations used for given API"""
        notationmap = self.notations()
        if  not notationmap:
            return {}
        notations = {}
        if  notationmap.has_key(''):
            notations = dict(notationmap['']) # notations applied to all APIs
            if  notationmap.has_key(api): # overwrite the one for provided API
                notations.update(notationmap[api])
        return notations

    def parser(self, query, dformat, data, api):
        """
        DAS data parser. Input parameters:

        - *query* input DAS query
        - *dformat* is a data format, e.g. XML, JSON
        - *data* is a data source, either file-like object or
          actual data
        - *api* is API name
        """
        msg  = "DASAbstractService::%s::parser, api=%s, format=%s " \
                % (self.name, api, dformat)
        self.logger.info(msg)
        prim_key  = self.dasmapping.primary_key(self.name, api)
        notations = self.get_notations(api)
        apitag    = self.dasmapping.apitag(self.name, api)
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
                    self.analytics.update_apicall(query, das_dict)
                if  apitag and row.has_key(apitag):
                    row = row[apitag]
                if  isinstance(row, list):
                    for item in row:
                        if  item.has_key(prim_key):
                            counter += 1
                            yield item
                        else:
                            counter += 1
                            yield {prim_key:item}
                else:
                    if  row.has_key(prim_key):
                        counter += 1
                        yield row
                    else:
                        counter += 1
                        yield {prim_key:row}
        else:
            msg = 'Unsupported data format="%s", API="%s"' % (dformat, api)
            raise Exception(msg)
        msg  = "DASAbstractService::%s::parser, api=%s, format=%s " \
                % (self.name, api, dformat)
        msg += "prim_key=%s yield %s rows" \
                % (prim_key, counter)
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
            elif  row.has_key(prim_key):
                if  row[prim_key].has_key(prim_key):
                    yield row[prim_key] # remapping may create nested dict
                else:
                    yield row
            else:
                yield {prim_key:row}
        msg = "DASAbstractService::%s::translator yield %s rows" \
                % (self.name, count)
        self.logger.debug(msg)

    def set_misses(self, query, api, genrows):
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

        spec  = query['spec']
        skeys = spec.keys()
        row   = genrows.next()
        ddict = DotDict(row)
        keys2adjust = []
        for key in spec.keys():
            val = ddict.get(key)
            if  spec[key] != val and key not in keys2adjust:
                keys2adjust.append(key)
        msg   = "DASAbstractService::%s::set_misses, adjust keys %s"\
                % (self.name, keys2adjust)
        self.logger.debug(msg)
        count = 1
        if  keys2adjust:
            # adjust of the rows
            for row in yield_rows(row, genrows):
                ddict = DotDict(row)
                pval  = ddict.get(map_key)
                if  isinstance(pval, dict) and pval.has_key('error'):
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
                        value.has_key('$in'): # we got a range {'$in': []}
                            value = value['$in']
                        elif isinstance(value, dict) and \
                        value.has_key('$lte') and value.has_key('$gte'):
                            # we got a between range
                            min = value['$gte']
                            max = value['$lte']
                            value = [min, max]
                        else: 
                            value = json.dumps(value) 
                    elif existing_value and value != existing_value:
                        # we got proximity results
                        if  ddict.has_key('proximity'):
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
        msg   = "DASAbstractService::%s::set_misses yield %s rows"\
                % (self.name, count)
        self.logger.debug(msg)
            
    def api(self, query):
        """
        Data service api method, can be defined by data-service class.
        It parse input query and invoke appropriate data-service API
        call. All results are stored into the DAS cache along with
        api call inserted into Analytics DB.
        """
        self.logger.info('DASAbstractService::%s::api(%s)' \
                % (self.name, query))
        result  = False
        genrows = self.apimap(query)
        if  not genrows:
            return
        jobs = []
        for url, api, args, dformat, expire in genrows:
            if  self.multitask:
                jobs.append(self.taskmgr.spawn(self.apicall, \
                            query, url, api, args, dformat, expire))
            else:
                self.apicall(query, url, api, args, dformat, expire)
        if  self.multitask:
            self.taskmgr.joinall(jobs)

    def apicall(self, query, url, api, args, dformat, expire):
        """
        Data service api method, can be defined by data-service class.
        It parse input query and invoke appropriate data-service API
        call. All results are stored into the DAS cache along with
        api call inserted into Analytics DB.
        """
        try:
            mkey    = self.dasmapping.primary_mapkey(self.name, api)
            args    = self.inspect_params(api, args)
            time0   = time.time()
            self.analytics.insert_apicall(self.name, query, url, 
                                          api, args, expire)
            headers = make_headers(dformat)
            data, expire = self.getdata(url, args, expire, headers)
            rawrows = self.parser(query, dformat, data, api)
            dasrows = self.translator(api, rawrows)
            dasrows = self.set_misses(query, api, dasrows)
            ctime   = time.time() - time0
            self.write_to_cache(query, expire, url, api, args, 
                    dasrows, ctime)
        except Exception as exc:
            msg  = 'Fail to process: url=%s, api=%s, args=%s' \
                    % (url, api, args)
            print msg
            print_exc(exc)

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

    def apimap(self, query):
        """
        Analyze input query and yield url, api, args, format, expire
        for further processing.
        """
        cond  = getarg(query, 'spec', {})
        instance = query.get('instance', self.dbs_global)
        skeys = getarg(query, 'fields', [])
        if  not skeys:
            skeys = []
        self.logger.info("\n")
        for api, value in self.map.items():
            expire = value['expire']
            format = value['format']
            url    = self.adjust_url(value['url'], instance)
            args   = dict(value['params']) # make new copy, since we'll adjust
            wild   = value.get('wild_card', '*')
            found  = 0
            for key, val in cond.items():
                # check if key is a special one
                if  key in das_special_keys():
                    found += 1
                # check if keys from conditions are accepted by API.
                if  self.dasmapping.check_dasmap(self.name, api, key, val):
                    # need to convert key (which is daskeys.map) into
                    # input api parameter
                    for apiparam in \
                        self.dasmapping.das2api(self.name, key, val, api):
                        if  args.has_key(apiparam):
                            args[apiparam] = val
                            found += 1
                else:
                    found = 0
                    break # condition key does not map into API params
            self.adjust_params(api, args, instance)
            if  not found:
                msg = "--- %s rejects API %s, parameters don't match"\
                        % (self.name, api)
                self.logger.info(msg)
                msg = 'args=%s' % args
                self.logger.debug(msg)
                continue
            if 'optional' in args.values():
                for key, val in args.items():
                    if  val == 'optional':
                        del args[key]
            # check that there is no "required" parameter left in args,
            # since such api will not work
            if 'required' in args.values():
                msg = '--- %s rejects API %s, parameter is required'\
                        % (self.name, api)
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
            # check if analytics db has a similar API call
#TODO: I commented this part on 2011-02-03 since I found that das record can be
# wiped out sooner then data/analytics records. It carries the smallest expire 
# timestamp among all, so checking analytics can lead to the case when records
# in analytics are present, while das record is gone. As consequence it leads to
# situation when I don't get any records during merging step (since new data
# records are not created due to this miss)
#            if  not self.pass_apicall(query, url, api, args):
#                continue

#            self.adjust_params(api, args)

            prim_key = self.dasmapping.primary_key(self.name, api)
            if  prim_key not in skeys:
                msg = "--- %s rejects API %s, primary_key %s is not selected"\
                        % (self.name, api, prim_key)
                self.logger.info(msg)
                continue

            msg = '+++ %s passes API %s' % (self.name, api)
            self.logger.info(msg)
            msg = 'args=%s' % args
            self.logger.debug(msg)

            msg  = "DASAbstractService::apimap yield "
            msg += "system ***%s***, url=%s, api=%s, args=%s, format=%s, " \
                % (self.name, url, api, args, format)
            msg += "expire=%s, wild_card=%s" \
                % (expire, wild)
            self.logger.debug(msg)

            yield url, api, args, format, expire
