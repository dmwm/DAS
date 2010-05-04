#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Abstract interface for DAS service
"""
__revision__ = "$Id: abstract_service.py,v 1.69 2010/02/10 18:59:19 valya Exp $"
__version__ = "$Revision: 1.69 $"
__author__ = "Valentin Kuznetsov"

import re
import time
import types
import urllib
import urllib2
import traceback
import DAS.utils.jsonwrapper as json

from DAS.utils.utils import dasheader, getarg, genkey
from DAS.utils.utils import row2das
from DAS.utils.utils import xml_parser, json_parser
from DAS.core.das_mongocache import compare_specs

from pymongo import DESCENDING, ASCENDING

class DASAbstractService(object):
    """
    Abstract class describing DAS service. It initialized with a name who
    is used to identify service parameters from DAS configuration file.
    Those parameters are keys, verbosity level, URL of the data-service.
    """
    def __init__(self, name, config):
        self.name = name
        try:
            self.verbose     = config['verbose']
            self.logger      = config['logger']
            self.dasmapping  = config['dasmapping']
            self.analytics   = config['dasanalytics']
            self.mongoparser = config['mongoparser']
            self.write2cache = config.get('write_cache', True)
        except:
            traceback.print_exc()
            print config
            raise Exception('fail to parse DAS config')

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

    def getdata(self, url, params, headers=None):
        """
        Invoke URL call and retrieve data from data-service based
        on provided URL and set of parameters. All data will be parsed
        by data-service parsers to provide uniform JSON representation
        for further processing.
        """
        input_params = params
        # if necessary the data-service implementation will adjust parameters,
        # for instance, DQ need to parse the following input
        # Tracker_Global=GOOD&Tracker_Local1=1
        # into the following form
        # [{"Oper": "=", "Name": "Tracker_Global",  "Value": "GOOD"},...]
        self.adjust_params(params)

        # based on provided interface correctly deal with input parameters
        if  self.name == 'dq':
            encoded_data = json.dumps(params)
        else:
            encoded_data = urllib.urlencode(params, doseq=True)
        if  encoded_data:
            url = url + '?' + encoded_data
        msg = 'DASAbstractService::%s::getdata, request url=%s' \
                % (self.name, url)
        self.logger.info(msg)
        req = urllib2.Request(url)
        if  headers:
            for key, val in headers.items():
                req.add_header(key, val)
        if  self.verbose > 1:
            h=urllib2.HTTPHandler(debuglevel=1)
            opener = urllib2.build_opener(h)
            urllib2.install_opener(opener)
        data = urllib2.urlopen(req)
        return data

    def call(self, query):
        """
        Invoke service API to execute given query.
        Return results as a collect list set.
        """
        msg = 'DASAbstractService::%s::call(%s)' \
                % (self.name, query)
        self.logger.info(msg)

        # check the cache for records with given query/system
        qhash = genkey(query)
        dasquery = {'spec': {'das.qhash': qhash, 'das.system': self.name}, 
                    'fields': None}
        if  self.localcache.incache(query=dasquery, collection='cache'):
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
        msg  = 'DASAbstractService::%s Analytics DB has been updated,' \
                % self.name
        self.logger.info(msg)
        header  = dasheader(self.name, query, api, url, args, ctime,
            expire)
        header['lookup_keys'] = self.lookup_keys(api)
        self.localcache.update_cache(query, result, header)
        msg  = 'DASAbstractService::%s cache has been updated,' \
                % self.name
        self.logger.info(msg)
        self.insert_apicall(expire, url, api, args)

    def adjust_params(self, args):
        """
        Data-service specific parser to adjust parameters according to
        specifications.
        """
        pass

    def insert_apicall(self, expire, url, api, api_params):
        """
        Remove obsolete apicall records and
        insert into Analytics DB provided information about API call.
        """
        spec = {'apicall.expire':{'$lt' : int(time.time())}}
        self.analytics.col.remove(spec)
        doc  = dict(sytsem=self.name, url=url, api=api, api_params=api_params,
                        expire=time.time()+expire)
        self.analytics.col.insert(dict(apicall=doc))
        index_list = [('apicall.url', DESCENDING), ('apicall.api', DESCENDING)]
        self.analytics.col.ensure_index(index_list)

    def pass_apicall(self, url, api, api_params):
        """
        Filter provided apicall wrt existing apicall records in Analytics DB.
        """
        spec = {'apicall.expire':{'$lt' : int(time.time())}}
        self.analytics.col.remove(spec)
        spec = {'apicall.url':url, 'apicall.api':api}
        msg  = 'DBSAbstractService::pass_apicall, %s, API=%s, args=%s'\
        % (self.name, api, api_params)
        for row in self.analytics.col.find(spec):
            input_query = {'spec':api_params}
            exist_query = {'spec':row['apicall']['api_params']}
            if  compare_specs(input_query, exist_query):
                msg += '\nwill re-use existing api call with args=%s'\
                % row['apicall']['api_params']
                self.logger.info(msg)
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
        return lkeys

    def inspect_params(self, api, args):
        """
        Perform API parameter inspection. Check if API accept a range
        of parameters, etc.
        """
        for key, value in args.items():
            if  type(value) is types.DictType:
                minval = None
                maxval = None
                for oper, val in value.items():
                    if  oper == '$in':
                        minval = int(val[0])
                        maxval = int(val[-1])
                    elif oper == '$lt':
                        maxval = int(val) - 1
                    elif oper == '$lte':
                        maxval = int(val)
                    elif oper == '$gt':
                        minval = int(val) + 1
                    elif oper == '$gte':
                        minval = int(val)
                    else:
                        msg  = 'DASAbstractService::inspect_params, API=%s'\
                                % api
                        msg += ' does not support operator %s' % oper
                        raise Exception(msg)
                args[key] = range(minval, maxval)
        return args

    def clean_params(self, api, args):
        """
        For some data-services it's easier to call API with parameters
        matching wild-card pattern. For instance, instead of passing 
        site name into SiteDB API, we will call SiteDB API without
        parameters (with wildcard). Must be implemented in sub-classes.
        """
        return args

    def patterns(self, api, args):
        """
        Define how to deal with patterns at API level. Some API accept a 
        star, '*', as pattern parameter, some use another schema, e.g.
        SiteDB don't use '*', and instead it should be drop-off from parameter.
        Must be implemented in sub-classes
        """
        return args

    def get_notations(self, api):
        """Return notations used for given API"""
        notationmap = self.notations()
        if  not notationmap:
            return {}
        notations = dict(notationmap['']) # notations applied to all APIs
        if  notationmap.has_key(api): # overwrite the one for provided API
            notations.update(notationmap[api])
        return notations

    def parser(self, format, data, api, args=None):
        """
        DAS data parser. It accepts:

        - *format* is a data format, e.g. XML, JSON
        - *data* is a data source, either file-like object or
          actual data
        - *api* is API name
        - *args* API input parameters 
        """
        prim_key  = self.dasmapping.primary_key(self.name, api)
        notations = self.get_notations(api)
        apitag    = self.dasmapping.apitag(self.name, api)
        print "\n### parser", prim_key, notations, apitag
        if  format.lower() == 'xml':
            tags = self.dasmapping.api2daskey(self.name, api)
            gen  = xml_parser(data, prim_key, tags)
            for row in gen:
                yield row
        elif format.lower() == 'json':
            gen  = json_parser(data)
            for row in gen:
                if  apitag and row.has_key(apitag):
                    row = row[apitag]
                print "\n###row\n", row
                if  type(row) is types.ListType:
                    for item in row:
                        if  item.has_key(prim_key):
                            yield item
                        else:
                            yield {prim_key:item}
                else:
                    if  row.has_key(prim_key):
                        yield row
                    else:
                        yield {prim_key:row}
        else:
            msg = 'Unsupported data format="%s", API="%s"' % (format, api)
            raise Exception(msg)

    def translator(self, api, genrows):
        """
        Convert raw results into DAS records.
        """
        prim_key  = self.dasmapping.primary_key(self.name, api)
        notations = self.dasmapping.notations(self.name)[self.name]
        count = 0
        for row in genrows:
            row2das(self.dasmapping.notation2das, self.name, api, row)
            count += 1
            # check for primary key existance, since it can be overriden
            # by row2das. For example DBS3 uses flat namespace, so we
            # override dataset=>name, while dataset still is a primary key
            if  row.has_key(prim_key):
                yield row
            else:
                yield {prim_key:row}
        msg = "DASAbstractService::%s::translator yield %s records" \
                % (self.name, count)
        self.logger.info(msg)

    def api(self, query):
        """
        Data service api method, can be defined by data-service class.
        It parse input query and invoke appropriate data-service API
        call. All results are stored into the DAS cache along with
        api call inserted into Analytics DB.
        """
        self.logger.info('DASAbstractService::%s::api(%s)' \
                % (self.name, query))
        result = False
        for url, api, args, format, expire in self.apimap(query):
            try:
                mkey    = self.dasmapping.primary_mapkey(self.name, api)
                args    = self.inspect_params(api, args)
                args    = self.clean_params(api, args)
                args    = self.patterns(api, args)
                time0   = time.time()
                data    = self.getdata(url, args)
                rawrows = self.parser(format, data, api, args)
                dasrows = self.translator(api, rawrows)
                ctime   = time.time() - time0
                self.write_to_cache(query, expire, url, api, args, 
                        dasrows, ctime)
            except:
                msg  = 'Fail to process: url=%s, api=%s, args=%s' \
                        % (url, api, args)
                msg += traceback.format_exc()
                self.logger.info(msg)

    def apimap(self, query):
        """
        This method analyze the input query and create apimap
        dictionary which includes url, api, parameters and
        data services interface (JSON, XML, etc.)
        In a long term we can store this results into API db.
        """
        cond = getarg(query, 'spec', {})
        skeys = getarg(query, 'fields', [])
        self.logger.info("\n")
        for api, value in self.map.items():
            expire = value['expire']
            format = value['format']
            url    = value['url']
            args   = value['params']
            found  = False
            for key, val in cond.items():
                # check if keys from conditions are accepted by API.
                if  self.dasmapping.check_dasmap(self.name, api, key):
                    # need to convert key (which is daskeys.map) into
                    # input api parameter
                    found = True
                    for apiparam in self.dasmapping.das2api(self.name, key):
                        if  args.has_key(apiparam):
                            args[apiparam] = val
            if  not found:
                continue
            # check that there is no "required" parameter left in args,
            # since such api will not work
            if 'required' in args.values():
                continue
            # check if analytics db has a similar API call
            if  not self.pass_apicall(url, api, args):
                continue
            msg  = "DASAbstractService::apimap yield "
            msg += "system %s, url=%s, api=%s, args=%s, format=%s, expire=%s" \
                % (self.name, url, api, args, format, expire)
            self.logger.info(msg)
            yield url, api, args, format, expire
