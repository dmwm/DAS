#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Abstract interface for DAS service
"""
__revision__ = "$Id: abstract_service.py,v 1.28 2009/09/01 01:42:45 valya Exp $"
__version__ = "$Revision: 1.28 $"
__author__ = "Valentin Kuznetsov"
import time
import types
import urllib
import urllib2
import traceback
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json

from DAS.utils.utils import dasheader
from DAS.utils.utils import cartesian_product
from DAS.core.qlparser import QLLexer, mongo_exp

class DASAbstractService(object):
    """
    Abstract class describing DAS service. It initialized with a name who
    is used to identify service parameters from DAS configuration file.
    Those parameters are keys, verbosity level, URL of the data-service.
    """
    def __init__(self, name, config):
        self.name = name
        try:
            sdict           = config[name]
            self.verbose    = int(sdict['verbose'])
            self.expire     = int(sdict['expire'])
            self.url        = sdict['url']
            self.logger     = config['logger']
            self.dasmapping = config['dasmapping']
        except:
            traceback.print_exc()
            print config
            raise Exception('fail to parse DAS config')

        self.map          = {} # to be defined by data-service implementation
        self.qllexer      = None # to be defined at run-time in self.worker
        self._keys        = None # to be defined at run-time in self.keys
        self._params      = None # to be defined at run-time in self.parameters

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

    def getdata(self, url, params, headers=None):
        """
        Invoke URL call and retrieve data from data-service based
        on provided URL and set of parameters. All data are parsed
        by data-service parsers to provide uniform JSON representation
        for further processing.
        """
        msg = 'DASAbstractService::%s::getdata(%s, %s)' \
                % (self.name, url, params)
        self.logger.info(msg)
        input_params = params
        # if necessary the data-service implementation will adjust parameters,
        # for instance, DQ parser Tracker_Global=GOOD&Tracker_Local1=1
        # into the following form
        # [{"Oper": "=", "Name": "Tracker_Global",  "Value": "GOOD"},...]
        self.adjust_params(params)

        # based on provided interface correctly deal with input parameters
        if  self.name == 'dq':
            encoded_data = json.dumps(params)
        else:
            encoded_data = urllib.urlencode(params, doseq=True)
        req = urllib2.Request(url)
        if  headers:
            for key, val in headers.items():
                req.add_header(key, val)
        if  not encoded_data:
            encoded_data = None
        data = urllib2.urlopen(req, encoded_data)
        return data.read()
#        results = data.read()
#        return self.parser(results, input_params)

    def product(self, resdict):
        """
        Make cartesian product for all entries in provided result dict.
        """
        data = []
        if  not resdict:
            return data
        keys = resdict.keys()
        if  len(keys) == 1:
            for rows in resdict.values():
                data += rows
        else: # need to make cartesian product of results
            set0 = resdict[keys[0]]
            set1 = resdict[keys[1]]
            result = cartesian_product(set0, set1)
            if  len(keys) > 2:
                for rest in keys[2:]: 
                    result = cartesian_product(result, resdict[rest])
            data = result
        return data

    def call(self, query):
        """
        Invoke service API to execute given query.
        Return results as a collect list set.
        """
        msg = 'DASAbstractService::%s::call(%s)' \
                % (self.name, query)
        self.logger.info(msg)
        mongo_query = self.mongo_query_parser(query)
        msg = 'DASAbstractService::%s mongo_query=%s' % (self.name, mongo_query)
        self.logger.info(msg)

        # check the cache if there are records with given parameters
        spec = mongo_query['spec'].keys() # we take all parameters
        spec.remove('das.system') # and check if conditions provided
        if  self.localcache.incache(query=mongo_query) and spec:
            return self.localcache.get_from_cache(query=mongo_query)
        # check the cache if there are records with given input query
        dasquery = {'spec': {'das.query': query, 'das.system': self.name}, 
                    'fields': None}
        if  self.localcache.incache(query=dasquery):
            return self.localcache.get_from_cache(query=dasquery)

        # ask data-service api to get results, they'll be store them in
        # cache, so return at the end what we have in cache.
        result = self.api(query)
        if  result:
            return self.localcache.get_from_cache(query=mongo_query)
        else:
            return []
#        return self.localcache.get_from_cache(query=dasquery)

    def adjust_params(self, args):
        """
        Data-service specific parser to adjust parameters according to
        specifications.
        """
        pass

    def parser(self, api, data, params=None):
        """
        Output data parser, can be implemeted in derived classes.
        By default we assume that data has string type (returned by
        URL call to data-service api). We substitute
        null (e.g. Phedex) into null string and evaluate the results.
        """
        # to prevent unicode/ascii errors like
        # UnicodeDecodeError: 'utf8' codec can't decode byte 0xbf in position
        if  type(data) is types.StringType:
            data = unicode(data, errors='ignore')

        self.logger.debug('DASAbstractService::%s results=%s' \
                % (self.name, data))

        res = data.replace('null', '\"null\"')
        try:
            jsondict = json.loads(res)
        except:
            jsondict = eval(res)
            pass
        if  self.verbose > 2:
            print "\n### %s returns" % self.name
            print jsondict
        for key in jsondict.keys():
            newkey = self.dasmapping.notation2das(self.name, key)
            if  newkey != key:
                jsondict[newkey] = jsondict[key]
                del jsondict[key]
        # add which sub-system has been used parameters
#        jsondict['system'] = self.name
        if  params:
            for key, val in params.items():
                newkey = self.dasmapping.notation2das(self.name, key)
                if  not jsondict.has_key(newkey):
                    jsondict[newkey] = val
        yield jsondict

    def query_parser(self, query):
        """
        Init QLLexer and parse input query.
        """
        if  not self.qllexer:
            imap = {self.name:self.keys()}
            params = {self.name:self.parameters()}
            self.qllexer = QLLexer(imap, params)

        selkeys = self.qllexer.selkeys(query)
        conditions = self.qllexer.conditions(query)
        msg = 'DASAbstractService::query_parser, selkeys %s' % selkeys
        self.logger.debug(msg)
        msg = 'DASAbstractService::query_parser, conditions %s' % conditions
        self.logger.debug(msg)
        return selkeys, conditions

    def mongo_query_parser(self, query):
        """
        Init QLLexer and transform input query into MongoDB syntax
        """
        selkeys, conditions = self.query_parser(query)
        specialcase = False
        if  selkeys == ['records']: # special case to look-up all records
            specialcase = True
            fields = None
        else:
            fields = ['das'] # we will always look-up the system name
            for key in selkeys:
                fields.append(key)
        condlist = []
        for item in conditions:
            if  type(item) is types.DictType:
                key = item['key']
                nkey = self.dasmapping.primary_key(self.name, key)
                if  not nkey:
                    nkey = key
                cdict = dict(key=nkey, op=item['op'], value=item['value'])
                condlist.append(cdict)
            else:
                condlist.append(item)
        cond_dict = mongo_exp(condlist)
        cond_dict['das.system'] = self.name
        cond_dict['das.selection_keys'] = {'$in' : selkeys}
        # see MongoDB API, http://api.mongodb.org/python/
        # we return spec and fields dict
        return dict(spec=cond_dict, fields=fields)

    def primary_key(self, api):
        """
        Return primary key of data output for given data-service API.
        """
#        return self.map[api]['primary_key']
        # TODO: I'm not sure what to do with primary keys yet
        # how many each API output will supply?
        prim_keys = []
        for key in self.map[api]['keys']:
            pkey = self.dasmapping.primary_key(self.name, key)
            if  pkey not in prim_keys:
                prim_keys.append(pkey)
        return prim_keys[0]

    def api(self, query):
        """
        Data service api method, can be defined by data-service class.
        It parse input query and invoke appropriate data-service API
        call. All results are store into localcache.
        """
        self.logger.info('DASAbstractService::%s::api(%s)' \
                % (self.name, query))
        selkeys, conditions = self.query_parser(query)
        mongo_query = self.mongo_query_parser(query)
        result = False
        for url, api, args in self.apimap(query):
            msg  = 'DASAbstractService::%s::api found %s, %s' \
                % (self.name, api, str(args))
            self.logger.info(msg)
            time0   = time.time()
            data    = self.getdata(url, args)
            genrows = self.parser(api, data, args)
            ctime   = time.time() - time0
            header  = dasheader(self.name, query, api, url, args, ctime,
                self.expire, self.version())
            header['primary_keys'] = self.primary_key(api)
            header['selection_keys'] = selkeys
            self.localcache.update_cache(mongo_query, genrows, header)
            msg  = 'DASAbstractService::%s cache has been updated,' % self.name
            self.logger.info(msg)
            result = True
        return result

    def apimap(self, query):
        """
        This method analyze the input query and create apimap
        dictionary which includes url, api, parameters and
        data services interface (JSON, XML, etc.)
        In a long term we can store this results into API db.
        """
        selkeys, conditions = self.query_parser(query)

        # loop over conditions and create input params dict which will
        # be passed to data-service, convert DAS condition keys into
        # ones accepted by data-service
        params = {}
        for item in conditions:
            if  type(item) is types.DictType: # cond dict
                oper = item['op']
                val  = item['value']
                key  = item['key']
            else:
                continue # TODO: what should I do if I find brackets?
            keylist = self.dasmapping.das2api(self.name, key, val)
            if  type(keylist) is not types.ListType:
                keylist = [keylist]
            for newkey in keylist:
                params[newkey] = val

        apiname = ""
        def get_args(params):
            """
            Create a dict of arguments passed to API out of required and
            default ones
            """
            args = {}
            for key, val in params.items():
                if  val == 'required':
                    args[key] = ''
                elif val: # default
                    args[key] = val
            return args

        # loop over dataservices and find out which api/params
        # we need to call. Yield results in a form of
        # url, api, args
        for api, aparams in self.map.items():
            if  selkeys == ['records']: # special case
                if  not set(params.keys()) & set(aparams['params']):
                    continue
            elif  not set(selkeys) & set(aparams['keys']):
                continue # datasrv API doesn't cover requested selkeys
            
            args = get_args(aparams['params'])
            for par, value in aparams['params'].items():
                if  params.has_key(par):
                    args[par] = params[par]
                else:
                    args[par] = value
                if  args[par] == 'list' or args[par] == 'required':
                    msg  = 'Missing required parameter %s, api2das returns %s'\
                        % (par, self.dasmapping.api2das(self.name, par))
                    msg += '\nAPI: %s, params %s' % (api, str(aparams))
                    self.logger.info('Skip API=%s, reason\n%s' % (api, msg))
#                    raise Exception(msg)
            if  aparams.has_key('api') or aparams['params'].has_key('api'):
                try:
                    apidict = aparams['api']
                    apikey  = apidict.keys()[0]
                    apival  = apidict[apikey]
                    args[apikey] = apival 
                except:
                    pass
                url = self.url # JAVA, e.g. http://host/Servlet
            else: # if we have http://host/api?...
                url = self.url + '/' + api
            yield url, api, args
