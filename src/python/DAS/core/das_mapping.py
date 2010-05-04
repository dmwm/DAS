#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mapping
"""

from __future__ import with_statement

__revision__ = "$Id: das_mapping.py,v 1.13 2009/09/01 17:06:15 valya Exp $"
__version__ = "$Revision: 1.13 $"
__author__ = "Valentin Kuznetsov"

import re # we get regex pattern from DB and eval it in primary_key
import types
from DAS.core.das_mapping_db import DASMappingMgr
from DAS.core.das_mapping_db import System, DASKey, Api, APIMap 
from DAS.core.das_mapping_db import DASMap, API2DAS, DASNotation

class DASMapping(DASMappingMgr):
    """
    Provides bi-directional mapping between DAS and data-services.
    So far all maps are defined in configuration files, later we
    will switch to mapping DB. We require 3 type of maps:
    1. mapping between DAS keyword to data-service APIs notations
    2. mapping between data-service output to DAS keys
    3. mapping between data-services, e.g. in phedex the block
    size is defined as "bytes", while in DBS as "size"
    """
    def __init__(self, config):
        DASMappingMgr.__init__(self, config)

    def primary_key(self, system, daskey, api=None, value=None):
        """
        Returns primary key for given system and provided
        selection DAS key, e.g. block => block.name
        """
        session = self.session()
        query = session.query(Api, API2DAS, DASKey, System, DASMap).\
            filter(DASMap.system_id==System.id).\
            filter(DASMap.daskey_id==DASKey.id).\
            filter(System.name==system).\
            filter(DASKey.name==daskey).\
            filter(Api.id==API2DAS.api_id).\
            filter(Api.id==DASMap.api_id).\
            filter(API2DAS.daskey_id==DASMap.daskey_id).\
            filter(API2DAS.system_id==System.id)
        if  api:
            query = query.filter(Api.name==api)
        primkeys = []
        for aobj, adas, dobj, sobj, dmap in query.all():
            pkey = dmap.primary_key
            pat  = eval(adas.pattern)
            if  value:
                if  not pat.match(value): 
                    # not match, since we already know value
                    # and need to find key which provide other
                    # information then value
                    if  pkey not in primkeys:
                        primkeys.append(pkey)
            else:
                if  pkey not in primkeys:
                    primkeys.append(pkey)
#        for row in query.all():
#            pkey = row[-1].primary_key
#            if  pkey not in primkeys:
#                primkeys.append(pkey)
        if  len(primkeys) > 1:
            msg  = 'Ambigous primary keys: %s\n' % str(primkeys)
            msg += 'system=%s, daskey=%s' % (system, daskey)
            raise Exception(msg)
        return primkeys[0]
#        row = query.one()
#        return row[-1].primary_key

    def api2das(self, system, api_input_name):
        """
        Translates data-service API input parameter into DAS QL key,
        e.g. run_number => run.
        """
        session = self.session()
        query = session.query(System, APIMap, API2DAS, DASKey).\
        filter(System.name==system).\
        filter(System.id==APIMap.system_id).\
        filter(APIMap.param==api_input_name).\
        filter(APIMap.api_id==API2DAS.api_id).\
        filter(API2DAS.system_id==System.id).\
        filter(API2DAS.param_id==APIMap.id).\
        filter(API2DAS.daskey_id==DASKey.id)
        names = []
        for row in query.all():
            daskey = row[-1].name
            if  daskey not in names:
                names.append(daskey)
        return names

    def das2api(self, system, daskey, value=None):
        """
        Translates DAS QL key into data-service API input parameter
        """
        session = self.session()
        query = session.query(System, DASKey, API2DAS, APIMap).\
        filter(System.name==system).\
        filter(System.id==APIMap.system_id).\
        filter(APIMap.api_id==API2DAS.api_id).\
        filter(API2DAS.system_id==System.id).\
        filter(API2DAS.param_id==APIMap.id).\
        filter(API2DAS.daskey_id==DASKey.id).\
        filter(DASKey.name==daskey)
        if  value:
            query.filter(API2DAS.pattern==value)
        names = []
        for row in query.all():
            api_param = row[-1].param
            if  api_param not in names:
                names.append(api_param)
        return names

    def notation2das(self, system, api_param):
        """
        Translates data-service API parameter name into DAS name, e.g.
        run_number=run. In case when api_param is not presented in DB
        just return it back.
        """
        session = self.session()
        try:
            row = session.query(System, DASNotation).\
            filter(System.name==system).\
            filter(System.id==DASNotation.system_id).\
            filter(DASNotation.api_param==api_param).one()
            daskey = row[-1].das_param
        except:
            daskey = api_param
        return daskey

    def api2daskey(self, system, api):
        """
        Returns list of DAS keys which cover provided data-service API
        """
        session = self.session()
        query = session.query(System, Api, API2DAS, DASKey).\
        filter(System.name==system).\
        filter(System.id==Api.system_id).\
        filter(API2DAS.api_id==Api.id).\
        filter(API2DAS.system_id==System.id).\
        filter(API2DAS.daskey_id==DASKey.id).\
        filter(Api.name==api)
        keys = []
        for row in query.all():
            daskey = row[-1].name
            if  daskey not in keys:
                keys.append(daskey)
        return keys

    def servicemap(self, system, implementation=None):
        """
        Constructs data-service map, e.g.
        {api: {keys:[list of DAS keys], params: dict_of_api_params} }
        """
        smap = {}
        apis = self.list_apis(system)
        for api in self.list_apis(system):
            params = self.api_params(api)
            daskeys = self.api_keys(api)
            if  implementation=='javaservlet':
                smap[api] = dict(keys=daskeys, params=params, api=dict(api=api))
            else:
                smap[api] = dict(keys=daskeys, params=params)
        return smap

# NEW json2das/jsonparser implementation
def json2das(system, input, keylist, selkeys):
    """
    Convert rows from provided input dict (input) into DAS rows
    using system name , e.g. phedex, dbs, etc., data-service
    output keys (keylist) and selection key (selkeys) from DAS QL.
    """
    # NOTE: the input dict should be in a form of
    # {'system':{result_dict}} as phedex or
    # {'1':{result dict}, } as sitedb
    for row in jsonparser(input, keylist):
        for key in keylist:
            found = 0
            for nkey in result2das(system, key):
                if  nkey != key and nkey in selkeys:
                    row[nkey] = row[key]
                    found = 1
            if  found:
                del row[key]
#        yield row
        count = 0
        for newrow in json2das_zip(row):
            count = 1
            yield newrow
        if  not count:
            yield row

def json2das_zip(row):
    """
    Analyze input row and if key value is a list, expand it in a list
    of rows with the same keys.
    """
    longest = 0
    for val in row.values():
        if  type(val) is types.ListType:
            if  len(val) > longest:
                longest = len(val)
    if  not longest:
        yield row
    else:
        for key, val in row.items():
            if  type(val) is not types.ListType:
                 row[key] = [val for i in range(0, longest)]
        for i in range(0, longest):
            newrow = {}
            for key, val in row.items():
                newrow[key] = val[i]
            yield newrow

def jsonparser(input, keylist):
    """
    Yields rows from provided input dict (input) and key list (keylist)
    """
    row = {}
    for key in keylist:
        gen = jsonparser4key(input, key)
#        if  gen:
        if  type(gen) is types.GeneratorType or type(gen) is types.ListType:
            item = [i for i in gen]
            if  not item:
                item = ''
            if  len(item) == 1:
                row[key] = item[0]
            else:
                row[key] = item
            if  len(row.keys()) == len(keylist):
                yield row
        elif gen:
            row[key] = gen
            yield row
        else:
            row[key] = ''
            yield row

def jsonparser4key(jsondict, ikey):
    """
    Yield value for provided key from given jsondict
    """
    if  type(jsondict) is not types.DictType:
        return
    if  ikey.count('.'): # composed key
        key, attr = ikey.split('.', 1)
    else:
        key  = ikey
        attr = ''
    if  jsondict.has_key(key):
        for k, v in jsondict.items():
            if  k != key:
                continue
            if  not attr:
                return v
            if  type(v) is types.ListType:
                return [jsonparser4key(i, attr) for i in v]
            elif  type(v) is types.DictType:
                if  v.has_key(attr):
                    return v[attr]
    else:
        def helper(jsondict, ikey):
            for k in jsondict.keys():
                val = jsonparser4key(jsondict[k], ikey)
                if  val:
                    if  type(val) is types.ListType or \
                        type(val) is types.GeneratorType:
                        for item in val:
                            yield item
                    else:
                        yield val
        return helper(jsondict, ikey)
# END
