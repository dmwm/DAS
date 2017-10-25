#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: das_query.py
Author: Gordon Ball and Valentin Kuznetsov
Description: Main class to hold DAS query object
"""

# system modules
import re
import sys
import json
import copy
from   bson.objectid import ObjectId

# DAS modules
from   DAS.core.das_ql import DAS_RECORD_KEYS
from   DAS.utils.regex import RE_3SLASHES
from   DAS.utils.utils import genkey, deepcopy, print_exc, dastimestamp
from   DAS.utils.query_utils import compare_specs
from   DAS.core.das_parser import ql_manager
try:
    from   DAS.core.das_process_dataset_wildcards import process_dataset_wildcards
    DATASET_WILDCARDS = True
except ImportError: # python3
    DATASET_WILDCARDS = False
from   DAS.core.das_exceptions import WildcardMultipleMatchesException
from   DAS.core.das_exceptions import WildcardMatchingException

# python3
if  sys.version.startswith('3.'):
    basestring = str
    unicode = str

def check_query(query):
    "Check query"
    prohibited = ['__class__', '__bases__', '__getitem__', '__subclasses__', '__builtins__', '__import__']
    for word in prohibited:
        if  isinstance(query, basestring) or isinstance(query, DASQuery):
            if  query.find(word) != -1:
                raise Exception("Bad query %s" % query)

class DASQuery(object):
    """
    Wrapper around base query. Uses properties to calculate
    qhash, storage query, mongo query on demand. Other methods, 
    such as to_bare_query return a new DASQuery, modified
    appropriately.
    """

    NON_CACHEABLE_FLAGS = ['mongoparser', 'active_dbsmgr']

    def _handle_dataset_slashes(self, key, val):
        """
        Tries to convert a wildcard pattern given in free from (e.g. *Zmm*) into
        three slash pattern required by services using cached dataset names.

        If no single interpretation could be found we'll raise an Exception
         (no match or multiple matches)

        Fixes: ticket #3071
        """
        msg  = 'Dataset value requires 3 slashes.'
        if  DATASET_WILDCARDS:
            dataset_matches = process_dataset_wildcards(val, self._instance)
        else:
            dataset_matches = []
#        print "Matching wildcard query=%s into dataset patterns=%s" % (
#            val, dataset_matches)
        if not len(dataset_matches):
            # TODO: fuzzy matching to propose fixes?
            # This also could be done for any dataset...
            raise WildcardMatchingException(
                    'The pattern you specified did not match '
                    'any datasets in DAS cache:\n'
                    '%s\n'
                    'Check for misspellings or if you know '
                    'it exists, provide it '
                    'with three slashes: \n'
                    '/primary_dataset/processed_daset/data_tier' % val)
        elif len(dataset_matches) > 1:
            options = {}
            for match in dataset_matches:
                options[match] = self.query.replace(val, match)
            raise WildcardMultipleMatchesException(msg + ' ' +
                    'The query matches these dataset patterns:\n', options)
        else:
            # If there's only one match, still ask for user's confirmation
            match = dataset_matches[0]
            options = {match: self.query.replace(val, match)}
            raise WildcardMultipleMatchesException(msg + ' ' +
                    'The query matches one dataset pattern in our cache, but '
                    'please check if this is what you intended:\n', options)

    def __init__(self, query, **flags):
        """
        Accepts general form of DAS query, supported formats are
        DAS input query, DAS mongo query, DAS storage query. The
        supplied flags can carry any query attributes, e.g.
        filters, aggregators, system, instance, etc.
        """
        check_query(query)
        self._mongoparser   = None
        self._params        = {}
        self._service_apis_map = {}
        self._str           = ''
        self._query         = ''
        self._query_pat     = ''
        self._query_full    = ''
        self._storage_query = {}
        self._mongo_query   = {}
        self._qhash         = None
        self._hashes        = None
        self._system        = None
        self._instance      = None
        self._loose_query   = None
        self._pattern_query = None
        self._sortkeys      = []
        self._filters       = {}
        self._mapreduce     = []
        self._aggregators   = []
        self._qcache        = 0
        self._flags         = flags
        self._error         = ''

        # loop over flags and set available attributes
        for key, val in flags.items():
            setattr(self, '_%s' % key, val)

        # test data type of input query and apply appropriate initialization
        if  isinstance(query, basestring):
            self._query = query
            try:
                self._mongo_query = self.mongoparser.parse(query)
                for key, val in flags.items():
                    if  key in self.NON_CACHEABLE_FLAGS:
                        continue
                    if  key not in self._mongo_query:
                        self._mongo_query[key] = val
            except Exception as exp:
                msg = "Fail to parse DAS query='%s', %s" % (query, str(exp))
                print_exc(msg, print_traceback=True)
                self._mongo_query = {'error': msg, 'spec': {}, 'fields': []}
                self._storage_query = {'error': msg}
                self._error = msg
#                 raise exp
        elif isinstance(query, dict):
            newquery = {}
            for key, val in query.items():
                newquery[key] = val
            if  isinstance(newquery.get('spec'), dict): # mongo query
                self._mongo_query = newquery
            else: # storage query
                self._storage_query = newquery
        elif isinstance(query, object) and hasattr(query, '__class__')\
            and query.__class__.__name__ == 'DASQuery':
            self._query = query.query
            self._query_pat = query.query_pat
            self._hashes = query.hashes
            self._mongo_query = query.mongo_query
            self._storage_query = query.storage_query
        else:
#             raise Exception('Unsupported data type of DAS query')
            self._error = 'Unsupported data type of DAS query'
        if self._error:
            return
        self.update_attr()

        # check dataset wild-cards
        for key, val in self._mongo_query['spec'].items():
            if  key == 'dataset.name':
                if  isinstance(val, dict): # we get {'$in':[a,b]}
                    continue
                # only match dataset.name but do not primary_dataset.name
                if  not RE_3SLASHES.match(val):

                    # TODO: we currently do not support wildcard matching
                    #       from command line interface
                    if not self._instance:
                        continue

                    # apply 3 slash pattern look-up, continuing only if one
                    # interpretation existings here, ticket #3071
                    self._handle_dataset_slashes(key, val)


    def find(self, pat):
        "Find method"
        return self._query.find(pat)

    def update_attr(self):
        """
        setup DAS query attributes if they were supplied in input query
        for instance if input query is mongo query/storage_query
        """
        self._instance    = self.mongo_query.get('instance', self._instance)
        self._system      = self.mongo_query.get('system', self._system)
        self._filters     = self.mongo_query.get('filters', self._filters)
        self._aggregators = \
                self.mongo_query.get('aggregators', self._aggregators)
        self._mapreduce   = self.mongo_query.get('mapreduce', self._mapreduce)

    ### Class properties ###
    @property
    def services(self):
        "Return list of services which may provide information about this query"
        return list(self.service_apis_map().keys())

    @property
    def qcache(self):
        "qcache property of the DAS query"
        return int(self._qcache)

    @property
    def hashes(self):
        "hashes property of the DAS query"
        return self._hashes

    @property
    def error(self):
        "error property of the DAS query"
        return self._error

    @property
    def mongoparser(self):
        "mongoparser property of the DAS query"
        if  not self._mongoparser:
            self._mongoparser = ql_manager()
        return self._mongoparser

    @property
    def filters(self):
        "filters property of the DAS query"
        filters = self._mongo_query.get('filters', {})
        return filters.get('grep', [])

    @property
    def sortkeys(self):
        "sortkeys property of the DAS query"
        filters = self._mongo_query.get('filters', {})
        return filters.get('sort', [])

    @property
    def unique_filter(self):
        "Check if DAS query has unique filter"
        filters = self._mongo_query.get('filters', {})
        return True if 'unique' in filters.keys() else False

    @property
    def mapreduce(self):
        "mapreduce property of the DAS query"
        return self._mapreduce

    @property
    def aggregators(self):
        "aggregators property of the DAS query"
        return self._aggregators

    @property
    def system(self):
        "system property of the DAS query"
        return self._system

    @property
    def instance(self):
        "instance property of the DAS query"
        return self._instance

    @property
    def query_pat(self):
        "Creates pattern for DAS query"
        if  not self._query_pat:
            fields = self.mongo_query.get('fields', [])
            if  not fields:
                fields = [] # will use empty list for conversion
            query  = ','.join([f for f in fields if f not in DAS_RECORD_KEYS])
            query += ' ' # space between fields and spec values
            query += ' '.join(['{0}={0}_value'.format(k.split('.')[0]) for k \
                        in self.mongo_query.get('spec', {}).keys()])
            self._query_pat = query.strip()
        return self._query_pat

    @property
    def query(self):
        "query property of the DAS query (human readble form)"
        if  not self._query:
            fields = self.mongo_query.get('fields', [])
            if  not fields:
                fields = [] # will use empty list for conversion
            query  = ' '.join(fields)
            query += ' ' # space between fields and spec values
            query += ' '.join(['%s=%s' % (k, v) for k, v \
                        in self.mongo_query.get('spec', {}).items()])
            self._query = query.strip()
        return self._query

    @property
    def storage_query(self):
        """
        Read only storage query, generated on demand.
        """
        if  not self._storage_query:
            self._storage_query = deepcopy(self.mongo_query)
            speclist = []
            for key, val in self._storage_query.pop('spec').items():
                if  str(type(val)) == "<type '_sre.SRE_Pattern'>":
                    val = json.dumps(val.pattern)
                    speclist.append({"key":key, "value":val, "pattern":1})
                elif isinstance(val, ObjectId):
                    speclist.append({"key":key, "value":str(val)})
                else:
                    val = json.dumps(val)
                    speclist.append({"key":key, "value":val})
            self._storage_query['spec'] = speclist
        return self._storage_query
        
    @property
    def mongo_query(self):
        """
        Read only mongo query, generated on demand.
        """
        system = self._mongo_query.get('system', [])
        filters = self._mongo_query.get('filters', {})
        aggregators = self._mongo_query.get('aggregators', [])
        if  not self._mongo_query:
            self._mongo_query = deepcopy(self.storage_query)
            for key, val in self._mongo_query.items():
                if  key not in ['fields', 'spec']:
                    setattr(self, '_%s' % key, val)
            spec = {}
            for item in self._mongo_query.pop('spec'):
                val = json.loads(item['value'])
                if  'pattern' in item:
                    val = re.compile(val)
                spec.update({item['key'] : val})
            self._mongo_query['spec'] = spec
        # special case when user asks for all records
        fields = self._mongo_query.get('fields', None)
        if  fields and fields == ['records']:
            self._mongo_query['fields'] = None
            spec = {}
            for key, val in self._mongo_query['spec'].items():
                if  key != 'records':
                    spec[key] = val
            self._mongo_query = dict(fields=None, spec=spec)
        if  filters:
            self._mongo_query.update({'filters':filters})
        if  aggregators:
            self._mongo_query.update({'aggregators':aggregators})
        if  system:
            self._mongo_query.update({'system':system})
        return self._mongo_query
    
    @property
    def qhash(self):
        """
        Read only qhash, generated on demand.
        """
        if  not self._qhash:
            sdict = deepcopy(self.storage_query)
            for key in ['filters', 'aggregators', 'mapreduce']:
                if  key in sdict:
                    del sdict[key]
            self._qhash = genkey(sdict)
        return self._qhash

    @property
    def loose_query(self):
        """
        Construct loose version of the query. That means add 
        pattern '*' to string type values for all conditions.
        """
        if  not self._loose_query:
            query   = deepcopy(self.mongo_query)
            spec    = query.get('spec', {})
            fields  = query.get('fields', None)
            newspec = {}
            for key, val in spec.items():
                if  key != '_id' and \
                isinstance(val, str) or isinstance(val, unicode):
                    if  val[-1] != '*':
                        val += '*' # add pattern
                newspec[key] = val
            self._loose_query = dict(spec=newspec, fields=fields)
        return self._loose_query

    @property
    def pattern_query(self):
        """
        Patter property for DAS query whose spec is modified
        to regexes and $exists keys
        """
        if  not self._pattern_query:
            nmq = deepcopy(self.loose_query)
            
            def edit_dict(old):
                """
                Inner recursive dictionary manipulator
                """
                result = {}
                for key, val in old.items():
                    if isinstance(val, basestring):
                        if  '*' in val:
                            if len(val) == 1:
                                result[key] = {'$exists': True}
                            else:
                                result[key] = \
                                    re.compile('^%s' % val.replace('*', '.*'))
                    elif isinstance(val, dict):
                        result[key] = edit_dict(val)
                    else:
                        result[key] = val
                return result
            
            nmq['spec'] = edit_dict(nmq['spec'])    
            self._pattern_query = nmq
        return self._pattern_query
    
    ### Class method ###

    def params(self):
        "Extract params (keys, conditions and services) from mongo query"
        if  self._params:
            return self._params
        if  self.mongoparser:
            self._params = self.mongoparser.params(self.mongo_query)
        return self._params

    def service_apis_map(self):
        "Return data service APIs map who may answer DAS query"
        if  self._service_apis_map:
            return self._service_apis_map
        if  self.mongoparser:
            self._service_apis_map = \
                self.mongoparser.service_apis_map(self.mongo_query)
        return self._service_apis_map

    def is_bare_query(self):
        """
        Return true if we only have 'fields' and 'spec' keys.
        """
        return sorted(list(self.mongo_query.keys())) == ['fields', 'spec']
            
    def to_bare_query(self):
        """
        Return a new query containing only field and spec keys of this query.
        May be identical if this is already a bare query.
        """
        if  self.is_bare_query():
            return self
        mongo_query = {'fields': copy.deepcopy(self.mongo_query['fields']),
                'spec': deepcopy(self.mongo_query['spec'])}
        return mongo_query

    def to_dict(self):
        "Convert DAS query into dictionary"
        obj = dict(query=self.query, mongo_query=self.mongo_query,
                   instance=self.instance, filters=self.filters,
                   aggregators=self.aggregators, system=self.system,
                   qhash=self.qhash, mapreduce=self.mapreduce)
        return obj

    def has_id(self):
        """
        Check if there is an _id key in the spec.
        """
        return '_id' in self.mongo_query['spec']
    
    def has_mongo_id(self):
        """
        Check if the _id key is a mongo ObjectId
        """
        return isinstance(self.mongo_query.get('_id', None), ObjectId)
    
    def to_mongo_id(self):
        """
        Return a new DASQuery where the _id has been converted
        (as adjust_id) to a mongo ObjectId
        """
        nmq = deepcopy(self.mongo_query)
        if self.has_id():
            val = self.mongo_query['spec']['_id']
            if isinstance(val, str):
                nmq['spec']['_id'] = ObjectId(val)
            elif isinstance(val, unicode):
                nmq['spec']['_id'] = ObjectId(unicode.encode(val))
            elif isinstance(val, list):
                result = []
                for item in val:
                    if isinstance(val, str):
                        result.append(ObjectId(item))
                    elif isinstance(val, unicode):
                        result.append(ObjectId(unicode.encode(item)))
                    else:
                        raise Exception("non str|unicode _id.child")
                nmq['spec']['_id'] = result
            else:
                raise Exception("non str|unicode|list _id")
        return DASQuery(nmq, **self._flags)
    
    def flag(self, name):
        """
        Test the state of any flags that were set.
        """
        return self._flags.get(name, False)
    
    def is_superset_of(self, other):
        """
        Return true if we are a superset of other
        """
        return compare_specs(other.mongo_query, self.mongo_query)
    
    def is_subset_of(self, other):
        """
        Return true if we are not a superset of the other.
        This is perhaps not the same thing as "is subset of"
        """
        return compare_specs(self.mongo_query, other.mongo_query)

    def __gt__(self, other):
        "Query comparision operator"
        return self.is_superset_of(other)

    def __lt__(self, other):
        "Query comparision operator"
        return self.is_subset_of(other)

    def __eq__(self, other):
        "Query comparision operator"
        return self.qhash == other.qhash

    def __hash__(self):
        "Hash of the object"
        return hash(self.qhash)

    def __str__(self):
        "Query string representation"
        if  self._str:
            return self._str
        if  self._qcache:
            msg = """<query='''%s''' instance=%s qhash=%s services=%s qcache=%s>""" \
                % (self.query, self.instance, self.qhash, self.services, self._qcache)
        else:
            msg = """<query='''%s''' instance=%s qhash=%s services=%s>""" \
                % (self.query, self.instance, self.qhash, self.services)
        self._str = msg
        return msg
