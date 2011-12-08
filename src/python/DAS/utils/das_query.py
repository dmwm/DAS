#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: das_query.py
Author: Gordon Ball and Valentin Kuznetsov
Description: Main class to hold DAS query object
"""

# system modules
import re
import copy
from   pymongo.objectid import ObjectId

# DAS modules
import DAS.utils.jsonwrapper as json
from   DAS.utils.utils import genkey, deepcopy, print_exc
from   DAS.core.das_mongocache import compare_specs
from   DAS.core.das_parser import QLManager

class DASQuery(object):
    """
    Wrapper around base query. Uses properties to calculate
    qhash, storage query, mongo query on demand. Other methods, 
    such as to_bare_query return a new DASQuery, modified
    appropriately.
    """
    def __init__(self, query=None, storage_query=None, 
                 mongo_query=None, **flags):
        """
        Accepts either "query", which it will determine
        whether is a storage or mongo query, or either or
        both mongo_query and storage_query. It is assumed
        if you supply them they are equivalent.
        """
        assert bool(query) ^ bool(storage_query or mongo_query)
        self.mongoparser = QLManager()
        self._query = None
        self._storage_query = None
        self._mongo_query = None
        self._flags = flags
        self._qhash = None
        self._system = None
        self._instance = None
        self._filters = []
        for key, val in flags.items():
            setattr(self, '_%s' % key, val)

        if  mongo_query:
            assert isinstance(mongo_query, dict)
            if  not 'spec' in mongo_query:
                mongo_query['spec'] = {}
            if  not 'fields' in mongo_query:
                mongo_query['fields'] = None
            self._mongo_query = mongo_query
            self._instance = self._mongo_query.get('instance', None)
            self._system = self._mongo_query.get('system', None)
            self._filters = self._mongo_query.get('filters', None)
        if  storage_query:
            assert isinstance(storage_query, dict)
            if  not 'spec' in storage_query:
                storage_query['spec'] = []
            if  not 'fields' in storage_query:
                storage_query['fields'] = None
            self._storage_query = storage_query
        if  query:
            self._query = query
            try:
                add_to_analytics = \
                    True if hasattr(self, '_add_to_analytics') else False
                self._mongo_query = self.mongoparser.parse(query, 
                            add_to_analytics)
                self._instance = self._mongo_query.get('instance', None)
                self._system = self._mongo_query.get('system', None)
                self._filters = self._mongo_query.get('filters', None)
            except Exception as exp:
                msg = "Fail to process query='%s'" % query
                print_exc(msg)
                raise exp

    @property
    def filters(self):
        "filters property of the DAS query"
        return self._filters

    @property
    def system(self):
        "system property of the DAS query"
        return self._system

    @property
    def instance(self):
        "instance property of the DAS query"
        return self._instance

    @property
    def query(self):
        "query property of the DAS query (human readble form)"
        if  not self._query:
            query  = ' '.join(self.mongo_query.get('fields', []))
            query += ' ' # space between fields and spec values
            query += ' '.join(['%s=%s' % (k.split('.')[0], v) for k, v \
                        in self.mongo_query.get('spec', {}).items()])
            self._query = query.strip()
        return self._query

    @property
    def storage_query(self):
        """
        Read only storage query, generated on demand.
        """
        if  not self._storage_query:
            self._storage_query = deepcopy(self._mongo_query)
            speclist = []
            for key, val in self._storage_query.pop('spec').items():
                if  str(type(val)) == "<type '_sre.SRE_Pattern'>":
                    val = json.dumps(val.pattern)
                    speclist.append({"key":key, "value":val, "pattern":1})
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
        if  not self._mongo_query:
            self._mongo_query = deepcopy(self._storage_query)
            spec = {}
            for item in self._mongo_query.pop('spec'):
                val = json.loads(item['value'])
                if  item.has_key('pattern'):
                    val = re.compile(val)
                spec.update({item['key'] : val})
            self._mongo_query['spec'] = spec
        return self._mongo_query
    
    @property
    def qhash(self):
        """
        Read only qhash, generated on demand.
        """
        if  not self._qhash:
            self._qhash = genkey(self.storage_query)
        return self._qhash
            
    def is_bare_query(self):
        """
        Return true if we only have 'fields' and 'spec' keys.
        """
        return sorted(self.mongo_query.keys()) == ['fields', 'spec']
            
    def to_bare_query(self):
        """
        Return a new query containing only field and spec keys of this query.
        May be identical if this is already a bare query.
        """
        if  self.is_bare_query():
            return self
        mongo_query = {'fields': copy.deepcopy(self.mongo_query['fields']),
                'spec': deepcopy(self.mongo_query['spec'])}
        return DASQuery(mongo_query=mongo_query, **self._flags)

    def is_loose_query(self):
        """
        Check if all text values in the spec are wildcarded.
        """
        return all([v[-1] == '*' 
                    for v in self.mongo_query['spec'].values() 
                    if isinstance(v, basestring)])

    def to_loose_query(self):
        """
        Construct loose version of the query. That means add 
        pattern '*' to string type values for all conditions.
        """
        query   = deepcopy(self.mongo_query)
        spec    = query.get('spec', {})
        fields  = query.get('fields', None)
        system  = query.get('system', None)
        inst    = query.get('instance', None)
        newspec = {}
        for key, val in spec.items():
            if  key != '_id' and \
            isinstance(val, str) or isinstance(val, unicode):
                if  val[-1] != '*':
                    val += '*' # add pattern
            newspec[key] = val
        mongo_query = dict(spec=newspec, fields=fields)
        return DASQuery(mongo_query=mongo_query, system=system, instance=inst)
    
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
        return DASQuery(mongo_query=nmq, **self._flags)
    
    def flag(self, name):
        """
        Test the state of any flags that were set.
        """
        return self._flags.get(name, False)
    
    def to_pattern(self):
        """
        Return a new DASQuery with the spec modified with
        $exists keys and regexes. This should replicate
        convert2pattern[0] - convert2pattern[1] doesn't seem
        to be used anywhere.
        """
        nmq = deepcopy(self.mongo_query)
        
        def edit_dict(old):
            """
            Inner recursive dictionary manipulator
            """
            result = {}
            for key, val in old.items():
                if isinstance(val, basestring):
                    if '*' in val:
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
        return DASQuery(mongo_query=nmq, **self._flags)
    
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
        return self.is_superset_of(other)
    
    def __lt__(self, other):
        return self.is_subset_of(other)
    
    def __eq__(self, other):
        return self.qhash == other.qhash
    
    def __str__(self):
        return self.query

    def __repr__(self):
        "Query representation"
        msg = "<query: %s, mongo_query: %s>" % (self.query, self.mongo_query)
        return msg
