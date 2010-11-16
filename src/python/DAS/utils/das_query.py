
from DAS.utils.utils import genkey
import copy
import DAS.utils.jsonwrapper as json
from pymongo.objectid import ObjectId
import re
from DAS.core.das_mongocache import compare_specs

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
        
        self._storage_query = None
        self._mongo_query = None
        self._flags = flags
        
        self._qhash = None
        
        if not mongo_query == None:
            assert isinstance(mongo_query, dict)
            if not 'spec' in mongo_query:
                mongo_query['spec'] = {}
            if not 'fields' in mongo_query:
                mongo_query['fields'] = None
            self._mongo_query = mongo_query
        if not storage_query == None:
            assert isinstance(storage_query, dict)
            if not 'spec' in storage_query:
                storage_query['spec'] = []
            if not 'fields' in storage_query:
                storage_query['fields'] = None
            self._storage_query = storage_query
        if not query == None:
            assert isinstance(query, dict)
            assert 'spec' in query and isinstance(query['spec'], (dict, list, tuple))
            if not 'fields' in query:
                query['fields'] = None
            if isinstance(query['spec'], dict):
                self._mongo_query = query
            else:
                self._storage_query = query
                
    @property
    def storage_query(self):
        """
        Read only storage query, generated on demand.
        """
        if not self._storage_query:
            self._storage_query = copy.deepcopy(self._mongo_query)
            self._storage_query['spec'] = [{'key':k, 'value':json.dumps(v)} 
                                           for k, v in self._storage_query['spec'].items()]
        return self._storage_query
        
    @property
    def mongo_query(self):
        """
        Read only mongo query, generated on demand.
        """
        if not self._mongo_query:
            self._mongo_query = copy.deepcopy(self._storage_query)
            self._mongo_query['spec'] = dict([(i['key'], json.loads(i['value'])) 
                                              for i in self._mongo_query['spec']])
        return self._mongo_query
    
    @property
    def qhash(self):
        """
        Read only qhash, generated on demand.
        """
        if not self._qhash:
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
        
        TODO: Return self if self.is_bare_query() ?
        """
        return DASQuery(mongo_query={'fields': copy.deepcopy(self.mongo_query['fields']),
                                     'spec': copy.deepcopy(self.mongo_query['spec'])},
                        **self._flags)
        
    def is_loose_query(self):
        """
        Check if all text values in the spec are wildcarded.
        """
        return all([v[-1] == '*' 
                    for v in self.mongo_query['spec'].values() 
                    if isinstance(v, basestring)])
        
    def to_loose_query(self):
        """
        Append wildcards to all text values in the spec.
        """
        nmq = copy.deepcopy(self.mongo_query)
        spec = {}
        for k, v in nmq['spec'].items():
            if isinstance(v, basestring) and not v[-1] == '*':
                v += '*'
            else:
                v = copy.deepcopy(v)
            spec[k] = v
        nmq['spec'] = spec
        return DASQuery(mongo_query=nmq, **self._flags)
    
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
        nmq = copy.deepcopy(self.mongo_query)
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
        nmq = copy.deepcopy(self.mongo_query)
        
        def edit_dict(old):
            """
            Inner recursive dictionary manipulator
            """
            result = {}
            for k, v in old.items():
                if isinstance(v, basestring):
                    if '*' in v:
                        if len(v)==1:
                            result[k] = {'$exists': True}
                        else:
                            result[k] = re.compile('^%s'%v.replace('*', '.*'))
                elif isinstance(v, dict):
                    result[k] = edit_dict(v)
                else:
                    result[k] = v                
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
        return json.dumps(self.mongo_query)