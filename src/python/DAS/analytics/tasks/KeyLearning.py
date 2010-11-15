import collections
from pymongo.objectid import ObjectId

class KeyLearning:
    """
    This is the asynchronous part of the key-learning system, intended
    to run probably not much more than daily once the key learning DB is
    filled.
    
    This searches through the DAS raw cache for all API output records,
    recording at least `redundancy` das_ids for each primary_key found.
    
    These das_ids are then used to fetch the query record, which records
    the API system and urn of each of the records in question.
    
    These documents are then processed to extract all the unique member
    names they contained, which are then injected into the DAS keylearning
    system.
    """
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.DAS = kwargs['DAS']
        self.redundancy = kwargs.get('redundancy', 2)
        
        
    def __call__(self):
        
        self.logger.info("KeyLearning::__call__")
        
        self.DAS.rawcache.remove_expired("cache")
        
        autodeque = lambda: collections.deque(maxlen=self.redundancy)
        found_ids = collections.defaultdict(autodeque)
        
        self.logger.info("KeyLearning::__call__ finding das_ids")
        for doc in self.DAS.rawcache.col.find({'das.empty_record': 0,
                                               'das.primary_key': {'$exists': True}},
                                               fields=['das.primary_key',
                                                       'das_id']):
            found_ids[doc['das']['primary_key']].append(doc['das_id'])
        
        hit_ids = set()
        
        self.logger.info("KeyLearning::__call__ found %s primary_keys",
                         len(found_ids))
        
        for key in found_ids:
            self.logger.info("KeyLearning::__call__ primary_key=%s",
                             key)
            for das_id in found_ids[key]:
                if not das_id in hit_ids:
                    self.logger.info("KeyLearning::__call__ das_id=%s",
                                     das_id)
                    hit_ids.add(das_id)
                    doc = self.DAS.rawcache.col.find_one({'_id': ObjectId(das_id)})
                    if doc:
                        self.process_query_record(doc)
                    else:
                        self.logger.warning("KeyLearning::__call__ no record found for das_id=%s",
                                            das_id)
        
        return {}
    
    def process_query_record(self, doc):
        """
        Process a rawcache document, extracting the called
        system, urn and url, then looking up the individual data records.
        """
        
        self.logger.info("KeyLearning::process_query_record")
        
        das_id = str(doc['_id'])
        systems = doc['das']['system']
        urns = doc['das']['urn']
        
        result = self.DAS.rawcache.find_records(das_id)        
        
        if len(systems)==len(urns) and len(systems)==result.count():
            for i, record in enumerate(result):
                self.process_document(systems[i], urns[i], record)
        else:
            self.logger.warning("KeyLearning::process_query_record got inconsistent system/urn/das_id length")
            
            
    def process_document(self, system, urn, doc):
        """
        Process a rawcache document record, finding all the unique
        data members and inserting them into the cache.
        """
        
        self.logger.info("KeyLearning::process_document %s::%s",
                         system, urn)
        
        members = set()
        for key in doc.keys():
            if not key in ('das', '_id', 'das_id'):
                members |= self._process_document_recursor(doc[key], key)
        
        self.DAS.keylearning.add_members(system, urn, list(members))
        
    def _process_document_recursor(self, doc, prefix):
        """
        Recurse through a nested data structure, finding all
        the unique endpoint names. Lists are iterated over but do
        not add anything to the prefix, eg
        
        a: {b: 1, c: {d: 1, e: 1}, f: [{g: 1}, {h: 1}]} ->
        a.b, a.c.d, a.c.e, a.f.g, a.f.h
        
        (although normally we would expect each member of a list to
        have the same structure)
        """
        result = set()
        if isinstance(doc, dict):
            for key in doc.keys():
                result |= self._process_document_recursor(doc[key], 
                                                          prefix+'.'+key)
        elif isinstance(doc, list):
            for item in doc:
                result |= self._process_document_recursor(item, prefix)
        else:
            result.add(prefix)
        return result