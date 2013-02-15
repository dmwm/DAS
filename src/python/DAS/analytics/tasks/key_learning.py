"""
Keylearning task manager
"""

import collections
from bson.objectid import ObjectId
from DAS.utils.logger import PrintManager

from pprint import pprint



class key_learning(object):
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
    task_options = [{'name':'redundancy', 'type':'int', 'default':2,
                     'help':'Number of records to examine per DAS primary key'}]
    def __init__(self, **kwargs):
        self.logger = PrintManager('KeyLearning', kwargs.get('verbose', 0))
        self.das = kwargs['DAS']
        self.redundancy = kwargs.get('redundancy', 10)
        
        
    def __call__(self):
        "__call__ implementation"
        self.das.rawcache.remove_expired("cache")
        
        autodeque = lambda: collections.deque(maxlen=self.redundancy)
        found_ids = collections.defaultdict(autodeque)
        
        self.logger.info("finding das_ids")
        for doc in self.das.rawcache.col.find(\
            {'das.empty_record': 0, 'das.primary_key': {'$exists': True}},
            fields=['das.primary_key', 'das_id']):
            for das_id in doc['das_id']:
                found_ids[doc['das']['primary_key']].append(das_id)
        
        hit_ids = set()
        
        self.logger.info("found %s primary_keys" % len(found_ids))
        
        for key in found_ids:
            self.logger.info("primary_key=%s" % key)
            for das_id in found_ids[key]:
                if False:
                    print '-======= DAS ID ======'
                    pprint(das_id)
                    print '-======= HIT ID (ALREADY VISITED) ======'
                    pprint(hit_ids)

                if not das_id in hit_ids:
                    self.logger.info("das_id=%s" % das_id)
                    hit_ids.add(das_id)
                    doc = self.das.rawcache.col.find_one(\
                        {'_id': ObjectId(das_id)})
                    if doc:
                        self.process_query_record(doc)
                    else:
                        self.logger.warning(\
                        "no record found for das_id=%s" % das_id)


        #from pprint import pprint
        print 'keylearning collection:', self.das.keylearning.col
        print 'result attributes (all):'
        for r  in self.das.keylearning.list_members():
            pprint(r)
            result_type = self.das.mapping.primary_key(r['system'], r['urn'])
            print r.get('keys', ''), '-->', result_type, ':', ', '.join([m for m in r.get('members', [])])

        return {}
    
    def process_query_record(self, doc):
        """
        Process a rawcache document, extracting the called
        system, urn and url, then looking up the individual data records.
        """
        das_id = str(doc['_id'])
        systems = doc['das']['system']
        urns = doc['das']['urn']
        
        result = self.das.rawcache.find_records(das_id)

        print 'in process_query_record. (das_id, systems, urns)=', (das_id, systems, urns)
        print 'result count=', result.count(), '~= systems=', len(systems)
        print 'len(systems)=', len(systems), '~= len(urns)', len(urns)

        #from pprint import pprint

        if False:
            #print 'doc:'
            pprint(doc)
            result_count = result.count()
            result = [r for r in result]
            print 'result:'
            pprint(result)
        print '-----------------------------------'
        # TODO: it seems these conditions are non-sense!!!
        if len(systems)==len(urns) and len(systems)==1:
            for i, record in enumerate(result):
                self.process_document(systems[0], urns[0], record)

        else:
            self.logger.warning("got inconsistent system/urn/das_id length")
            
            
    def process_document(self, system, urn, doc):
        """
        Process a rawcache document record, finding all the unique
        data members and inserting them into the cache.
        """
        
        self.logger.info("%s::%s" % (system, urn))
        members = set()
        for key in doc.keys():
            if not key in ('das', '_id', 'das_id'):
                members |= self._process_document_recursor(doc[key], key)

        if False:
            print 'process_document(): das.keylearning.add_members(system=',\
            system, ', urn=', urn , 'members:', list(members)
        self.das.keylearning.add_members(system, urn, list(members))
        
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
