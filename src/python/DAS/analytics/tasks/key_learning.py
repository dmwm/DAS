"""
Keylearning task manager
"""

import collections
from bson.objectid import ObjectId
from DAS.utils.logger import PrintManager
from DAS.utils.utils import record_codes

from pprint import pprint


_DEBUG = False


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
    task_options = [
        {'name': 'redundancy',
         'type': 'int',
         'default': 2,
         'help': 'Number of records to examine per DAS primary key'}]

    def __init__(self, **kwargs):
        self.logger = PrintManager('KeyLearning', kwargs.get('verbose', 0))
        self.das = kwargs['DAS']
        self.redundancy = kwargs.get('redundancy', 10)

    def __call__(self):
        """__call__ implementation"""
        self.das.rawcache.clean_cache("cache")
        rawcache = self.das.rawcache.col
        autodeque = lambda: collections.deque(maxlen=self.redundancy)
        found_ids = collections.defaultdict(autodeque)

        self.logger.info("finding das_ids")
        for doc in rawcache.find({'das.record': record_codes('data_record'),
                                  'das.primary_key': {'$exists': True}},
                                 fields=['das.primary_key', 'das_id']):
            for das_id in doc['das_id']:
                found_ids[doc['das']['primary_key']].append(das_id)

        hit_ids = set()
        self.logger.info("found %s primary_keys" % len(found_ids))
        for key in found_ids:
            self.logger.info("primary_key=%s" % key)
            for das_id in found_ids[key]:
                if _DEBUG:
                    print '-======= DAS ID ======'
                    pprint(das_id)
                    print '-======= HIT ID (ALREADY VISITED) ======'
                    pprint(hit_ids)

                if not das_id in hit_ids:
                    self.logger.info("das_id=%s" % das_id)
                    hit_ids.add(das_id)
                    doc = rawcache.find_one({'_id': ObjectId(das_id)})
                    if doc:
                        self.process_query_record(doc)
                    else:
                        self.logger.warning("no record for das_id=%s" % das_id)

        if _DEBUG:
            print 'result attributes (all):'
            for row in self.das.keylearning.list_members():
                pprint(row)
                res_t = self.das.mapping.primary_key(row['system'], row['urn'])
                print row.get('keys', ''), '-->', res_t, ':', \
                    ', '.join([m for m in row.get('members', [])])

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

        if _DEBUG:
            print 'in process_query_record. (das_id, systems, urns)=', \
                (das_id, systems, urns)
            print 'result count=', result.count(), '~= systems=', len(systems)
            print 'len(systems)=', len(systems), '~= len(urns)', len(urns)

        if _DEBUG:
            print 'doc:'
            pprint(doc)
            result = [r for r in result]
            print 'results in doc:'
            pprint(result)
            print '-----------------------------------'

        # TODO: it seems these conditions are non-sense!!!
        if len(systems) == len(urns) and len(systems) == 1:
            for _, record in enumerate(result):
                self.process_document(systems[0], urns[0], record)
        else:
            self.logger.warning("got inconsistent system/urn/das_id length")

    def process_document(self, system, urn, doc):
        """
        Process a rawcache document record coming from one API of a service.
        Find all the unique output fields and insert them into the cache.
        """
        self.logger.info("%s::%s" % (system, urn))
        members = set()
        for key in doc.keys():
            if key not in ('das', '_id', 'das_id'):
                members |= self.recursive_walk(doc[key], key)

        if _DEBUG:
            print 'process_document(): das.keylearning.add_members(system=', \
                system, ', urn=', urn, 'members:', list(members)
        self.das.keylearning.add_members(system, urn, list(members))

    @classmethod
    def recursive_walk(cls, doc, prefix):
        """
        Recurse through a nested data structure, finding all
        the unique endpoint names. Lists are iterated over but do
        not add anything to the prefix, eg.:

        a: {b: 1, c: {d: 1, e: 1}, f: [{g: 1}, {h: 1}]} ->
        a.b, a.c.d, a.c.e, a.f.g, a.f.h

        (although normally we would expect each member of a list to
        have the same structure)
        """
        result = set()
        if isinstance(doc, dict):
            for key in doc.keys():
                result |= cls.recursive_walk(doc[key], prefix + '.' + key)
        elif isinstance(doc, list):
            for item in doc:
                result |= cls.recursive_walk(item, prefix)
        else:
            result.add(prefix)
        return result


if __name__ == '__main__':
    import doctest
    doctest.testmod()