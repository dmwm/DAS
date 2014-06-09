# pymongo modules
from bson.objectid import ObjectId
# DAS modules
from DAS.core.das_ql import DAS_RECORD_KEYS
from DAS.utils.das_db import db_connection, create_indexes
from DAS.utils.logger import PrintManager
import collections

def dict_members(data, prefix):
    """
    Extract all key/attributes from given data structure
    and update members container based on given key prefix
    """
    members = set()
    for rdict in data:
        if  not isinstance(rdict, dict):
            members.add(prefix)
            continue
        for key, val in rdict.items():
            ckey = '%s.%s' % (prefix, key)
            if  isinstance(val, list):
                for row in dict_members(val, ckey):
                    members.add(row)
            elif  isinstance(val, dict):
                for row in dict_members([val], ckey):
                    members.add(row)
            else:
                members.add(ckey)
    return list(members)

class DASKeyLearning(object):
    """
    This class manages DAS key-learning DB.

    Key-learning is an intermittent process (triggered infrequently
    by a task running in the analytics framework), which involves
    searching through the raw cache for (a subset of but with
    maximum primary key coverage) all output documents, generating
    the set of all data members (in a dotted-dict fashion) and storing
    those as primary-key:data-member records (with an associated
    last-updated-time).

    """
    def __init__(self, config):
        self.verbose  = config['verbose']
        self.logger   = PrintManager('DASKeyLearning', self.verbose)
        self.services = config['services']
        self.dburi    = config['mongodb']['dburi']
        self.dbname   = config['keylearningdb']['dbname']
        self.colname  = config['keylearningdb']['collname']

        self.mapping  = config['dasmapping']

        msg = "%s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)

        self.col = None
        self.create_db()
        index_list = [('system', 1), ('urn', 1), ('members', 1), ('stems', 1)]
        create_indexes(self.col, index_list)

    def create_db(self):
        """
        Establish connection to MongoDB back-end and create DB.
        """
        self.col = db_connection(self.dburi)[self.dbname][self.colname]

    def add_record(self, dasquery, rec):
        """
        Add/update to keylearning DB keys/attributes from given record.
        To do so, we parse it and call add_members method.
        """
        if  not ('das' in rec and 'system' in rec['das']):
            return
        das = rec['das']
        if  'system' not in das or 'api' not in das or 'primary_key' not in das:
            return
        systems = das['system']
        apis = das['api']
        pkey = das['primary_key'].split('.')[0]
        data = rec.get(pkey, [])
        members = dict_members(data, pkey)
        for srv, api in zip(systems, apis):
            self.add_members(srv, api, members)
        # insert new record for query patern
        fields = dasquery.mongo_query.get('fields', [])
        if  fields:
            for field in fields:
                if  field in DAS_RECORD_KEYS:
                    continue
                new_members = [m for m in dict_members(rec[field], field) if m]
                members += new_members
        for attr in members:
            spec = {'member': attr}
            doc = {'query_pat': dasquery.query_pat}
            self.col.update(spec, {'$addToSet': doc}, upsert=True)

    def add_members(self, system, urn, members):
        """
        Add a list of data members for a given API (system, urn, url),
        and generate, which are stored as separate records.
        """
        msg = "system=%s, urn=%s, members=%s)" % (system, urn, members)
        self.logger.info(msg)

        result = self.col.find_one({'system': system, 'urn': urn})
        if result:
            self.col.update({'_id': ObjectId(result['_id'])},
                            {'$addToSet': {'members': {'$each': members}}})
        else:
            keys = self.mapping.api2daskey(system, urn)
            self.col.insert({'system': system,
                             'urn': urn,
                             'keys': keys,
                             'members': members})

        for member in members:
            if not self.col.find_one({'member': member}):
                self.col.insert({'member': member,
                                 'stems': self.stem(member)})

    def stem(self, member):
        """
        Produce an extended set of strings which can be used for text-search.
        TODO: Use PyStemmer or something more sophisticated here.
        """

        return member.lower().split('.')

    def text_search(self, text):
        """
        Perform a text search for data members matching a string. The input is
        split if it already includes dotted elements (in which case we need to
        find a member matching all the split elements), otherwise we look for
        any member whose stem list contains the text.
        """
        text = text.lower()
        if '.' in text:
            possible_members = self.col.find(
                    {'stems': {'$all': text.split('.')}}, fields=['member'])
        else:
            possible_members = self.col.find({'stems': text},
                                             fields=['member'])
        return [doc['member'] for doc in possible_members]

    def attributes(self):
        """
        Return full list of keyword attributes known in DAS.
        """
        spec = {'member':{'$exists':True}}
        return self.col.find(spec)

    def member_info(self, member):
        """
        Once the text search has identified a member that might be a match,
        return which systems, APIs and hence DAS keys this points to.
        """
        result = []
        for doc in self.col.find({'members': member},
                                 fields=['system', 'urn', 'keys']):

            result.append({'system': doc['system'],
                           'urn': doc['urn'],
                           'keys': doc['keys']})
        return result

    def key_search(self, text, limitkey=None):
        """
        Try and find suggested DAS keys, by performing a member search and then
        mapping back to the DAS keys those are produced by.
        """
        text = text.lower()
        result = collections.defaultdict(set)
        for member in self.text_search(text):
            for info in self.member_info(member):
                result[tuple(info['keys'])].add(member)
        if limitkey:
            for key in result:
                if not limitkey in key:
                    del result[key]
        return result

    def members_for_keys(self, keys):
        """
        Return all the members that exactly match the set of keys
        """
        result = []
        for doc in self.col.find({'keys': {'$all': keys, '$size': len(keys)}},
                                 fields=['members']):
            result += doc['members']
        return result


    def has_member(self, member):
        """
        Return true if we know anything about the given member.
        """
        if self.col.find_one({'member': member}):
            return True
        else:
            return False

    def list_members(self):
        return self.col.find({'members': {'$exists': 'True'},
                              'system': {'$exists': 'True'},
                              'urn': {'$exists': 'True'}})
        #{ urn: {$exists: 1}, system: {$exists: 1}, members: {$exists: 1}}
