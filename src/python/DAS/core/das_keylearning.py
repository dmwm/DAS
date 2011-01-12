# DAS modules
from DAS.utils.das_db import db_connection
import collections

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
        self.logger   = config['logger']
        self.verbose  = config['verbose']
        self.services = config['services']
        self.dburi    = config['mongodb']['dburi']
        self.dbname   = config['keylearningdb']['dbname']
        self.colname  = config['keylearningdb']['collname']
        
        self.mapping  = config['dasmapping']

        msg = "DASKeyLearning::__init__ %s@%s" % (self.dburi, self.dbname)
        self.logger.info(msg)
        
        self.col = None
        self.create_db()
        
        

    def create_db(self):
        """
        Establish connection to MongoDB back-end and create DB.
        """
        self.col = db_connection(self.dburi)[self.dbname][self.colname]
        
    def add_members(self, system, urn, members):
        """
        Add a list of data members for a given API (system, urn, url),
        and generate, which are stored as separate records.
        """
        msg = "DASKeyLearning::add_members(%s, %s, %s)" % \
                                        (system, urn, members)
        self.logger.info(msg)
        
        result = self.col.find_one({'system': system, 'urn': urn})
        if result:       
            self.col.update({'_id': result['_id']},
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
                
        self.col.ensure_index([('system', 1),
                               ('urn', 1), 
                               ('members', 1),
                               ('stems', 1)])
            
        
    def stem(self, member):
        """
        Produce an extended set of strings which can be used for text-search.
        TODO: Use PyStemmer or something more sophisticated here.
        """
        
        return member.lower().split('.')
    
    def text_search(self, text):
        """
        Perform a text search for data members matching a string. The input is
        split if it already includes dotted elements (in which case we need to find
        a member matching all the split elements), otherwise we look for any member
        whose stem list contains the text.
        """
        text = text.lower()
        if '.' in text:
            possible_members = self.col.find({'stems': {'$all': text.split('.')}}, 
                                             fields=['member'])
        else:
            possible_members = self.col.find({'stems': text}, 
                                             fields=['member'])
        
        return [doc['member'] for doc in possible_members]
        
    
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
            