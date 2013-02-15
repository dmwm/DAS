"""
Keylearning task manager
"""

import collections
from bson.objectid import ObjectId
from DAS.utils.logger import PrintManager

from pprint import pprint



class update_result_field_index(object):
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

        self.logger.info("updating keyword search index")

        from DAS.keywordsearch import das_schema_adapter
        from DAS.keywordsearch.whoosh.service_fields import build_index

        das_schema_adapter.init(self.das)
        rfields = das_schema_adapter.list_result_fields()
        build_index(rfields)

        return {}

