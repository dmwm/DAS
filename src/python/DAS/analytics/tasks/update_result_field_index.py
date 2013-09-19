"""
Keylearning task manager
"""

import collections
from bson.objectid import ObjectId
from DAS.utils.logger import PrintManager

from pprint import pprint



class update_result_field_index(object):
    """
    updates whoosh IR-based index of list of fields contained in API results...
    """
    task_options = [{'name':'redundancy', 'type':'int', 'default':2,
                     'help':'Number of records to examine per DAS primary key'}]
    def __init__(self, **kwargs):
        self.logger = PrintManager('UpdateResultFieldIndex_whoosh', kwargs.get('verbose', 0))
        self.das = kwargs['DAS']
        self.redundancy = kwargs.get('redundancy', 10)
        
        
    def __call__(self):
        "__call__ implementation"

        self.logger.info("updating keyword search index")

        from DAS.keywordsearch.metadata import schema_adapter_factory as s_fact
        from DAS.keywordsearch.whoosh.ir_entity_attributes import build_index

        schema_adapter = s_fact.getSchema(self.das)
        rfields = schema_adapter.list_result_fields()
        build_index(rfields)

        return {}

