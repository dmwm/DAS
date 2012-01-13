#pylint: disable-msg=R0903
"""
Query Runner class
"""
from DAS.utils.logger import PrintManager
from DAS.core.das_query import DASQuery

class QueryRunner(object):
    "Replaces das_robot"
    task_options = [{'name':'query', 'type':'string', 'default':None,
                   'help':'Query to issue using das_core::call'}]
    def __init__(self, **kwargs):
        self.logger = PrintManager('QueryRunner', kwargs.get('verbose', 0))
        self.das = kwargs['DAS']
        self.dasquery = DASQuery(kwargs['dasquery'])
    def __call__(self):
        "__call__ implementation"
        self.logger.info("Issuing query %s" % self.dasquery)
        result = self.das.call(self.dasquery, add_to_analytics=False)
        return {'result':result}
