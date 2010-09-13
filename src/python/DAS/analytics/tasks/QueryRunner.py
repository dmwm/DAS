"""
Query Runner class
"""

class QueryRunner(object):
    "Replaces das_robot"
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.das = kwargs['DAS']
        self.query = kwargs['query']
    def __call__(self):
        self.logger.info("Issuing query %s", self.query)
        result = self.das.call(self.query, add_to_analytics=False)
        return {'result':result}
