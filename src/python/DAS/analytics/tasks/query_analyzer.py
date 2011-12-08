"""
Generates statistics about queries being run by service,
structure, presence of wildcards, time-of-day.

Information currently unavailable in analyticsDB that might be useful:
    user IP or similar to detect recurrent queries, or follow-on queries
    response time
    determine whether query was answered from the cache or otherwise
    aggregator or filter requests
       (stripped by the time a query is added to analytics)
        we could query parserdb to get this, maybe

"""
import json
import time
import collections
from DAS.utils.utils import genkey
from DAS.utils.logger import PrintManager

class QueryAnalyzer(object):
    "QueryAnalyzer class"
    task_options = []
    def __init__(self, **kwargs):
        self.das = kwargs['DAS']
        self.logger = PrintManager('QueryAnalyzer', kwargs.get('verbose', 0))
        self.interval = kwargs['interval']

    def __call__(self):
        "__call__ implementation"
        now = time.time()
        start = now-self.interval
        finish = now
        queries = self.das.analytics.list_queries(after=start, before=finish)
        
        query_map = {}
        counter = collections.defaultdict(int)
        
        for query in queries:
            mongoquery = query['mongoquery']
            result = {'fields': mongoquery.get('fields', []),
                      'instance': mongoquery.get('instance', 'unknown'),
                      'keys': {}}
            for keyval in mongoquery.get('spec', []):
                key = keyval.get('key', '').replace('.', '_')
                value = json.loads(keyval.get('value', 'null'))
                valuetype = "unknown"
                if isinstance(value, dict):
                    if "$gte" in value and "$lte" in value:
                        valuetype = "between_value"
                    elif "$gte" in value:
                        valuetype = "last_value"
                elif isinstance(value, basestring):
                    star_count = len([c for c in value if c=='*'])
                    if star_count == 1 and value[-1] == '*':
                        valuetype = "wildcard_end"
                    elif star_count > 0:
                        valuetype = "wildcard_%d" % star_count
                    else:
                        valuetype = "string"
                result['keys'][key] = valuetype
            qhashish = genkey(result)
            query_map[qhashish] = result
            counter[qhashish] += \
                len([1 for t in query['times'] if t > start and t < finish])
        
        summary = [(query_map[qh], counter[qh]) for qh in query_map]
        self.das.analytics.add_summary(\
                "query_analyzer", start=start, finish=finish, queries=summary)
