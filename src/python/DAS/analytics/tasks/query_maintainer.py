"""
Query Maintainer

This takes a query (in mongo form) and attempts to ensure
that it remains in the cache, until some termination time.

TODO: Atomic updates, instead of delete-then-requery
"""

import time
from DAS.core.das_query import DASQuery

class QueryMaintainer(object):
    "Ensures a query remains in the cache"
    task_options = [{'name':'query', 'type':'string', 'default':None,
                     'help':'The query to be maintained'},
                    {'name':'preempt', 'type':'int', 'default':60,
     'help':'Number of seconds before expiry we try and refresh the data'}]
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.das = kwargs['DAS']
        self.dasquery = DASQuery(kwargs['dasquery'])
        self.preempt = kwargs.get('preempt', 60) #default 1 minute
        
    def __call__(self):
        """
        This method should run shortly before the data for
        a given query expires in the cache, forcibly replace
        the existing data with new data and then reschedule
        itself to run shortly before the next expiry time.
        """
        
        self.logger.info(\
        "Query %s QHash %s", self.dasquery, self.dasquery.qhash)
        
        #delete old data from cache, if any
        self.logger.info("Deleting query %s", self.dasquery)
        self.das.remove_from_cache(self.dasquery)
        
        #remake query
        
        self.logger.info("Re-issuing query %s", self.dasquery)
        status = self.das.call(self.dasquery, add_to_analytics=False)
        if status:
            
            #read result for expiry            
            expiries = [result.get('apicall', {}).get('expire', 0) 
                        for result in \
                self.das.analytics.list_apicalls(qhash=self.dasquery.qhash)]
            
            if expiries:
                expiry_time = min(expiries)
                now = time.time()
                schedule = max(now, expiry_time - self.preempt)
                self.logger.info(\
                "Found minimum expiry time %s (%s), scheduling at %s (%s)", 
                                 expiry_time, int(expiry_time - now),
                                 schedule, int(schedule - now))
                
                #reschedule in future
                #specifying 'next' forces the absolute re-run time 
                return {'next': schedule}
            
                
            
            else:
                self.logger.warning("Find expiry time failed, no results")
                raise Exception("find-expiry-failed")
        else:
            self.logger.warning("Query failed, status=%s", status)
            raise Exception("query-failed")
