import time
import collections

class Hotspot(object):
    """
    This analyzer identifies all arguments to a given primary key,
    selects the top _fraction_ keys. If mode is "calls", the selected
    keys are the keys containing the top fraction of all calls,
    whereas if the mode is "keys", the selection is just the top
    fraction of all keys sorted by number of calls.
    
    The calls to be considered are defined by _period_ (default 1month).
    
    The optional argument 'allowed_gap' is the maximum gap in
    the summary record we are happy to ignore (default 1h).
    
    The summary identifier is "hotspot-key"
    It spawns queryrunner jobs of name "hotspot-key-foundkey" which issue
    the query "key=foundkey" at hourly intervals until the next run of this
    analyzer. This should attempt to determine the expiry time out of the
    analytics DB but currently does not do so.
    """
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.das = kwargs['DAS']
        self.key = kwargs['key']
        self.fraction = kwargs.get('fraction', 0.15)
        self.mode = kwargs.get('mode','calls').lower()
        self.period = kwargs.get('period', 86400*30)
        self.interval = kwargs['interval']
        self.allowed_gap = kwargs.get('allowed_gap', 3600)
        self.identifier = "hotspot-%s" % (self.key)
    def __call__(self):
        epoch_end = time.time()
        epoch_start = time.time() - self.period
        
        #get all the summaries we can from this time
        summaries = self.das.analytics.get_summary(self.identifier, 
                                                   start=epoch_start)
        self.logger.info("Found %s summary documents.", len(summaries))  
        #see how much coverage of the requested period we have
        summaries = sorted(summaries, key=lambda x: x['start'])
        extra_summaries = []
        last_time = epoch_start
        for summary in summaries:
            if last_time < summary['start']:                
                result = self.make_summary(last_time, summary['start'])
                extra_summaries.extend(result)
            last_time = summary['finish']
        result = self.make_summary(last_time, epoch_end)
        extra_summaries.extend(result)
        summaries = [s['keys'] for s in summaries]
        summaries += extra_summaries
        
        self.logger.info("Starting analysis.")
        
        keys = collections.defaultdict(int)
        for summary in summaries:
            for k,v in summary.items():
                keys[k] += v
        
        sorted_keys = sorted(keys.keys(), key=lambda x: keys[x], reverse=True)
        selected_keys = []
        if self.mode == 'calls':
            total_calls = sum(keys.values())
            running_total = 0
            for key in sorted_keys:
                running_total += keys[key]
                selected_keys += [key]
                if running_total > total_calls * self.fraction:
                    break
        elif self.mode == 'keys':
            selected_keys = sorted_keys[0:int(len(sorted_keys)*self.fraction)]
        self.logger.info("In '%s' mode, selected %s of %s.", 
                         self.mode, len(selected_keys), len(sorted_keys))
        
        # here we should actually determine the expiry time based
        # on apicall records and re-request at that point
        new_tasks = [{'classname': 'QueryRunner',
                      'name': 'hotspot-%s-%s' % (self.key, key),
                      'interval': 3600,
                      'only_before': epoch_end + self.interval,
                      'kwargs':{'query': '%s=%s' % (self.key, key)}} 
                      for key in selected_keys]
        
        result = {'selected_keys': selected_keys,
                  'all_keys': dict(keys),
                  'new_tasks': new_tasks}
        return result
        
            
    
    def make_summary(self, start, finish):
        """
        Split the summarise requests into interval-sized chunks and decide
        if they're necessary at all
        """
        self.logger.info("Found summary gap: %s->%s (%s)", 
                         start, finish, finish-start)
        result = []
        delta = finish - start
        if delta > self.allowed_gap:
            if delta > self.interval:
                blocks = int(delta/self.interval)
                span = delta/blocks
                self.logger.info("Gap longer than interval, creating %s summaries.", blocks)
                return [self._make_summary(start+span*i, 
                                           start+span*(i+1)) 
                        for i in range(blocks)]
            else:
                return [self._make_summary(start, finish)]
        else:
            self.logger.info("...short enough to ignore.")
        return result
    
    def _make_summary(self, start, finish):
        "Actually make the summary"
        
        keys = collections.defaultdict(int)
        queries = self.das.analytics.list_queries(key=self.key,
                                                     after=start,
                                                     before=finish)
        self.logger.info("Found %s queries in %s->%s", 
                         len(queries), start, finish)
        for query in queries:
            for spec in query['mongoquery']['spec']:
                if spec['key'] == self.key:
                    keys[spec['value']] += 1
        
        self.das.analytics.add_summary(self.identifier,
                                       start,
                                       finish,
                                       keys=dict(keys))
        return keys
        
                
            