import time
import collections
import copy
import fnmatch
from DAS.utils.utils import genkey
from DAS.core.das_mongocache import decode_mongo_query
from DAS.analytics.tasks.HotspotBase import HotspotBase

class ValueHotspot(HotspotBase):
    """
    This analyzer identifies all arguments to a given primary key,
    selects the top _fraction_ keys. If mode is "calls", the selected
    keys are the keys containing the top fraction of all calls,
    whereas if the mode is "keys", the selection is just the top
    fraction of all keys sorted by number of calls. Finally, you
    can use mode "fixed", in which case the _fraction_ kwarg should
    be a number >= 1 and that many keys are selected (provided they
    exist).
    
    The calls to be considered are defined by _period_ (default 1month).
    
    The optional argument 'allowed_gap' is the maximum gap in
    the summary record we are happy to ignore (default 1h).
    
    The summary identifier is "valuehotspot-key"
    It spawns querymaintainer jobs of name "valuehotspot-foundkey" which
    try and sensibly maintain the given query in cache.
    
    If the option "allow_wildcarding" is given, queries containing
    a wildcard will be considered. Otherwise, these will be ignored.
    
    If the option "find_supersets" is given, it will try and find
    superset queries (already in the cache) answering multiple hot keys
    and have them maintained instead of the specific one.
    
    The option "preempt" controls how long before data expiry the 
    re-fetch is scheduled. By default this is 15 minutes (which may 
    be inappropriate for some fast-flux data).
    
    """
    def __init__(self, **kwargs):
        self.key = kwargs['key']
        self.allow_wildcarding = kwargs.get('allow_wildcarding', False)
        self.find_supersets = kwargs.get('find_supersets', False)
        self.preempt = kwargs.get('preempt', 300)
        HotspotBase.__init__(self,
                             identifier="valuehotspot-%s" % \
                             (self.key.replace('.','-')),
                             **kwargs)
    
    def generate_task(self, item, count, epoch_start, epoch_end):
        only_before = epoch_end + self.interval
        query = {'fields': None, 'spec': [{'key':self.key, 'value': item}]}
        expiry = self.get_query_expiry(query)
        schedule = expiry - self.preempt
        interval = schedule - time.time()
        itemname = item.replace('"','')
        if schedule < only_before:
            return {'classname': 'QueryMaintainer',
                    'name': '%s-%s' % (self.identifier, itemname),
                    'only_before': only_before,
                    'interval': interval,
                    'kwargs':{'query':query,
                              'preempt':self.preempt}}
    
    def preselect_items(self, items):
        if not self.allow_wildcarding:
            for key in items.keys():
                if '*' in key:
                    del items[key]
        return items
    
    def mutate_items(self, items):
        if self.find_supersets:
            new_keys = self.get_superset_keys(items.keys())
            return dict([(k, items.get(k, 0)) for k in new_keys])
        else:
            return items
    
    def get_superset_keys(self, keys):
        """
        For multiple keys, try and identify an existing queries for
        wildcard supersets of the keys, and reduce the keylist appropriately.
        Important to note, this only uses existing queries (won't try
        and make new ones).
        """
        superset_cache = {}
        
        keys = set(keys)
        change_made = True
        while change_made:
            change_made = False
            for key in list(keys):
                if key in superset_cache:
                    superset_keys = superset_cache[key]
                else:
                    superset_keys = sorted([k for k in self.das.rawcache.get_superset_keys(self.key, key)], key=len)
                if superset_keys:
                    super_key = superset_keys[0]
                    for key in keys:
                        if fnmatch.fnmatch(super_key, key):
                            keys.remove(key)
                            keys.add(super_key)
                            change_made = True
                    if change_made:
                        break
        return keys
    
    def get_query_expiry(self, query):
        qhash = genkey(query)
        mongoquery = decode_mongo_query(copy.deepcopy(query))
        
        if not self.das.rawcache.incache(mongoquery):
            self.das.call(mongoquery, add_to_analytics=False)
            
        expiries = [result.get('apicall',{}).get('expire',0) for result in self.das.analytics.list_apicalls(qhash=qhash)]
        
        return min(expiries)
              
    def make_one_summary(self, start, finish):
        "Actually make the summary"
        
        keys = collections.defaultdict(int)
        queries = self.das.analytics.list_queries(key=self.key,
                                                  after=start,
                                                  before=finish)
        
        for query in queries:
            count = len(filter(lambda t: t>=start and t<=finish, 
                               query['times']))
            for spec in query['mongoquery']['spec']:
                if spec['key'] == self.key:
                    keys[spec['value']] += count
        
        if keys:
            self.logger.info("Found %s queries in %s->%s", 
                             len(keys), start, finish)
        else:
            self.logger.info("Found no queries in %s->%s",
                             start, finish)
        return keys
        
                
            