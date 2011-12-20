#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0301
"""
ValueHotspot implementation
"""

import time
import collections
import fnmatch
from DAS.utils.utils import genkey
from DAS.core.das_core import DASQuery
from DAS.analytics.tasks.hotspot_base import HotspotBase
from DAS.analytics.utils import get_mongo_query
from DAS.utils.logger import PrintManager

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
    re-fetch is scheduled. By default this is 1 minute (which may 
    be inappropriate for some fast-flux data).
    
    """
    task_options = [{'name':'key', 'type':'string', 'default':None,
                   'help':'DAS primary key to work with'},
                  {'name':'fraction', 'type':'float', 'default':0.15,
                   'help':'Fraction of queries to satisfy (in calls mode) or fraction of all keys (in keys mode) or absolute number of keys (in fixed mode)'},
                  {'name':'mode', 'type':'string', 'default':'calls',
                   'help':'How to interpret the _fraction_ argument'},
                  {'name':'period', 'type':'int', 'default':30*24*60*60,
                   'help':'Duration over which the moving average is performed (in sec)'},
                  {'name':'allowed_gap', 'type':'int', 'default':60*60,
                   'help':'Ignore gaps in summary records of up to this many seconds (<< _period_)'},
                  {'name':'allow_wildcarding', 'type':'bool', 'default':'true',
                   'help':'Whether to consider wildcard queries. Probably best with _find_supersets_'},
                  {'name':'find_supersets', 'type':'bool', 'default':'true',
                   'help':'If wildcard queries are allowed, identify and remove redundant wildcard queries'},
                  {'name':'preempt', 'type':'int', 'default':60,
                   'help':'Cache filling jobs are spawned to refresh data this many seconds before expiry'},
                  {'name':'fields', 'type':'list', 'default':None,
                   'help':'Fields that should be queried, each resulting in "field key=<value>". An attempt will be made to determine them from the mapping if unspecified.'},
                  {'name':'instance', 'type':'string', 'default':'cms_dbs_prod_global',
                   'help':'DBS instance to include in queries'}]
    
    def __init__(self, **kwargs):
        self.key = kwargs['key']
        self.logger = PrintManager('ValueHotspot', kwargs.get('verbose', 0))
        self.allow_wildcarding = kwargs.get('allow_wildcarding', False)
        self.find_supersets = kwargs.get('find_supersets', False)
        self.preempt = int(kwargs.get('preempt', 60))
        self.fields = kwargs.get('fields', None)
        self.instance = kwargs.get('instance', 'cms_dbs_prod_global')
        
        HotspotBase.__init__(self,
                             identifier="valuehotspot-%s" % \
                             (self.key.replace('.','-')),
                             **kwargs)
        
        # set fields if look-up key is present
        if  not self.fields and self.key:
            self.fields = [self.key.split('.')[0]]

        # finally if fields is not yet set, look-up all DAS keys allowed
        # for given query
        if  not self.fields:
            try:
                self.fields = set()
                self.das.mapping.init_presentationcache()
                plist = self.das.mapping.presentation(self.key.split('.', 1)[0])
                for item in plist:
                    if 'link' in item:
                        for link in item['link']:
                            if len(link['query'].split(' ')) == 2:
                                self.fields.add(link['query'].split(' ')[0])
                self.fields.add(self.key.split('.', 1)[0])
                self.fields = list(self.fields)
            except:
                self.fields = []
                    
    
    def generate_task(self, item, count, epoch_start, epoch_end):
        only_before = epoch_end + self.interval
        for field in self.fields:
            query = {'fields': [field],
                     'spec':[{'key':self.key, 'value': item}], 
                     'instance':self.instance}
            dasquery = DASQuery(query)
            expiry = self.get_query_expiry(dasquery)
            schedule = expiry - self.preempt
            if  schedule < time.time() + 60:
                schedule = time.time() + 60
            interval = schedule - time.time()
            itemname = item.replace('"','')
            if schedule < only_before:
                yield {'classname': 'QueryMaintainer',
                        'name': '%s-%s-%s' % (self.identifier, itemname, field),
                        'only_before': only_before,
                        'interval': interval,
                        'kwargs':{'dasquery':dasquery.storage_query,
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
                    try:
                        superset_keys = \
                            sorted([k for k in \
                            self.das.rawcache.get_superset_keys(self.key, key)],\
                            key=len)
                    except:
                        superset_keys = []
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
    
    def get_query_expiry(self, dasquery):
        "Extract analytics apicall the expire timestamp for given query"
        err_return = time.time() + (2*self.preempt)
        try:
            if not self.das.rawcache.incache(dasquery):
                try:
                    self.das.call(dasquery, add_to_analytics=False)
                except Exception as err:
                    print "\n### FAIL input query=%s, err=%s" \
                            % (dasquery, str(err))
                    raise err
            expiries = [result.get('apicall', {}).get('expire', 0) for result in \
                            self.das.analytics.list_apicalls(qhash=dasquery.qhash)]
            if  not expiries:
                return err_return
            return min(expiries)
        except:
            return err_return
              
    def make_one_summary(self, start, finish):
        "Actually make the summary"
        keys = collections.defaultdict(int)
        try:
            queries = self.das.analytics.list_queries(key=self.key,
                                                      after=start,
                                                      before=finish)
        except:
            queries = []
        for query in queries:
            count = len(filter(lambda t: t>=start and t<=finish, 
                               query['times']))
            for spec in query['mongoquery']['spec']:
                if spec['key'] == self.key:
                    keys[spec['value']] += count
        if keys:
            self.logger.info("Found %s queries in %s->%s" \
                             % (len(keys), start, finish))
        else:
            self.logger.info("Found no queries in %s->%s" \
                             % (start, finish))
        return keys
