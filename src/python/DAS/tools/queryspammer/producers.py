#pylint: disable-msg=R0903
"Random data producers for queryspammer"

import multiprocessing
import random
import yaml
import os
import math
from random_data import WeightedDistribution
LOG = multiprocessing.get_logger()

try:
    from DAS.core.das_core import DASCore
    HAVE_DAS = True
except ImportError:
    HAVE_DAS = False

class Producer(object):
    "Baseclass for random data producers"
    def __init__(self, data):
        LOG.info('Initialised producer::%s'%self.__class__.__name__)
    def __call__(self):
        return None

class WeightedKeyProducer(Producer):
    """
    Produces simple queries of the form "key=value" where value
    is has a weighted distribution. The weighting for key x is:
    
    normal_scale * gauss(0, normal_sigma*len(items) + 1/len(items)
    
    ie, a normal distribution with a long flat tail. This seems
    to vaguely match the distribution observed in practice.
    
    The item array is randomly sorted and truncated to maxitems.
    """
    
    # this configuration gives 50% of results in the first 15% of items 
    maxitems = 100
    normal_sigma = 0.15
    normal_scale = 3
    
    def __init__(self, data, key):
        self.key = key
        
        #get random keys repeatedly to try and get the complete set
        items = [data.get_random(key) for i in xrange(10000)]
        #find the unique items
        items = list(set(items))
        
        items = items[0:self.maxitems]
        
        random.shuffle(items)
        
        count = len(items)
        #make a weighted distribution
        sigma = self.normal_sigma * count
        weight = lambda i: self.normal_scale * 2\
                           * math.exp(-0.5 * i**2 / sigma**2)\
                           / (math.sqrt(2*math.pi) * sigma)\
                           + 1. / count
        weights = [weight(i) for i in xrange(count)]
        weightdict = dict([(i, w) for i, w in zip(items, weights)])
        self.dist = WeightedDistribution(weightdict)
        
        LOG.info("Weighted producer (%s) initialised", key)
        Producer.__init__(self, data)
        
    def __call__(self):
        """
        Return an appropriate, weighted query.
        """
            
        return "%s=%s" % (self.key, self.dist())
    
class WeightedDatasetProducer(WeightedKeyProducer):
    "Weighted dataset=value producer"
    def __init__(self, data):
        WeightedKeyProducer.__init__(self, data, 'dataset')
        
class WeightedSiteProducer(WeightedKeyProducer):
    "Weighted site=value producer"
    def __init__(self, data):
        WeightedKeyProducer.__init__(self, data, 'site')
        
class WeightedCityProducer(WeightedKeyProducer):
    def __init__(self, data):
        WeightedKeyProducer.__init__(self, data, 'city')
                
class MappingProducer(Producer):
    "Tries to produce random queries using DAS mapping DB directly"
    def __init__(self, data, **kwargs):
        self.data = data
        assert HAVE_DAS
        self.dascore = DASCore()
        self.keys = []
        for system, keys in self.dascore.mapping.daskeys().items():
            self.keys.extend(keys)
        self.keys = list(set(self.keys))
        self.systems = self.dascore.mapping.list_systems()
        self.syskey = self.dascore.mapping.daskeys()
        
        self.value_chance = kwargs.get('value_chance', 0.6)
        self.multiple_chance = kwargs.get('multiple_chance', 0.4)
        self.unique_chance = kwargs.get('unique_chance', 0.2)
        self.grep_chance = kwargs.get('grep_chance', 0.2)
        self.aggregate_chance = kwargs.get('aggregate_chance', 0.2)
        
        
        LOG.info("MappingProducer has %s primary keys", len(self.keys))
        LOG.info("MappingProducer has %s primary keys with generators", 
                 len(set(self.keys) & set(self.data.get_keys())))
        Producer.__init__(self, data)
        
    def find_secondary_keys(self, map_keys):
        #print 'find_secondary',map_keys
        apis = set(self.dascore.mapping.list_apis())
        for mk in map_keys:
            apis = apis & set(self.dascore.mapping.find_apis({'$ne': None}, mk))
        #print 'apis',apis
        possible_das_keys = []
        for api in apis:
            api_info = self.dascore.mapping.api_info(api)
            api_map_keys = [k['map'] for k in api_info['daskeys']]
            #print 'api',api,'api_map_keys',api_map_keys
            if all([mk in api_map_keys for mk in map_keys]):
                possible_das_keys += \
                        [k['key'] for k in api_info['daskeys'] \
                                if not k['map'] in map_keys]
            #print 'possible_das_keys',possible_das_keys
        
        return possible_das_keys
                
    def __call__(self):
        """
        
        Actually produce a query.
        
        We start by picking a primary key, from all keys. If we have a 
        generator for this key type and a random chance, we give it a
        value.
        
        We then decide whether to produce one or more secondary keys. To
        find these, we find the mapping key for the primary DAS key,
        then lookup all APIs looking for one that supports all the
        mapping keys we've found so far (ie, there will be a service that
        can be queried).
        
        Secondary keys must always have a value (ie, be in the set of
        keys we have a generator for).
        
        Finally, we look at the presentation keys for the DAS keys requested
        and randomly decide whether to add any aggregation or filter functions.
        
        We assume any presentation key can be aggregated. This will not
        always be true, but we have no good way of determining this (unless
        we store types along with presentation keys when we examine output).
        
        """
        primary_key = random.choice(self.keys)
        #print 'primary key', primary_key
        query = primary_key
        primary_mapping_key = self.dascore.mapping.find_mapkey({'$ne':None},
                                                               primary_key)
        #print 'primary map', primary_mapping_key
        
        if self.data.has_key(primary_key)\
            and random.random() < self.value_chance:
            primary_value = self.data.get_random(primary_key)
            #print 'primary_value', primary_value
            query += ' = %s' % primary_value
        
        das_keys = [primary_key]
        map_keys = [primary_mapping_key]
        while random.random() < self.multiple_chance:
            #print 'adding secondary key'
            possible_das_keys = self.find_secondary_keys(map_keys)
            possible_das_keys = \
                list(set(possible_das_keys) & set(self.data.get_keys()))
            if possible_das_keys:
                secondary_key = random.choice(possible_das_keys)
                #print 'secondary key',secondary_key
                query += ' %s' % secondary_key
                secondary_value = self.data.get_random(secondary_key)
                query += ' = %s' % secondary_value
                secondary_mapping_key = \
                    self.dascore.mapping.find_mapkey({'$ne':None}, \
                        secondary_key)
                #print 'secondary map', secondary_mapping_key
                das_keys += [secondary_key]
                map_keys += [secondary_mapping_key]
            else:
                break
        
        presentation_keys = []
        for key in das_keys:
            for pkey in self.dascore.mapping.presentation(key):
                if isinstance(pkey, dict):
                    presentation_keys += [pkey['das']]
                else:
                    presentation_keys += [pkey] 
        
        if random.random() < self.unique_chance:
            query += ' | unique'
        if random.random() < self.grep_chance:
            if presentation_keys:
                query += ' | grep'
                grep_keys = [random.choice(presentation_keys)]
                while random.random() < self.multiple_chance:
                    grep_keys += [random.choice(presentation_keys)]
                query += ' %s' % ', '.join(grep_keys)
        if random.random() < self.aggregate_chance:
            if presentation_keys:
                query += ' | %s(%s)' \
                        % (random.choice(('sum', 'min', 'max' ,'avg')),
                                        random.choice(presentation_keys))
                while random.random() < self.multiple_chance:
                    query += ', %s(%s)' \
                        % (random.choice(('sum', 'min', 'max', 'avg')),
                                       random.choice(presentation_keys))
        
        return query

class YAMLProducer(Producer):
    "Baseclass for data producers using a DAS yaml map"
    # this probably overcomplicated things and got them wrong
    def __init__(self, data, yamlfile):
        self.data = data
        self.keys = {} #key->subkeys
        self.keymap = {} #key->map
        yamlfile = os.environ['DAS_MAPS'] + yamlfile
        for block in yaml.load_all(open(yamlfile).read()):
            keys = []
            for key in block.get('daskeys', []):
                key_name = key.get('key','')
                key_map = key.get('map','')
                self.keys[key_name] = []
                self.keymap[key_name] = key_map
                keys.append(key_name)
            for das2api in block.get('das2api', []):
                das_key = das2api.get('das_key','')
                for key in keys:
                    self.keys[key].append(das_key)
        all_subkeys = list(set(reduce(lambda x, y:x+y, self.keys.values())+\
                               self.keymap.values()))
        self.generator = dict([(sk, None) 
                               for sk in all_subkeys]) #subkey->function()
        self.valid_operators = dict([(sk, {'=':1}) 
                 for sk in all_subkeys]) #subkey->operator->probability
        self.grep_target = dict([(k, [sk 
                      for sk in all_subkeys 
                      if '.' in sk and sk.split('.')[0]==k]) 
                 for k in self.keys]) #key->[grep targets]
        self.aggregator_target = dict([(k, []) 
                           for k in self.keys]) #key->[valid_operators]
        self.p_key = dict([(k, 1) for k in self.keys]) #key->probability 
        self.p_subkey = dict([(k, dict([(sk, 1./len(self.keys[k])) 
                for sk in self.keys[k]])) 
                for k in self.keys]) #key->subkey->ind probability
        self.p_grep_op = 0.2
        self.p_aggr_op = 0.2
        self.p_set_primary = 0.4
        Producer.__init__(self)
    def __call__(self):
        #select a random primary key
        key = WeightedDistribution(self.p_key)()
        #either give it an argument (using keymap) 
        #or select one or more secondary keys (providing they have generators)
        if random.random() < self.p_set_primary \
                and self.generator[self.keymap[key]] != None:
            effective_subkey = self.keymap[key]
            operator = WeightedDistribution(\
                self.valid_operators[effective_subkey])()
            query = '%s %s %s' % (key,
                                  operator,
                                  self.generator[effective_subkey]())
        else:
            subkeys = [sk 
                       for sk in self.keys[key] 
                       if self.p_subkey[key][sk] > random.random() 
                       and self.generator[sk] != None 
                       and len(self.valid_operators[sk]) > 0 
                       and sk != self.keymap[key]]
            operators = [WeightedDistribution(self.valid_operators[sk])() 
                         for sk in subkeys]
        
            subkey_strings = ['%s %s %s' % (sk, op, self.generator[sk]()) 
                              for sk, op in zip(subkeys, operators)]
            query = key + ' ' + ' '.join(subkey_strings)
        #select whether to apply a filter operation (using grep_target(s))
        r_suffix = random.random()
        if r_suffix < self.p_grep_op:
            available_grep_targets = self.grep_target[key][:]
            random.shuffle(available_grep_targets)
            if len(available_grep_targets) > 0:
                query += ' | grep '
                g_targets = []
                r_grep = r_suffix
                while r_grep < self.p_grep_op and \
                len(available_grep_targets) > 0:
                    g_targets.append(available_grep_targets.pop())
                    r_grep = random.random()
                query += ','.join(g_targets)
        #select whether to apply an aggregate operation 
        # (using aggregator_target(s))
        elif r_suffix < self.p_grep_op + self.p_aggr_op:
            if len(self.aggregator_target[key])>0:
                query += ' | %s(%s)' % (
                              random.choice(('sum','min','max','count','avg')),
                              random.choice(self.aggregator_target[key]))
        return query  

class GoogleMapsProducer(YAMLProducer):
    def __init__(self, data):
        YAMLProducer.__init__(self, data, 'google_maps.yml')
        self.generator['city.name'] = self.data.city_name
        self.generator['zip.code'] = self.data.zip_code
        self.grep_target['city'] = ['city.name',
                                    'city.postalCodes.placeName',
                                    'city.postalCodes.countryCode',
                                    'city.postalCodes.adminName1']
        self.aggregator_target['city'] = ['city.postalCodes.lat',
                                          'city.postalCodes.lng']

class PhedexProducer(YAMLProducer):
    def __init__(self, data):
        YAMLProducer.__init__(self, data, 'phedex.yml')
        self.generator['node.name'] = self.data.node
        self.generator['block.name'] = self.data.block
        self.generator['site.name'] = self.data.node
        self.generator['site.se'] = self.data.se
        self.generator['site'] = \
        lambda: self.data.node() if random.random()>0.5 else self.data.se()
        self.generator['file.name'] = self.data.file
        self.generator['group.name'] = self.data.group
        self.generator['group'] = self.data.group
       
class DBS3Producer(YAMLProducer):
    def __init__(self, data):
        YAMLProducer.__init__(self, data, 'dbs3.yml')
        self.generator['primary_ds.name'] = self.data.dataset
        self.generator['release'] = self.data.release
        self.generator['run.run_number'] = self.data.run_number
        self.generator['dataset.name'] = self.data.dataset
        self.generator['parent.name'] = self.data.dataset
        self.generator['child.name'] = self.data.dataset
        #self.generator['config'] = self.data.config #not yet implemented
        self.generator['block.name'] = self.data.block
        self.generator['file.name'] = self.data.file
        
class CMSSWConfigProducer(YAMLProducer):
    def __init__(self, data):
        YAMLProducer.__init__(self, data, 'cmsswconfigs.yml')
        #'config' #not sure what this parameter should be...
        #'search' #tricker
        self.generator['release.name'] = self.data.release

class DashboardProducer(YAMLProducer):
    def __init__(self, data):
        YAMLProducer.__init__(self, data, 'dashboard.yml')
        self.generator['date'] = self.data.date
        self.generator['site'] = self.data.node
        self.generator['site.ce'] = self.data.ce
        self.generator['site.name'] = self.data.node
        
class IpProducer(YAMLProducer):
    def __init__(self, data):
        YAMLProducer.__init__(self, data, 'ip.yml')
        self.generator['ip.address'] = self.data.ip

def list_producers():
    "List all producer classes"
    return [k 
            for k, v in globals().items() 
            if type(v)==type(type) 
            and issubclass(v,Producer) 
            and not v==Producer 
            and not v==YAMLProducer]
        
    
