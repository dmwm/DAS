"Random data producers for queryspammer"

import multiprocessing
import random
import yaml
import os
from random_data import WeightedDistribution, RANDOMDATA
LOG = multiprocessing.get_logger()

class Producer(object):
    "Baseclass for random data producers"
    def __init__(self):
        LOG.info('Initialised producer::%s'%self.__class__.__name__)
    def __call__(self):
        return None

class YAMLProducer(Producer):
    "Baseclass for data producers using a DAS yaml map"
    def __init__(self, yamlfile):
        self.keys = {} #key->subkeys
        self.keymap = {} #key->map
        yamlfile = os.environ['DAS_ROOT'] +\
                    ('/' if not os.environ['DAS_ROOT'][-1]=='/' else '') +\
                     'src/python/DAS/services/maps/' +\
                      yamlfile
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
            operator = WeightedDistribution(self.valid_operators[effective_subkey])()
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
                while r_grep < self.p_grep_op and len(available_grep_targets) > 0:
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
    def __init__(self):
        YAMLProducer.__init__(self,'google_maps.yml')
        self.generator['city.name'] = RANDOMDATA.city_name
        self.generator['zip.code'] = RANDOMDATA.zip_code
        self.grep_target['city'] = ['city.name',
                                    'city.postalCodes.placeName',
                                    'city.postalCodes.countryCode',
                                    'city.postalCodes.adminName1']
        self.aggregator_target['city'] = ['city.postalCodes.lat',
                                          'city.postalCodes.lng']

class PhedexProducer(YAMLProducer):
    def __init__(self):
        YAMLProducer.__init__(self,'phedex.yml')
        self.generator['node.name'] = RANDOMDATA.node
        self.generator['block.name'] = RANDOMDATA.block
        self.generator['site.name'] = RANDOMDATA.node
        self.generator['site.se'] = RANDOMDATA.se
        self.generator['site'] = lambda: RANDOMDATA.node() if random.random()>0.5 else RANDOMDATA.se()
        self.generator['file.name'] = RANDOMDATA.file
        self.generator['group.name'] = RANDOMDATA.group
        self.generator['group'] = RANDOMDATA.group
       
class DBS3Producer(YAMLProducer):
    def __init__(self):
        YAMLProducer.__init__(self,'dbs3.yml')
        self.generator['primary_ds.name'] = RANDOMDATA.dataset
        self.generator['release'] = RANDOMDATA.release
        self.generator['run.run_number'] = RANDOMDATA.run_number
        self.generator['dataset.name'] = RANDOMDATA.dataset
        self.generator['parent.name'] = RANDOMDATA.dataset
        self.generator['child.name'] = RANDOMDATA.dataset
        #self.generator['config'] = RANDOMDATA.config #not yet implemented
        self.generator['block.name'] = RANDOMDATA.block
        self.generator['file.name'] = RANDOMDATA.file
        
class CMSSWConfigProducer(YAMLProducer):
    def __init__(self):
        YAMLProducer.__init__(self,'cmsswconfigs.yml')
        #'config' #not sure what this parameter should be...
        #'search' #tricker
        self.generator['release.name'] = RANDOMDATA.release

class DashboardProducer(YAMLProducer):
    def __init__(self):
        YAMLProducer.__init__(self,'dashboard.yml')
        self.generator['date'] = RANDOMDATA.date
        self.generator['site'] = RANDOMDATA.node
        self.generator['site.ce'] = RANDOMDATA.ce
        self.generator['site.name'] = RANDOMDATA.node
        
class IpProducer(YAMLProducer):
    def __init__(self):
        YAMLProducer.__init__(self,'ip.yml')
        self.generator['ip.address'] = RANDOMDATA.ip

def list_producers():
    "List all producer classes"
    return [k 
            for k, v in globals().items() 
            if type(v)==type(type) 
            and issubclass(v,Producer) 
            and not v==Producer 
            and not v==YAMLProducer]
        
    
