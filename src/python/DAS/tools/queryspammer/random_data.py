"Random data generation and storage for queryspammer"

import multiprocessing
import json
import urllib
import time
import random
import pickle
import os
LOG = multiprocessing.get_logger()

class WeightedDistribution:
    """
    Expects a dictionary of "result"->"probability" pairs. Probalities will be normalised to one.
    If result is callable, then return result()
    """
    def __init__(self, values):
        if isinstance(values, dict):
            total = float(sum(values.values()))
            running = 0
            self.weights = []
            for k, v in values.items():
                running += v/total
                self.weights.append((running, k))
        elif isinstance(values, (tuple, list)):
            total = float(sum([v[1] for v in values]))
            running = 0
            self.weights = []
            for k, v in values:
                running += v/total
                self.weights.append((running, k))

        else:
            raise Exception("Supply a dict or a list of pairs")
        
    def random(self):
        value = random.random()
        for (v, k) in self.weights:
            if v > value:
                if callable(k):
                    return k()
                else:
                    return k      
    __call__ = random
    def prob(self, item):
        for (v, k) in self.weights:
            if k == item:
                return v
    def generate(self, howmany = -1):
        if howmany == -1:
            while True:
                yield self.random()
        else:
            for i in xrange(howmany):
                yield self.random()

class RandomData(object):
    def __init__(self):
        
        self._city_name = ['Shanghai','Mumbai','Karachi','Delhi','Istanbul','Sao Paulo','Moscow','Seoul','Beijing','Mexico City','Tokyo','Kinshasa','Jakarta','New York','Lagos','Lima','London','Bogota','Tehran','Ho Chi Minh City']
    
        LOG.info('Fetching nodes')
        self._node = sorted([n['name'] for n in json.load(urllib.urlopen('http://cmsweb.cern.ch/phedex/datasvc/json/prod/nodes'))['phedex']['node']])
        LOG.info('Fetching groups')
        self._group = sorted([n['name'] for n in json.load(urllib.urlopen('http://cmsweb.cern.ch/phedex/datasvc/json/prod/groups'))['phedex']['group']])
        LOG.info('Fetching blocks')
        blocks = []
        for node in [random.choice(self._node) for i in range(10)]:
            LOG.info('Fetching blocks node=%s'%node)
            blocks += [b['name'] for b in json.load(urllib.urlopen('http://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas?node=%s'%node))['phedex']['block']]
        self._block = list(set(blocks))
        self._dataset = list(set([b.split('#')[0] for b in self._block]))
        LOG.info('Fetching files')
        files = []
        for dataset in [random.choice(self._dataset) for i in range(10)]:
            LOG.info('Fetching files dataset=%s'%dataset)
            for block in json.load(urllib.urlopen('http://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=%s'%dataset))['phedex']['block']:
                files += [f['name'] for f in block['file']]
        self._file = list(set(files))
        LOG.info('Fetching SEs')
        ses = []
        for node in [random.choice(self._node) for i in range(10)]:
            LOG.info('Fetching SE(s) for node=%s'%node)
            ses += [se['name'] for se in json.loads(urllib.urlopen('http://cmsweb.cern.ch/sitedb/json/index/CMSNametoSE?name=%s'%node).read().replace("'", '"')).values()]
        self._se = list(set(ses))
        LOG.info('Fetching CEs')
        ces = []
        for node in [random.choice(self._node) for i in range(10)]:
            LOG.info('Fetching CE(s) for node=%s'%node)
            ces += [se['name'] for se in json.loads(urllib.urlopen('http://cmsweb.cern.ch/sitedb/json/index/CMSNametoCE?name=%s'%node).read().replace("'", '"')).values()]
        self._ce = list(set(ces))

        self._release = ['CMSSW_3_4_0', 'CMSSW_3_4_1', 'CMSSW_3_4_2', 'CMSSW_3_4_2_patch1', 'CMSSW_3_5_0', 'CMSSW_3_5_0_patch1', 'CMSSW_3_5_1', 'CMSSW_3_5_1_patch1', 'CMSSW_3_5_2', 'CMSSW_3_5_2_patch1', 'CMSSW_3_5_2_patch2', 'CMSSW_3_5_3', 'CMSSW_3_5_4', 'CMSSW_3_5_4_patch1', 'CMSSW_3_5_4_patch2', 'CMSSW_3_5_6', 'CMSSW_3_5_6_patch1', 'CMSSW_3_5_7', 'CMSSW_3_5_8', 'CMSSW_3_5_8_patch1', 'CMSSW_3_5_8_patch2', 'CMSSW_3_6_0', 'CMSSW_3_5_8_patch3', 'CMSSW_3_6_0_patch2', 'CMSSW_3_6_1', 'CMSSW_3_5_8_patch4', 'CMSSW_3_6_1_patch1', 'CMSSW_3_6_1_patch2', 'CMSSW_3_5_7_hltpatch4', 'CMSSW_3_6_1_patch3', 'CMSSW_3_7_0', 'CMSSW_3_6_1_patch4', 'CMSSW_3_7_0_patch1', 'CMSSW_3_6_2', 'CMSSW_3_7_0_patch2', 'CMSSW_3_6_3', 'CMSSW_3_6_1_patch5', 'CMSSW_3_7_0_patch3', 'CMSSW_3_7_0_patch4', 'CMSSW_3_6_3_patch1']

    def node(self): return random.choice(self._node)
    def group(self): return random.choice(self._group)
    def ce(self): return random.choice(self._ce)
    def se(self): return random.choice(self._se)
    def file(self): return random.choice(self._file)
    def block(self): return random.choice(self._block)
    def dataset(self): return random.choice(self._dataset)
    def release(self): return random.choice(self._release)
    def city_name(self): return random.choice(self._city_name)
    
    def zip_code(self): return str(random.randint(10000,99999))

    def run_number(self): return str(random.randint(100000,150000))

    def date(self): #YYYY-MM-DD
        return time.strftime('%Y-%m-%d',
                             time.gmtime(time.time() - 31557600 * random.random()))

    def ip(self):
        return '%s.%s.%s.%s' % (random.randint(0,255),
                                random.randint(0,255),
                                random.randint(0,255),
                                random.randint(0,255))
     
RANDOMDATA = None

LOG.info("Looking for random data")
if os.path.exists('random_data.pkl'):
    picklefile = open('random_data.pkl', 'rb')
    time_create, data = pickle.load(picklefile)
    picklefile.close()
    if time.time() - time_create < 86400*7:
        LOG.info("Loading from random_data.pkl")
        RANDOMDATA = data
    else:
        LOG.info("Generating fresh random data - file old")
        RANDOMDATA = RandomData() 
        picklefile = open('random_data.pkl', 'wb')
        pickle.dump((time.time(), RANDOMDATA), picklefile, 2)
        picklefile.close()
else:
    LOG.info("Generating fresh random data - file not found")
    RANDOMDATA = RandomData()
    picklefile = open('random_data.pkl', 'wb')
    pickle.dump((time.time(), RANDOMDATA), picklefile, 2)
    picklefile.close()
    
       
