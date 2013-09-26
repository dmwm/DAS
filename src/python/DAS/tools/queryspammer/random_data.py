#pylint: disable-msg=C0301,C0103,C0111
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
    Expects a dictionary of "result"->"probability" pairs.
    Probabilities will be normalised to one.
    If result is callable, then return result()
    """
    @staticmethod
    def power_dist(items, power=1.5):
        """
        Generate a weighted distribution, by sorting the items
        randomly then assigning weights from a linear range
        raised to the given power (default 1.5).
        ie, weights 1**1.5, 2**1.5, ... n**1.5
        """
        random.shuffle(items)
        values = dict([(i, (j+1)**power) \
                for i,  j in zip(items, xrange(len(items)))])
        return WeightedDistribution(values)

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

        self._city_name = ['Shanghai', 'Mumbai', 'Karachi',
                'Delhi', 'Istanbul', 'Sao Paulo', 'Moscow',
                'Seoul', 'Beijing', 'Mexico City', 'Tokyo',
                'Kinshasa', 'Jakarta', 'New York', 'Lagos',
                'Lima', 'London', 'Bogota', 'Tehran', 'Ho Chi Minh City']

        LOG.info('Fetching nodes')
        url = 'http://cmsweb.cern.ch/phedex/datasvc/json/prod/nodes'
        self._node = sorted([n['name'] for n in \
        json.load(urllib.urlopen(url))['phedex']['node']])
        LOG.info('Fetching groups')
        url = 'http://cmsweb.cern.ch/phedex/datasvc/json/prod/groups'
        self._group = sorted([n['name'] for n in \
        json.load(urllib.urlopen(url))['phedex']['group']])
        LOG.info('Fetching blocks')
        blocks = []
        base_url = 'http://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas?'
        for node in [random.choice(self._node) for _ in xrange(16)]:
            if node[1] in ('2','3'):
                LOG.info('Fetching blocks node=%s' % node)
                url = base_url + 'node=%s' % node
                blocks += [b['name'] for b in \
                           json.load(urllib.urlopen(url))['phedex']['block']]
        self._block = list(set(blocks))
        self._dataset = list(set([b.split('#')[0] for b in self._block]))
        self._primary_ds = list(set([d[1:].split('/')[0]
                                     for d in self._dataset]))
        LOG.info('Fetching files')
        files = []
        base_url = 'http://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?'
        for dataset in [random.choice(self._dataset) for i in xrange(16)]:
            LOG.info('Fetching files dataset=%s' % dataset)
            url = base_url + 'dataset=%s' % dataset
            for block in json.load(urllib.urlopen(url))['phedex']['block']:
                files += [f['name'] for f in block['file']]
        self._file = list(set(files))
        LOG.info('Fetching SEs')
        ses = []
        base_url = 'http://cmsweb.cern.ch/sitedb/json/index/CMSNametoSE?'
        for node in [random.choice(self._node) for i in xrange(16)]:
            LOG.info('Fetching SE(s) for node=%s' % node)
            url = base_url + 'name=%s' % node
            ses += [se['name'] for se in json.loads(\
                urllib.urlopen(url).read().replace("'", '"')).values()]
        self._se = list(set(ses))
        LOG.info('Fetching CEs')
        ces = []
        base_url = 'http://cmsweb.cern.ch/sitedb/json/index/CMSNametoCE?'
        for node in [random.choice(self._node) for i in xrange(16)]:
            LOG.info('Fetching CE(s) for node=%s' % node)
            url = base_url + 'name=%s' % node
            ces += [se['name'] for se in json.loads(\
                urllib.urlopen(url).read().replace("'", '"')).values()]
        self._ce = list(set(ces))

        self._release = ['CMSSW_3_4_0', 'CMSSW_3_4_1', 'CMSSW_3_4_2',
                'CMSSW_3_4_2_patch1', 'CMSSW_3_5_0', 'CMSSW_3_5_0_patch1',
                'CMSSW_3_5_1', 'CMSSW_3_5_1_patch1', 'CMSSW_3_5_2',
                'CMSSW_3_5_2_patch1', 'CMSSW_3_5_2_patch2', 'CMSSW_3_5_3',
                'CMSSW_3_5_4', 'CMSSW_3_5_4_patch1', 'CMSSW_3_5_4_patch2',
                'CMSSW_3_5_6', 'CMSSW_3_5_6_patch1', 'CMSSW_3_5_7',
                'CMSSW_3_5_8', 'CMSSW_3_5_8_patch1', 'CMSSW_3_5_8_patch2',
                'CMSSW_3_6_0', 'CMSSW_3_5_8_patch3', 'CMSSW_3_6_0_patch2',
                'CMSSW_3_6_1', 'CMSSW_3_5_8_patch4', 'CMSSW_3_6_1_patch1',
                'CMSSW_3_6_1_patch2', 'CMSSW_3_5_7_hltpatch4',
                'CMSSW_3_6_1_patch3', 'CMSSW_3_7_0', 'CMSSW_3_6_1_patch4',
                'CMSSW_3_7_0_patch1', 'CMSSW_3_6_2', 'CMSSW_3_7_0_patch2',
                'CMSSW_3_6_3', 'CMSSW_3_6_1_patch5', 'CMSSW_3_7_0_patch3',
                'CMSSW_3_7_0_patch4', 'CMSSW_3_6_3_patch1']

        self.keygen = {
                        'block': ('block',),
                        'run': ('run_number',),
                        'site': ('node'),
                        'file': ('file',),
                        'dataset': ('dataset',),
                        'release': ('release',),
                        'city': ('city_name',),
                        'zip': ('zip_code',),
                        'ip': ('ip',),
                        'parent': ('dataset',),
                        'group': ('group',),
                        'primary_ds': ('primary_ds',),
                       } #direct references are unpickleable, apparently

    def get_random(self, key):
        if  key in self.keygen:
            generator = random.choice(self.keygen[key])
            data = getattr(self, generator)()
            if ' ' in data:
                data = '"%s"' % data
            return data
        return None

    def get_keys(self):
        return self.keygen.keys()

    def node(self):
        """Return a node"""
        return random.choice(self._node)
    def group(self):
        """Return a group"""
        return random.choice(self._group)
    def ce(self):
        """Return a CE"""
        return random.choice(self._ce)
    def se(self):
        """Return a SE"""
        return random.choice(self._se)
    def file(self):
        """Return a file"""
        return random.choice(self._file)
    def block(self):
        """Return a block"""
        return random.choice(self._block)
    def dataset(self):
        """Return a dataset"""
        return random.choice(self._dataset)
    def release(self):
        """Return a random release"""
        return random.choice(self._release)
    def city_name(self):
        """Return a city name"""
        return random.choice(self._city_name)

    def zip_code(self):
        """Return an zip code"""
        return str(random.randint(10000, 99999))

    def run_number(self):
        """Return a run number"""
        return str(random.randint(100000, 150000))

    def date(self):
        """Return a date in YYYY-MM-DD format"""
        return time.strftime('%Y-%m-%d',
                     time.gmtime(time.time() - 31557600 * random.random()))

    def ip(self):
        """Return an IP address"""
        return '%s.%s.%s.%s' % (random.randint(0, 255),
                                random.randint(0, 255),
                                random.randint(0, 255),
                                random.randint(0, 255))

    def primary_ds(self):
        """Return a primary dataset name"""
        return random.choice(self._primary_ds)

def get_random_data():

    LOG.info("Looking for random data")
    if os.path.exists('random_data.pkl'):
        picklefile = open('random_data.pkl', 'rb')
        time_create, data = pickle.load(picklefile)
        picklefile.close()
        if time.time() - time_create < 86400*7:
            LOG.info("Loading from random_data.pkl")
            return data
        else:
            LOG.info("Generating fresh random data - file old")
            data = RandomData()
            picklefile = open('random_data.pkl', 'wb')
            pickle.dump((time.time(), data), picklefile, 2)
            picklefile.close()
            return data
    else:
        LOG.info("Generating fresh random data - file not found")
        data = RandomData()
        picklefile = open('random_data.pkl', 'wb')
        pickle.dump((time.time(), data), picklefile, 2)
        picklefile.close()
        return data


