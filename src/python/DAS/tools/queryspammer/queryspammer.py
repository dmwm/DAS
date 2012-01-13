#!/usr/bin/env python
#pylint: disable-msg=C0103

"User access code for queryspammer"

import optparse
import multiprocessing.managers
import logging
import copy

LOG = multiprocessing.get_logger()
LOG.setLevel(logging.INFO)      
LOG.addHandler(logging.StreamHandler())

from DAS.tools.queryspammer import random_data
from DAS.tools.queryspammer import producers
from DAS.tools.queryspammer import filters
from DAS.tools.queryspammer import submitters

class QueryMaker(object):
    "Callable that selects a random producer and filters the result"
    def __init__(self, producers_, filters_):
        self.producer_distribution = \
                random_data.WeightedDistribution(producers_)
        self.filter_chain = filters_
    def __call__(self):
        return reduce(lambda x, y: y(x), 
                      self.filter_chain, 
                      self.producer_distribution())

multiprocessing.managers.BaseManager.register('QueryMaker', QueryMaker)

class QuerySpammer(object):
    "Actual object the user runs"
    def __init__(self, producers_, filters_, submitter_, **kwargs):
        self.producers = producers_
        self.filters = filters_
        self.submitter = submitter_
        self.workers = kwargs.get('workers', 1)
        self.max_calls = kwargs.get('max_calls', -1)
        self.max_time = kwargs.get('max_time', -1)
        self.delay = kwargs.get('delay', 0)
        self.mode = kwargs.get('mode', 'continuous')
        self.submit_args = kwargs.get('submit_args', {})

    def run(self):
        "Actually run the submitters"
        #manager = multiprocessing.managers.BaseManager()
        #manager.start()
        #shared_maker = manager.QueryMaker(producers,filters)
        shared_maker = QueryMaker(self.producers, self.filters)        

        kwargs = {
            'max_calls':self.max_calls,
            'max_time':self.max_time,
            'delay':self.delay,
            'mode':self.mode
        }
        kwargs.update(self.submit_args)
            
        submit_processes = [self.submitter(shared_maker,
                                **copy.deepcopy(kwargs))
                             for _ in range(self.workers)]
        LOG.info("Starting processes")
        map(lambda x: x.start(), submit_processes)
        LOG.info("Joining processes")
        map(lambda x: x.join(), submit_processes)
        
        
def get_class(module, classname):
    "Try and load a class from the specified module"
    imported_module = __import__(module, fromlist = [classname])
    assert hasattr(imported_module, classname)
    return getattr(imported_module, classname)
                
if __name__ == '__main__':
    parser = optparse.OptionParser()
    group = optparse.OptionGroup(parser, "Thread Options")
    group.add_option("-w", "--workers", type="int", dest="workers", 
        default=1, help="Number of concurrent submission processes")
    parser.add_option_group(group)
    group = optparse.OptionGroup(parser, "Run Options")
    group.add_option("-t", "--maxtime", type="int", dest="max_time", 
        default=-1, help="Maximum time (in seconds) for submitters to run")
    group.add_option("-c", "--maxcalls", type="int", dest="max_calls", 
        default=-1, help="Maximum calls for submitters to make")
    group.add_option("-d", "--delay", type="float", dest="delay", 
        default=0, help="Time to wait between submissions")
    group.add_option("-m", "--mode", type="choice", dest="mode", 
        default="continuous", choices=['continuous', 'delay', 'random'], 
        help="Delay mode")
    parser.add_option_group(group)
    group = optparse.OptionGroup(parser, "Filter Options")
    group.add_option("-f", "--filter", type="choice", 
        action="append", dest="filters", default=[], 
        choices=filters.list_filters()+['all'], 
        help="Filter class to use[:probability]")
    group.add_option("--filter-prob", type="float", 
        dest="filterprob", default=0.1, 
        help="Default probability for filters")
    parser.add_option_group(group)
    group = optparse.OptionGroup(parser, "Producer Options")
    group.add_option("-p", "--producer", type="choice", 
        action="append", dest="producers", default=[], 
        choices=producers.list_producers()+['all'], 
        help="Producer class to use [:weight]")
    parser.add_option_group(group)
    group = optparse.OptionGroup(parser, "Submitter Options")
    group.add_option("-s", "--submitter", type="choice", 
        dest="submitter", default="StdOutSubmitter", 
        choices=submitters.list_submitters(), help="Submitter class to use")
    group.add_option("--submitter-args", type="string", 
        dest="submit_args", default="{}", 
        help="Additional options for submitter (dictionary)")
    parser.add_option_group(group)

    (options, args) = parser.parse_args()
    
    if 'all' in options.producers:
        options.producers = producers.list_producers()
    if 'all' in options.filters:
        options.filters = filters.list_filters()


    producer_list = [(p.split(':')[0], float(p.split(':')[1])\
                       if len(p.split(':'))==2 else 1)
                      for p in options.producers]
    filter_list = [(f.split(':')[0], float(f.split(':')[1])\
                     if len(f.split(':'))==2 else options.filterprob)
                    for f in options.filters]
    try:
        options.submit_args = eval(options.submit_args)
        assert isinstance(options.submit_args, dict)
    except:
        LOG.warning("Failed to convert submitter-args to dictionary")
        options.submit_args = {}

    if producer_list:
        data = random_data.get_random_data()

    LOG.info("Loading producer classes")
    producer_classes = [(get_class('producers', name)(data), prob) 
                        for (name, prob) in producer_list]
    LOG.info("Loading filter classes")
    filter_classes = [get_class('filters', name)(prob) 
                      for (name, prob) in filter_list]
    LOG.info("Loading submitter class")
    submitter_class = get_class('submitters', options.submitter)

    if producer_classes:

        LOG.info("Creating QuerySpammer")
        spammer = QuerySpammer(producer_classes, 
                           filter_classes, 
                           submitter_class, 
                           **{'workers':options.workers,
                              'max_calls':options.max_calls,
                              'max_time':options.max_time,
                              'delay':options.delay,
                              'mode':options.mode,
                              'submit_args':options.submit_args})
        spammer.run()
    else:
        LOG.info("Must specify >0 producers to run")
    
    
