#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0301

"""
Task class for analytics.
"""

# system modules
import time
import copy

# DAS modules
from DAS.utils.utils import genkey, print_exc
from DAS.analytics.utils import multilogging, DAS_CONFIG, TASK_CLASSES
#from DAS.core.das_core import DASCore
from DAS.utils.das_singleton import das_singleton
from DAS.utils.logger import PrintManager

class Task(object):
    "Representation of a repeatedly-run task with access to DAS"
    def __init__(self, name, classname, interval, 
                 kwargs, max_runs=None, only_before=None,
                 parent=None):
        #name of the python class that will be run
        self.classname = classname
        #kwargs for the class instance
        self.kwargs = kwargs
        #desired frequency
        self.interval = interval
        #name by which this task shall be known
        self.name = name
        #tuple of master_ids, if any, that created this task
        self.parent = parent if parent else tuple()
        #master id is a static key for a given task
        #whereas task_id is a UUID for each time the task runs
        self.master_id = genkey({'hash':(name, classname, interval, kwargs,
                                 max_runs, only_before, parent)})
        
        self.run_count = 0
        self.max_runs = max_runs
        self.only_before = only_before
        
        self.last_run_at = None
        
        self.retries = 0
    
    def get_dict(self):
        "Get a dict representing this task for the web interface"
        return {'name':         self.name,
                'classname':    self.classname,
                'interval':     self.interval,
                'kwargs':       self.kwargs,
                'run_count':    self.run_count,
                'max_runs':     self.max_runs,
                'only_before':  self.only_before,
                'can_respawn':  self.can_respawn(time.time()+self.interval),
                'retries':      self.retries,
                'master_id':    self.master_id,
                'last_run_at':  self.last_run_at,
                'parent':       self.parent}
    
    
    def can_respawn(self, when):
        "Return whether this task can be run again"
        if self.max_runs and self.run_count >= self.max_runs:
            return False
        if self.only_before and when > self.only_before:
            return False
        return True
        
    
    
    def spawn(self):
        "Spawn a runnable version of this task to send to the pool"
        
        self.run_count += 1
        return RunnableTask(self.name,
                            self.classname,
                            self.master_id, 
                            self.kwargs, 
                            self.run_count,
                            self.interval,
                            self.parent)

class RunnableTask(object):
    """
    This object is spawned by a task object, to be pickled and 
    passed to a worker process to actually run.
    
    This object is created when the task is added to the worker
    pool (apply_async), which will normally result in immediate
    execution.
    
    The actual implementation class is not imported until the
    worker process starts running.
    """
    def __init__(self, name, classname, master_id, kwargs, 
                 index, interval, parent):
        self.classname = classname
        self.kwargs = copy.deepcopy(kwargs)
        self.name = name
        self.index = index
        self.interval = interval
        self.parent = copy.deepcopy(parent)
        self.master_id = master_id
        self.logger = None
#        self.logger = PrintManager('RunnableTask', kwargs.get('verbose', 0))

    def __call__(self):
        "Callable that the worker process will run."
        
        self.logger = multilogging().getLogger("DASAnalytics.RunnableTask")
        msg = 'Starting task=%s:%s, class=%s' \
                % (self.name, self.index, self.classname)
        self.logger.info(msg)
        start_time = time.time()
        
        if not self.classname in TASK_CLASSES:
            msg = 'Task "%s:%s" unknown class "%s", aborting.' \
                % (self.name, self.index, self.classname)
            self.logger.error(msg)
            return {'success': False, 'error': "unknown class", 
                    'start_time':start_time, 'finish_time':start_time,
                    'name':self.name, 'index':self.index, 'parent':self.parent,
                    'master_id':self.master_id, 'classname': self.classname}
            
        klass = TASK_CLASSES[self.classname]
        
        childlogger = multilogging().getLogger("DASAnalytics.Task",
                                               task_name=self.name, 
                                               task_class=self.classname,
                                               task_index=self.index,
                                               task_master=self.master_id, 
                                               task_parent=self.parent)
        
#        das = DASCore(config=copy.deepcopy(DAS_CONFIG), multitask=None, debug=2)
#        das = DASCore(multitask=None, debug=1)
        try:
            das = das_singleton(multitask=None, debug=1)
        except Exception as exp:
            msg = 'ERROR: task=%s:%s failed to instantiate, aborting' \
                         % (self.name, self.index)
            print msg
            print_exc(exp)
            return {'success': False, 'error': exp, 
                    'start_time':start_time, 'finish_time':start_time,
                    'name':self.name, 'index':self.index, 'parent':self.parent,
                    'master_id':self.master_id, 'classname': self.classname}
        
        #the DAS instance will now be global if this option is set,
        #and otherwise uniquely created by das_factory
        self.kwargs.update({'logger':childlogger, 
                            'name':self.name, #task title
                            'index':self.index, # #runs of this task
                            'interval':self.interval, #desired frequency
                            'DAS':das})
        
        try:
            instance = klass(**self.kwargs)
        except Exception as exp:
            msg = 'ERROR: task=%s:%s failed to instantiate, aborting' \
                         % (self.name, self.index)
            print msg
            print_exc(exp)
            return {'success': False, 'error': exp, 
                    'start_time':start_time, 'finish_time':start_time,
                    'name':self.name, 'index':self.index, 'parent':self.parent,
                    'master_id':self.master_id, 'classname': self.classname}
        try:
            result = instance()
        except Exception as exp:
            finish_time = time.time() #we might have run for some time by now
            msg = 'ERROR: task=%s:%s failed during run, aborting' \
                         % (self.name, self.index)
            self.logger.error(msg)
            print_exc(exp)
            return {'success': False, 'error': exp,
                    'start_time':start_time, 'finish_time':finish_time,
                    'name':self.name, 'index':self.index, 'parent':self.parent,
                    'master_id':self.master_id, 'classname': self.classname}
        
        finish_time = time.time()
        if isinstance(result, dict):
            result.update({'success':True, 
                           'start_time':start_time, 'finish_time':finish_time,
                           'name':self.name, 'index':self.index, 'parent':self.parent,
                           'master_id':self.master_id, 'classname': self.classname})
            return result
        else:
            return {'success':True, 'result':result, 
                    'start_time':start_time, 'finish_time':finish_time,
                    'name':self.name, 'index':self.index, 'parent':self.parent,
                    'master_id':self.master_id, 'classname': self.classname}
