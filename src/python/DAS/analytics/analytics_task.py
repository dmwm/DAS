#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0301

"""
Task class for analytics.
"""

# system modules
import time
import uuid
import copy

# DAS modules
from DAS.utils.utils import genkey, print_exc
from DAS.analytics.analytics_utils import multilogging, das_factory

class Task(object):
    "Representation of a repeatedly-run task with access to DAS"
    def __init__(self, name, classname, 
                 interval, kwargs, only_once=False, 
                 max_runs=None, only_before=None,
                 parent=None):
        self.classname = classname
        self.kwargs = kwargs
        self.interval = interval
        self.name = name
        self.parent = parent
        self.master_id = genkey({'hash':(name, classname, interval, kwargs,
                                 only_once, max_runs, parent)})
        
        self.run_count = 0
        self.only_once = only_once
        self.max_runs = max_runs
        self.only_before = only_before
        
        self.running_id = None
        
        self.last_run_at = None
        
        self.retries = 0
    
    def get_dict(self):
        "Get a dict representing this task for the web interface"
        return {'name':         self.name,
                'classname':    self.classname,
                'interval':     self.interval,
                'kwargs':       self.kwargs,
                'run_count':    self.run_count,
                'only_once':    self.only_once,
                'max_runs':     self.max_runs,
                'only_before':  self.only_before,
                'can_respawn':  self.can_respawn(time.time()+self.interval),
                'retries':      self.retries,
                'master_id':    self.master_id,
                'last_run_at':  self.last_run_at,
                'parent':       self.parent,
                'running_id':   self.running_id}
    
    
    def can_respawn(self, when):
        "Return whether this task can be run again"
        if self.only_once and self.run_count > 0:
            return False
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
                            self.run_count - 1,
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
        self.taskid = str(uuid.uuid4())
        self.interval = interval
        self.parent = parent
        self.master_id = master_id

    def get_task_id(self):
        "Get a string identifying this task."
        return self.taskid
    
    def __call__(self):
        "Callable that the worker process will run."
        taskid = self.get_task_id()
        self.logger = multilogging().getLogger("DASAnalytics.RunnableTask")
        msg = 'Starting task=%s, id=%s, class=%s' \
                % (self.name, taskid, self.classname)
        self.logger.info(msg)
        start_time = time.time()
        
        try:
            klass = self._load_class()
        except Exception as exp:
            msg = 'Task "%s" (%s) failed to load class "%s", aborting.' \
                % (self.name, taskid, self.classname)
            print "ERROR: %s" % msg
            print_exc(exp)
            return {'task_id':taskid, 'success': False, 'error': exp, 
                    'start_time':start_time, 'finish_time':start_time,
                    'name':self.name, 'index':self.index, 'parent':self.parent,
                    'master_id':self.master_id}
        
        log_child = "DASAnalytics.Task.%s.%s" % (self.classname, self.taskid)
        log_das = "DASAnalytics.Task.%s.%s.DAS" % (self.classname, self.taskid)                                           
        
        childlogger = multilogging().getLogger(log_child)
        
        #the DAS instance will now be global if this option is set,
        #and otherwise uniquely created by das_factory
        self.kwargs.update({'logger':childlogger, 
                            'taskid': taskid, #task uuid
                            'name':self.name, #task title
                            'index':self.index, # #runs of this task
                            'interval':self.interval, #desired frequency
                            'DAS':das_factory(log_das)})
        
        try:
            instance = klass(**self.kwargs)
        except Exception as exp:
            msg = 'ERROR: task=%s, id=%s failed to instantiate, aborting' \
                         % (self.name, taskid)
            print msg
            print_exc(exp)
            return {'task_id':taskid, 'success': False, 'error': exp, 
                    'start_time':start_time, 'finish_time':start_time,
                    'name':self.name, 'index':self.index, 'parent':self.parent,
                    'master_id':self.master_id}
        try:
            result = instance()
        except Exception as exp:
            finish_time = time.time() #we might have run for some time by now
            msg = 'ERROR: task=%s, id=%s failed during run, aborting' \
                         % (self.name, taskid)
            print msg
            print_exc(exp)
            return {'task_id':taskid, 'success': False, 'error': exp,
                    'start_time':start_time, 'finish_time':finish_time,
                    'name':self.name, 'index':self.index, 'parent':self.parent,
                    'master_id':self.master_id}
        
        finish_time = time.time()
#        self.logger.info('Finishing Task "%s" (%s) (success).', 
#                    self.name, taskid)
        if isinstance(result, dict):
            result.update({'task_id':taskid, 'success':True, 
                           'start_time':start_time, 'finish_time':finish_time,
                           'name':self.name, 'index':self.index, 'parent':self.parent,
                           'master_id':self.master_id})
            return result
        else:
            return {'task_id':taskid, 'success':True, 'result':result, 
                    'start_time':start_time, 'finish_time':finish_time,
                    'name':self.name, 'index':self.index, 'parent':self.parent,
                    'master_id':self.master_id}
                                                                            
    def _load_class(self):
        "Load the class this task will run."
        module = __import__('DAS.analytics.tasks.%s' % self.classname, 
                            fromlist=[self.classname])
        return getattr(module, self.classname)
