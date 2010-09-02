import time
import logging
import uuid
import copy

from DAS.core.das_core import DASCore
from DAS.utils.das_config import das_readconfig

class Task(object):
    "Representation of a repeatedly-run task with access to DAS"
    def __init__(self, name, classname, 
                 interval, kwargs, only_once=False, 
                 max_runs=None, only_before=None):
        self.classname = classname
        self.kwargs = kwargs
        self.interval = interval
        self.name = name
        
        self.run_count = 0
        self.only_once = only_once
        self.max_runs = max_runs
        self.only_before = only_before
        
        self.last_run_at = None
        
        self.retries = 0
    
    def get_dict(self):
        "Get a dict representing this task for the web interface"
        return {'name': self.name,
                'classname': self.classname,
                'interval': self.interval,
                'kwargs': self.kwargs,
                'run_count': self.run_count,
                'only_once': self.only_once,
                'max_runs': self.max_runs,
                'only_before': self.only_before,
                'can_respawn': self.can_respawn(),
                'retries': self.retries,
                'last_run_at': self.last_run_at}
                
            
    
    def __hash__(self):
        return hash((self.name,
                     self.classname,
                     self.interval))
    
    
    def can_respawn(self):
        "Return whether this task has any remaining runs left"
        if self.only_once and self.run_count > 0:
            return False
        if self.max_runs and self.run_count >= self.max_runs:
            return False
        if self.only_before and time.time() > self.only_before - self.interval:
            return False
        return True
        
    
    
    def spawn(self, das_config):
        "Spawn a runnable version of this task to send to the pool"
        
        self.run_count += 1
        return RunnableTask(self.name,
                            self.classname, 
                            self.kwargs, 
                            self.run_count - 1,
                            das_config,
                            self.interval)




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
    def __init__(self, name, classname, kwargs, 
                 index, das_config, interval):
        self.classname = classname
        self.kwargs = copy.deepcopy(kwargs)
        self.name = name
        self.index = index
        self.taskid = str(uuid.uuid4())
        self.das_config = das_config
        self.interval = interval

    def get_task_id(self):
        "Get a string identifying this task."
        return self.taskid
    
    def __call__(self):
        "Callable that the worker process will run."
        taskid = self.get_task_id()
        logger = logging.getLogger("DASAnalytics.RunnableTask")
        logger.info('Starting TaskID "%s".', taskid)
        start_time = time.time()
        
        try:
            klass = self._load_class()
        except Exception, e:
            logger.error('Task "%s" (%s) failed to load class "%s", aborting.',
                         self.name, taskid, self.classname)
            return {'taskid':taskid, 'success': False, 'error': e, 
                    'start_time':start_time, 'finish_time':start_time,
                    'name':self.name, 'index':self.index}
        childlogger = logging.getLogger("DASAnalytics.%s"%self.classname)
        daslogger = logging.getLogger("DASAnalytics.%s.DAS"%self.classname)
        
        #we probably need to add a DASCore instance here too.
        
        config = das_readconfig(self.das_config)
        
        self.kwargs.update({'logger':childlogger, 
                            'taskid': taskid, #task uuid
                            'name':self.name, #task title
                            'index':self.index, # #runs of this task
                            'interval':self.interval, #desired frequency
                            'DAS':DASCore(config, logger=daslogger)})
        
        try:
            instance = klass(**self.kwargs)
        except Exception, e:
            logger.error('Task "%s" (%s) failed to instantiate, aborting. Error was %s',
                         self.name, taskid, str(e))
            return {'taskid':taskid, 'success': False, 'error': e, 
                    'start_time':start_time, 'finish_time':start_time,
                    'name':self.name, 'index':self.index}
        try:
            result = instance()
        except Exception, e:
            finish_time = time.time() #we might have run for some time by now
            logger.error('Task "%s" (%s) failed during run, aborting. Error was %s',
                         self.name, taskid, str(e))
            return {'taskid':taskid, 'success': False, 'error': e,
                    'start_time':start_time, 'finish_time':finish_time,
                    'name':self.name, 'index':self.index}
        
        finish_time = time.time()
        logger.info('Finishing Task "%s" (%s) (success).', 
                    self.name, taskid)
        if isinstance(result, dict):
            result.update({'taskid':taskid, 'success':True, 
                           'start_time':start_time, 'finish_time':finish_time,
                           'name':self.name, 'index':self.index})
            return result
        else:
            return {'taskid':taskid, 'success':True, 'result':result, 
                    'start_time':start_time, 'finish_time':finish_time,
                    'name':self.name, 'index':self.index}
                                                                            
    def _load_class(self):
        "Load the class this task will run."
        module = __import__('DAS.analytics.tasks.%s' % self.classname, 
                            fromlist=[self.classname])
        return getattr(module, self.classname)