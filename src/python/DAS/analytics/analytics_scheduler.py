from analytics_config import DASAnalyticsConfig
from analytics_task import Task
import logging
import time
import multiprocessing

DASAnalyticsConfig.add_option("max_retries",
                              type=int,
                              default=1,
                              help="Number of times the task scheduler will retry a failed task before abandoning it.")
DASAnalyticsConfig.add_option("retry_delay",
                              type=int,
                              default=60,
                              help="Seconds to wait before retrying a failed job.")
DASAnalyticsConfig.add_option("minimum_interval",
                              type=int,
                              default=60,
                              help="Minimum repeat interval allowed for a task.")
DASAnalyticsConfig.add_option("workers",
                              type=int,
                              default=4,
                              help="Number of workers to use for tasks.")
DASAnalyticsConfig.add_option("das_config",
                              type=basestring,
                              default=None,
                              help="Location of DAS config file.")

class TaskScheduler(object):
    """
    This is a cron-like thread which keeps track of all the tasks to
    be run, and dispatches them to the worker pool at the appropriate
    time.
    
    Originally this was an independent thread, the loop is now run
    as a call from the main loop to avoid unnecessary thread
    duplication.
    
    A completed job calls back to this thread to request that it be
    re-scheduled at some point in the future.
    
    A job that fails will be retried a specified number of times before
    being prevented from being resubmitted.
    """
    def __init__(self, config):
        self.logger = logging.getLogger("DASAnalytics.TaskScheduler")
                
        self.scheduled = [] # -> (time, task)
        self.wakeup_at = None
        
        self.max_retries = config.max_retries
        self.retry_delay = config.retry_delay
        self.minimum_interval = config.minimum_interval
        
        self.pool = multiprocessing.Pool(processes=config.workers)
        
        self.callbacks = []
        
        self.das_config = config.das_config
        
        self.running = {}
    
    def add_callback(self, callback):
        "Add a callback to be dispatched when a job result returns."
        self.callbacks.append(callback)
    
    def get_tasks(self):
        "Get a de-classed list of current tasks for web interface."
        result = []
        for (when, task) in self.scheduled:
            taskdict = task.get_dict()
            taskdict['scheduled'] = when
            result.append(taskdict)
        return result 
    
    def add_task(self, task, when=None, offset=None):
        "Add a new task, specifying either an absolute time or an offset from now."
        assert bool(when) ^ bool(offset)
        if offset:
            when = time.time() + offset
            
        self.logger.info('Adding Task "%s" to TaskScheduler, scheduled in %d',
                         task.name, int(when-time.time()))
        
        self.scheduled.append((when, task))
        self._set_next_task()
        
        
    
    def _set_next_task(self):
        if len(self.scheduled) > 0:
            self.scheduled = sorted(self.scheduled, key=lambda x: x[0])
            self.wakeup_at = self.scheduled[0][0]
        
    def run(self):
            
        now = time.time()
        if self.wakeup_at and now > self.wakeup_at:
            self._submit_task()
            self._set_next_task()
        
    
    def _submit_task(self):
        "Do job submission."
        now = time.time()
        if len(self.scheduled) > 0:
            (when, task) = self.scheduled.pop(0)
            self.logger.info('Submitting Task "%s".', task.name)
            runnable = task.spawn(self.das_config)
            taskid = runnable.get_task_id()
            self.logger.info('Task "%s" has TaskID %s',
                             runnable.name, taskid)
            self.running[taskid] = task
            self.pool.apply_async(runnable, 
                                  callback=self.task_finished_callback)
            task.last_run_at = now
            
    def task_finished_callback(self, result):
        "Callback each finished job hits."
        now = time.time()
        taskid = result.get('taskid', None)
        success = result.get('success', False)
        resubmit = result.get('resubmit', True)
        new_tasks = result.get('new_tasks', [])
        task = self.running[taskid]
        self.logger.info('Finished Task "%s" (%s) success=%s.', 
                         task.name, taskid, success)
        if resubmit:
            if success:
                task.retries = 0
                if task.can_respawn():
                    when = task.last_run_at + task.interval
                    if when < now:
                        when = now + self.minimum_interval 
                    self.logger.info('..scheduling next run.')
                    self.add_task(task, when=task.last_run_at+task.interval)
                else:
                    self.logger.info('...exceeded maximum runs or time cutoff, not scheduling.')
            else:
                if task.retries < self.max_retries:
                    self.logger.info('...retrying in %s.', self.retry_delay)
                    task.retries += 1
                    self.add_task(task, offset=self.retry_delay)
                else:
                    self.logger.info('...failed and no more retries remaining.')
        else:
            self.logger.info('...rescheduling disabled by task.')
        
        self.logger.info('...scheduling %s new task(s).', len(new_tasks))
        for new in new_tasks:
            if 'name' in new and 'classname' in new and 'interval' in new:
                newtask = Task(name=new['name'],
                               classname=new['classname'],
                               interval=new['interval'],
                               kwargs=new.get('kwargs',{}),
                               only_once=new.get('only_once',False),
                               max_runs=new.get('max_runs',None),
                               only_before=new.get('only_before',None))
                self.add_task(newtask, offset=new['interval'])
            else:
                self.logger.error('New task contained insuffient arguments to be created: %s', new)
                
        for callback in self.callbacks:
            try:
                callback(result)
            except Exception, e:
                self.logger.error('Exception occurred during result dispatch for Task "%s" (%s). Error was %s',
                                  task.name, taskid, str(e))
                
                               
                               
                           
        