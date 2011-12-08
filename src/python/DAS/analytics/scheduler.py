#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
General set of useful utilities used by DAS
"""

__author__ = "Gordon Ball"

# system modules
import logging
import time
import multiprocessing
import copy
import threading
import signal

# DAS modules
from DAS.analytics.config import DASAnalyticsConfig
from DAS.analytics.task import Task
from DAS.utils.utils import print_exc
from cherrypy.process import plugins

#prevent the worker catching SIGINT, which causes a deadlock in
#multiprocessing.Pool - workaround from
#http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool
#this arises because not isinstance(KeyboardInterrupt, Exception)
#Disabling catching SIGINT is apparently also a viable solution
def poolsafe(func):
    "Poolsafe wrapper"
    def wrapper(*args, **kwargs):
        #some redundancy here, but just trapping KeyboardInterrupt didn't seem to work
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        "Worker wrapper"
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            #raise Exception("KeyboardInterrupt as Exception")
            return
    return wrapper
#monkey-patch multiprocessing worker function
from multiprocessing.pool import worker
multiprocessing.pool.worker = poolsafe(worker) 

DASAnalyticsConfig.add_option("max_retries", type=int, default=1,
    help="Number of times the task scheduler will retry a failed task before abandoning it.")
DASAnalyticsConfig.add_option("retry_delay", type=int, default=60,
    help="Seconds to wait before retrying a failed job.")
DASAnalyticsConfig.add_option("minimum_interval", type=int, default=60,
    help="Minimum repeat interval allowed for a task.")
DASAnalyticsConfig.add_option("workers", type=int, default=4,
    help="Number of workers to use for tasks.")
class TaskScheduler(plugins.SimplePlugin):
    """
    This is a cron-like thread which keeps track of all the tasks to
    be run, and dispatches them to the worker pool at the appropriate
    time.
    
    A completed job calls back to this thread to request that it be
    re-scheduled at some point in the future.
    
    A job that fails will be retried a specified number of times before
    being prevented from being resubmitted.
    
    This attaches to the cherrypy bus to receive start and stop signals.
    """
    def __init__(self, config, engine):
        plugins.SimplePlugin.__init__(self, engine)
        self.logger = logging.getLogger("DASAnalytics.TaskScheduler")
                
        self.scheduled = [] # -> (time, task)
        self.wakeup_at = None
        
        self.max_retries = config.max_retries
        self.retry_delay = config.retry_delay
        self.minimum_interval = config.minimum_interval
        self.workers = config.workers
        
        self.pool = None
        
        self.registry = {}
        self.callbacks = []
        self.running = {}
        
        #since this handles requests from the web threads,
        #pool monitor thread and main thread, we need
        #to lock access
        self.lock = multiprocessing.Lock()
        
        #whether we should kill the main loop
        self.terminate = False
    
    def start(self):
        "Receive the start signal from cherrypy bus and run the main loop."
        self.pool = multiprocessing.Pool(processes=self.workers)
        self.terminate = False
        #this may be meant to run in a new thread
        #note that we don't get a lock here, since run() subsequently gets the lock
        #and this should be occurring once before anything else
        thread = threading.Thread(target=self.run)
        thread.start()
    
    def stop(self):
        "Set the termination signal and request the pool terminate gracefully."
        #unclear what the correct locking should be here
        #since holding the lock for join() would prevent any callbacks running
        self.terminate = True
        if self.pool:
            self.pool.terminate() #sod it, kill it anyway
            self.pool.join()
            self.pool = None
    
    exit = stop
 
    def run(self):
        """
        Called by the controller main loop. Checks if there is
        a job due to be dispatched and dispatches it.
        """
        while True:
            with self.lock:
                if self.terminate:
                    return
                
                now = time.time()
                if self.wakeup_at and now > self.wakeup_at:
                    while self.scheduled and self.scheduled[0]['at'] < now:
                        self._submit_task()
                        self._set_next_task()
            time.sleep(1)
        
    def add_callback(self, callback):
        "Add a callback to be dispatched when a job result returns."
        self.callbacks.append(callback)
    
    def get_scheduled(self):
        "Get a de-classed list of current tasks for web interface."
        with self.lock:
            return copy.deepcopy(self.scheduled)
    
    def get_task(self, master_id):
        "Get a dict describing a single task from the registry"
        with self.lock:
            task = self.registry.get(master_id, None)
            if task:
                return task.get_dict()
            else:
                return None
            
    def get_children(self, master_id):
        "Get task children"
        with self.lock:
            return [m_id for m_id, task in self.registry.items() 
                    if master_id in task.parent]
    
    def add_task(self, task, when=None, offset=None):
        """
        Wrapper around _add_task (which can be called from an
        already locked context or an external context)
        """
        with self.lock:
            self._add_task(task, when, offset)
            return True
    
    def _add_task(self, task, when=None, offset=None):
        """
        Add a new task, specifying either an absolute time
        or an offset from now.
        """
        if not when:
            when = time.time() + self.minimum_interval
        if offset:
            when = time.time() + offset
            
        self.logger.info('Adding Task "%s" to TaskScheduler, scheduled in %d',
                         task.name, int(when - time.time()))
        
        self.registry[task.master_id] = task
        self.scheduled.append({'at':when, 'master_id':task.master_id,
                               'name': task.name, 'classname': task.classname})
        self._set_next_task()
        
    def remove_task(self, master_id):
        """
        Request the task with given ID be removed from the schedule
        Note, does not work if the task has already started running
        """
        with self.lock:
            if master_id in [x['master_id'] for x in self.scheduled]:
                self.scheduled = filter(lambda x: x['master_id'] != master_id,
                                        self.scheduled)
                self._set_next_task()
                return True
            else:
                return False
            
    def reschedule_task(self, masterid, when):
        """
        Request the task be moved to a different scheduled time.
        """
        with self.lock:
            for item in self.scheduled:
                if item['master_id'] == masterid:
                    item['at'] = when
                    self._set_next_task()
                    return True
            return False
    
    def _set_next_task(self):
        """
        Internal, re-sort the queue and record the next submission time
        """
        if len(self.scheduled) > 0:
            self.scheduled = sorted(self.scheduled, key=lambda x: x['at'])
            self.wakeup_at = self.scheduled[0]['at']
        else:
            self.wakeup_at = None        
    
    def _submit_task(self):
        "Do job submission."
        now = time.time()
        if len(self.scheduled) > 0:
            item = self.scheduled.pop(0)
            master_id = item['master_id']
            task = self.registry[master_id]
            self.logger.info('Submitting Task "%s".', task.name)
            runnable = task.spawn()
            task_id = (runnable.master_id, runnable.index)
            self.logger.info('Spawning Task "%s:%s"',
                             runnable.name, runnable.index)
            self.running[task_id] = {'master_id':master_id,
                                     'name':task.name,
                                     'classname':task.classname,
                                     'started': now,
                                     'index': runnable.index}
            self.pool.apply_async(runnable,
                                  callback=self.task_finished_callback)
            task.last_run_at = now
    
    def get_running(self):
        """
        Get a list of the currently running tasks.
        """
        with self.lock:
            return self.running.values()
        
    def get_registry(self):
        """
        Get all known tasks (which may not necessarily be scheduled
        or running)
        """
        with self.lock:
            return dict([(mid, self.registry[mid].get_dict()) \
                        for mid in self.registry])
            
    def task_finished_callback(self, result):
        "Callback each finished job hits."
        with self.lock:
            now = time.time()
            task_id = (result.get('master_id', None), result.get('index', None))
            success = result.get('success', False)
            resubmit = result.get('resubmit', True)
            new_tasks = result.get('new_tasks', [])
            item = self.running[task_id]
            del self.running[task_id]
            master_id = item['master_id']
            task = self.registry[master_id]
        
            self.logger.info('Finished Task "%s" (%s) success=%s.',
                             task.name, task_id, success)
            
            if 'next' in result:
                self.logger.info('Task "%s" (%s) requesting resubmission at %s',
                                 task.name, task_id, result['next'])
                interval = max(self.minimum_interval, result['next'] - now)
            else:
                interval = task.interval
        
            if resubmit:
                if success:
                    task.retries = 0
                    when = task.last_run_at + interval
                    if when < now + self.minimum_interval:
                        when = now + self.minimum_interval
                    if task.can_respawn(when):
                        self.logger.info('..scheduling next run.')
                        self._add_task(task, when=when)
                    else:
                        msg = '...exceeded maximum runs or time cutoff, not scheduling.'
                        self.logger.info(msg)
                else:
                    if task.retries <= self.max_retries:
                        self.logger.info('...retrying in %s.', self.retry_delay)
                        task.retries += 1
                        self._add_task(task, offset=self.retry_delay)
                    else:
                        msg = '...failed and no more retries remaining.'
                        self.logger.info(msg)
            else:
                self.logger.info('...rescheduling disabled by task.')
        
            self.logger.info('...scheduling %s new task(s).', len(new_tasks))
            for new in new_tasks:
                if 'name' in new and 'classname' in new and 'interval' in new:
                    newtask = Task(name=new['name'],
                                   classname=new['classname'],
                                   interval=new['interval'],
                                   kwargs=new.get('kwargs', {}),
                                   max_runs=new.get('max_runs', None),
                                   only_before=new.get('only_before', None),
                               parent=result.get('parent',())+(task.master_id,))
                    self._add_task(newtask, offset=new['interval'])
                else:
                    self.logger.error('New task contained insufficient arguments to be created: %s', new)
                
            for callback in self.callbacks:
                try:
                    callback(result)
                except Exception as exc:
                    msg = 'ERROR: dispatch for task=%s, id=%s, error=%s' \
                        % (task.name, task_id, exc[0])
                    self.logger.error(msg)
                    print_exc(exc)
