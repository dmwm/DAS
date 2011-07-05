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

# DAS modules
from DAS.analytics.analytics_config import DASAnalyticsConfig
from DAS.analytics.analytics_task import Task
from DAS.utils.utils import dastimestamp, print_exc

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
    def __init__(self, controller, config):
        self.controller = controller
        self.logger = logging.getLogger("DASAnalytics.TaskScheduler")
                
        self.scheduled = [] # -> (time, task)
        self.wakeup_at = None
        
        self.max_retries = config.max_retries
        self.retry_delay = config.retry_delay
        self.minimum_interval = config.minimum_interval
        
        self.pool = multiprocessing.Pool(processes=config.workers)
        
        self.registry = {}
        
        self.callbacks = []
        
        self.running = {}
        
        #since this handles requests from the web threads,
        #pool monitor thread and main thread, we need
        #to lock access
        self.lock = multiprocessing.Lock()
        
        #self.loghandler = RunningJobHandler()
        #self.loghandler.addFilter(logging.Filter("DASAnalytics.Task"))
        #self.controller.logger.addHandler(self.loghandler)
    
    def add_callback(self, callback):
        "Add a callback to be dispatched when a job result returns."
        self.callbacks.append(callback)
    
    def get_scheduled(self):
        "Get a de-classed list of current tasks for web interface."
        self.lock.acquire()
        try:
            return copy.deepcopy(self.scheduled)
        finally:
            self.lock.release()
    
    def get_task(self, master_id):
        "Get a dict describing a single task from the registry"
        self.lock.acquire()
        try:
            task = self.registry.get(master_id, None)
            if task:
                return task.get_dict()
            else:
                return None
        finally:
            self.lock.release()
    
    def add_task(self, task, when=None, offset=None):
        """
        Wrapper around _add_task (which can be called from an
        already locked context or an external context)
        """
        self.lock.acquire()
        try:
            self._add_task(task, when, offset)
            return True
        finally:
            self.lock.release()
    
    def _add_task(self, task, when=None, offset=None):
        "Add a new task, specifying either an absolute time or an offset from now."
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
        self.lock.acquire()
        try:
            if master_id in [x['master_id'] for x in self.scheduled]:
                self.scheduled = filter(lambda x: x['master_id'] != master_id,
                                        self.scheduled)
                self._set_next_task()
                return True
            else:
                return False
        finally:
            self.lock.release()
            
    def reschedule_task(self, masterid, when):
        """
        Request the task be moved to a different scheduled time.
        """
        self.lock.acquire()
        try:
            for item in self.scheduled:
                if item['master_id'] == masterid:
                    item['at'] = when
                    self._set_next_task()
                    return True
            return False
        finally:
            self.lock.release()
    
    def _set_next_task(self):
        """
        Internal, re-sort the queue and record the next submission time
        """
        if len(self.scheduled) > 0:
            self.scheduled = sorted(self.scheduled, key=lambda x: x['at'])
            self.wakeup_at = self.scheduled[0]['at']
        else:
            self.wakeup_at = None
        
    def run(self):
        """
        Called by the controller main loop. Checks if there is
        a job due to be dispatched and dispatches it.
        """
        self.lock.acquire()
        try:
            
            now = time.time()
            if self.wakeup_at and now > self.wakeup_at:
                self._submit_task()
                self._set_next_task()
        finally:
            self.lock.release()
        
    
    def _submit_task(self):
        "Do job submission."
        now = time.time()
        if len(self.scheduled) > 0:
            item = self.scheduled.pop(0)
            master_id = item['master_id']
            task = self.registry[master_id]
            self.logger.info('Submitting Task "%s".', task.name)
            tstamp = dastimestamp()
            runnable = task.spawn()
            task_id = runnable.get_task_id()
            #self.loghandler.add_watch(task_id)
            self.logger.info('Task "%s" has TaskID %s',
                             runnable.name, task_id)
            msg = '%s task=%s, id=%s' % (tstamp, runnable.name, task_id)
            print msg
            self.running[task_id] = {'master_id':master_id,
                                     'name':task.name,
                                     'classname':task.classname,
                                     'started': now}
            self.pool.apply_async(runnable,
                                  callback=self.task_finished_callback)
            task.last_run_at = now
    
    def get_running(self):
        """
        Get a list of the currently running tasks.
        """
        self.lock.acquire()
        try:
            result = []
            for task_id, item in self.running.items():
                item['task_id'] = task_id
                result.append(item)
            return result
        finally:
            self.lock.release()
            
    def task_finished_callback(self, result):
        "Callback each finished job hits."
        self.lock.acquire()
        try:
            now = time.time()
            task_id = result.get('task_id', None)
            success = result.get('success', False)
            resubmit = result.get('resubmit', True)
            new_tasks = result.get('new_tasks', [])
            item = self.running[task_id]
            del self.running[task_id]
            master_id = item['master_id']
            task = self.registry[master_id]
        
            self.logger.info('Finished Task "%s" (%s) success=%s.',
                             task.name, task_id, success)
            
            result['log'] = []#self.loghandler.get(task_id)
            
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
                        self._add_task(task, when=task.last_run_at + interval)
                    else:
                        self.logger.info('...exceeded maximum runs or time cutoff, not scheduling.')
                else:
                    if task.retries <= self.max_retries:
                        self.logger.info('...retrying in %s.', self.retry_delay)
                        task.retries += 1
                        self._add_task(task, offset=self.retry_delay)
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
                                   kwargs=new.get('kwargs', {}),
                                   only_once=new.get('only_once', False),
                                   max_runs=new.get('max_runs', None),
                                   only_before=new.get('only_before', None),
                                   parent=task.master_id)
                    self._add_task(newtask, offset=new['interval'])
                else:
                    self.logger.error('New task contained insuffient arguments to be created: %s', new)
                
            for callback in self.callbacks:
                try:
                    callback(result)
                except Exception as exc:
                    msg = 'ERROR: dispatch for task=%s, id=%s' \
                        % (task.name, task_id)
                    print msg
                    print_exc(exc)
        finally:
            self.lock.release()
        
