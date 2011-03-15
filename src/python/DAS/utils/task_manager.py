#!/usr/bin/python

"""
Task manager

spawn(func, *args) to spawn execution of given func(args)
is_alive(pid) return status of executing job
joinall() to join all tasks in a queue and exiting existing workers
join(jobs) to join all tasks without stopping workers

Usage of multiprocessing pool is limited to serializable functions, since using
class objects lead to the following error
Can't pickle <type 'instancemethod'> when using python's multiprocessing Pool
http://stackoverflow.com/questions/1816958/cant-pickle-type-instancemethod-when-using-pythons-multiprocessing-pool-map
http://bytes.com/topic/python/answers/552476-why-cant-you-pickle-instancemethods
"""

import time
from threading import Thread, Event
from Queue import Queue
from DAS.utils.utils import genkey

class Worker(Thread):
    """Thread executing worker from a given tasks queue"""
    def __init__(self, taskq, pidq):
        Thread.__init__(self)
        self._tasks = taskq
        self._pids  = pidq
        self.daemon = True
        self.start()

    def run(self):
        """Run thread loop."""
        while True:
            task = self._tasks.get()
            if  task == None:
                return
            evt, pid, func, args = task
            try:
                func(*args)
                self._pids.discard(pid)
            except Exception, err: 
                self._pids.discard(pid)
                print err
            evt.set()

class TaskManager:
    """
    Task manager class based on thread module which
    executes assigned tasks of functions concurently.
    It uses a pool of thread workers, queue of tasks
    and pid set to monitor jobs execution.
    """
    def __init__(self, num_workers = 10):
        self._pids  = set()
        self._tasks = Queue()
        self._workers = [Worker(self._tasks, self._pids) \
                        for _ in xrange(0, num_workers)]

    def spawn(self, func, *args):
        """Spawn new process for given function"""
        pid = genkey(str(args) + str(time.time()))
        self._pids.add(pid)
        ready = Event()
        self._tasks.put((ready, pid, func, args))
        return ready, pid

    def is_alive(self, pid):
        """Check worker queue if given pid of the process is still running"""
        return pid in self._pids 

    def join(self, tasks):
        """Join all tasks in a queue"""
        map(lambda (evt, pid): (evt.wait(), pid), tasks)

    def joinall(self, tasks):
        """Join all tasks in a queue and quite"""
        self.join(tasks)
        self.quit()

    def quit(self):
        """Put None task to all workers and let them quit"""
        map(lambda w: self._tasks.put(None), self._workers)
        map(lambda w: w.join(), self._workers)
