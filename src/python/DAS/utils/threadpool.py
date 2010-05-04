#/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Python thread pool, see
http://code.activestate.com/recipes/577187-python-thread-pool/
"""

__revision__ = "$Id: threadpool.py,v 1.2 2010/05/04 13:32:47 valya Exp $"
__version__ = "$Revision: 1.2 $"

from Queue import Queue
from threading import Thread

class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()
    
    def run(self):
        """Run thread loop."""
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception, exp:
                print exp
            self.tasks.task_done()

class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()

    def full(self):
        """Return status of the Queue"""
        return self.tasks.full()
