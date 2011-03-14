#!/usr/bin/python

"""
Task manager which spawn multiple workers and wait for them to complete
"""

from multiprocessing import Process

class TaskManager(object):
    """Task manager class executes assigned tasks of functions concurently"""
    def __init__(self):
        self.tasks = {}

    def spawn(self, func, *args):
        """Spawn new process for given function"""
        process = Process(target=func, args=args)
        process.start()
        self.tasks[process.pid] = process

    def joinall(self):
        """Join all processes in a queue"""
        while True:
            if  not self.tasks:
                break
            for pid, process in self.tasks.items():
                if  not process.is_alive():
                    self.tasks.pop(pid)
