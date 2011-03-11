#/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Multiprocessing pool with single worker.
"""

from multiprocessing import Pool, Queue
from Queue import Empty, Full

class MultiprocessingPool(object):
    """
    Pool class using multiprocessing module. It uses a given worker,
    a fixed pool size and a queue with given size.
    Tasks are assigned to the queue and executed by pool of workers.
    """
    def __init__(self, pool_size, queue_size, worker):
        self.size   = pool_size
        self.tasks  = multiprocessing.Queue(queue_size)
        self.pool   = multiprocessing.Pool(pool_size)
        self.worker = worker

    def add_query(self, query):
        """
        add new query to the queue
        """
        if  self.tasks.full():
            raise Full
        self.tasks.put(query)

    def cleaner(self, pool_tasks):
        """
        Clean tasks from pool of workers which are completed
        """
        for query, res in pool_tasks.items():
            if  res.ready():
                del pool_tasks[query]

    def run(self):
        """
        Run pool of worker which consume queries
        """
        pool_tasks = {}
        while True:
            self.cleaner(pool_tasks)
            if  len(pool_tasks.keys()) < self.size:
                try:
                    query = self.tasks.get_nowait()
                    if  not pool_tasks.has_key(query):
                        res = self.pool.apply_async(self.worker, (query,))
                        pool_tasks[query] = res
                except Empty:
                    pass
            time.sleep(1)

