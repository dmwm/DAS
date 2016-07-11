#!/usr/bin/python
#pylint: disable=W0141
"""
Task manager

spawn(func, \*args) to spawn execution of given func(args)
is_alive(pid) return status of executing job
joinall() to join all tasks in a queue and exiting existing workers
join(jobs) to join all tasks without stopping workers
"""
from __future__ import print_function

# system modules
from cherrypy.process import plugins
from threading import Thread, Event
try:
    from Queue import Queue, PriorityQueue
except ImportError: # python3
    from queue import Queue
    from queue import Queue as PriorityQueue

# DAS modules
from DAS.utils.utils import genkey, print_exc

class UidSet(object):
    "UID holder keeps track of uid frequency"
    def __init__(self):
        self._set = {}

    def add(self, uid):
        "Add given uid or increment uid occurence in a set"
        if  not uid:
            return
        if  uid in self._set.keys():
            self._set[uid] += 1
        else:
            self._set[uid]  = 1

    def discard(self, uid):
        "Either discard or downgrade uid occurence in a set"
        if  uid in self._set:
            self._set[uid] -= 1
        if  uid in self._set and not self._set[uid]:
            del self._set[uid]

    def __contains__(self, uid):
        "Check if uid present in a set"
        if  uid in self._set:
            return True
        return False

    def get(self, uid):
        "Get value for given uid"
        return self._set.get(uid, 0)

class Worker(Thread):
    """Thread executing worker from a given tasks queue"""
    def __init__(self, name, taskq, pidq, uidq):
        Thread.__init__(self, name=name)
        self.exit   = 0
        self._tasks = taskq
        self._pids  = pidq
        self._uids  = uidq
        self.daemon = True
        self.start()

    def force_exit(self):
        """Force run loop to exit in a hard way"""
        self.exit   = 1

    def run(self):
        """Run thread loop."""
        while True:
            if  self.exit:
                return
            if  isinstance(self._tasks, PriorityQueue):
                _, uid, task = self._tasks.get()
            else:
                task = self._tasks.get()
            if  task == None:
                return
            evt, pid, func, args, kwargs = task
            try:
                if  isinstance(self._tasks, PriorityQueue):
                    self._uids.discard(uid)
                func(*args, **kwargs)
                self._pids.discard(pid)
            except Exception as err:
                self._pids.discard(pid)
                print_exc(err)
                print("\n### args", func, args, kwargs)
            evt.set()

class TaskManager(object):
    """
    Task manager class based on thread module which
    executes assigned tasks concurently. It uses a
    pool of thread workers, queue of tasks and pid
    set to monitor jobs execution.

    .. doctest::

        Use case:
        mgr  = TaskManager()
        jobs = []
        jobs.append(mgr.spaw(func, args))
        mgr.joinall(jobs)

    """
    def __init__(self, nworkers=10, name='TaskManager', qtype='Queue', debug=0, qfreq=5):
        self.name   = name
        self.debug  = debug
        self.qfreq = qfreq
        self._pids  = set()
        self._uids  = UidSet()
        if  qtype == 'PriorityQueue':
            self._tasks = PriorityQueue()
        else:
            self._tasks = Queue()
        self._workers = [Worker(name, self._tasks, self._pids, self._uids) \
                        for _ in range(0, nworkers)]

    def status(self):
        "Return status of task manager queue"
        info = {'qsize':self._tasks.qsize(), 'full':self._tasks.full(),
                'unfinished':self._tasks.unfinished_tasks,
                'nworkers':len(self._workers)}
        return {self.name:info}

    def nworkers(self):
        """Return number of workers associated with this manager"""
        return len(self._workers)

    def assign_priority(self, uid):
        "Assign priority for given uid"
        return self._uids.get(uid)/self.qfreq

    def spawn(self, func, *args, **kwargs):
        """Spawn new process for given function"""
        pid = kwargs.get('pid', genkey(str(args) + str(kwargs)))
        evt = Event()
        if  not pid in self._pids:
            self._pids.add(pid)
            task  = (evt, pid, func, args, kwargs)
            if  isinstance(self._tasks, PriorityQueue):
                uid = kwargs.get('uid', None)
                self._uids.add(uid)
                priority = self.assign_priority(uid)
                self._tasks.put((priority, uid, task))
            else:
                self._tasks.put(task)
        else:
            # the event was not added to task list, invoke set()
            # to pass it in wait() call, see joinall
            evt.set()
        return evt, pid

    def remove(self, pid):
        """Remove pid and associative process from the queue"""
        self._pids.discard(pid)

    def is_alive(self, pid):
        """Check worker queue if given pid of the process is still running"""
        return pid in self._pids

    def clear(self, tasks):
        """
        Clear all tasks in a queue. It allows current jobs to run, but will
        block all new requests till workers event flag is set again
        """
        map(lambda evt_pid: (evt_pid[0].clear(), evt_pid[1]), tasks)

    def joinall(self, tasks):
        """Join all tasks in a queue and quite"""
        map(lambda evt_pid1: (evt_pid1[0].wait(), evt_pid1[1]), tasks)

    def quit(self):
        """Put None task to all workers and let them quit"""
        map(lambda w: self._tasks.put(None), self._workers)
        map(lambda w: w.join(), self._workers)

    def force_exit(self):
        """Force all workers to exit"""
        map(lambda w: w.force_exit(), self._workers)

class PluginTaskManager(TaskManager, plugins.SimplePlugin):
    """
    PluginTaksManager add start/stop/graceful functionality
    to base TaskManager class for provided bus engine, e.g.
    cherrypy.engine
    """
    def __init__(self, bus, nworkers=10, name='PluginTaskManager',
                    qtype='Queue', debug=0, qfreq=5):
        plugins.SimplePlugin.__init__(self, bus)
        TaskManager.__init__(self, nworkers, name, debug, qfreq)
        if  debug:
            print("%s init with %s workers" % (name, nworkers))

    def start(self):
        """
        Implementation for external bus, e.g. cherrypy.engine.
        When external bus will start this method will be invoked.
        """
        if  self.debug:
            print("%s start" % self.name)

    def stop(self):
        """
        Implementation for external bus, e.g. cherrypy.engine.
        When external bus will stop this method will be invoked.
        """
        if  self.debug:
            print("%s stop" % self.name)
        self.force_exit()

    def exit(self):
        """
        Implementation for external bus, e.g. cherrypy.engine.
        When external bus will exit this method will be invoked.
        """
        if  self.debug:
            print("%s exit" % self.name)
        self.stop()

    def graceful(self):
        """
        Implementation for external bus, e.g. cherrypy.engine.
        When external bus will stop this method will be invoked.
        """
        if  self.debug:
            print("%s graceful" % self.name)
        self.quit()

