#!/usr/bin/env python

def traced(func):
    """
    Print a message when the decorated function runs, 
    along with name, code location, arguments etc.
    Might be useful for debugging.
    """
    def inner(*args, **kwargs):
        """print function details"""
        print 'Trace', func.__name__, \
        '%s:%s' % (func.__code__.co_filename, func.__code__.co_firstlineno),\
                args, kwargs
        return func(*args, **kwargs)
    return inner

class FakePipe:
    """
    A dummy version of multiprocessing.
    Pipe which does nothing except print data passed to it,
    for use during startup (when nothing would listen to pipes).
    """
    def send(self, data):
        """send data"""
        print data
    def recv(self):
        """received data"""
        raise
    def poll(self):
        """poll data"""
        return False

class DASAnalyticsConfig:
    """
    Config class for analytics. 
    The functions in this are provided in the global namespace
    when executing the das analytics config file, and 
    the variables are defaults that can be overridden.
    """
    pidfile = '/tmp/das-analytics.pid'
    core_interval = 1
    analyser_interval = 5
    heartbeat_interval = 60
    heartbeat_warn = 1
    heartbeat_restart = 3
    stop_time = 60
    
    modules = []
    _analysers = {}
    _services = {}

    def analyser(self, klass, name=None, **kwargs):
        """
        Add a new analyser with class=klass. 
        Optionally a name may be provided (so multiple instances
        of the same class may run with different arguments, 
        otherwise the name will be the classname).
        """
        if name == None:
            name = klass
        if name in self._analysers:
            print "Warning: '%s' already defined" % name
        self._analysers[name] = (klass, kwargs)

    def service(self, klass, name=None, **kwargs):
        """
        Add a new service with class=klass. 
        Optionally a name may be provided (so multiple instances
        of the same class may run with different arguments, 
        otherwise the name will be the classname).
        """
        if name == None:
            name = klass
        if name in self._services:
            print "Warning: '%s' already defined" % name
        self._services[name] = (klass, kwargs)
    
