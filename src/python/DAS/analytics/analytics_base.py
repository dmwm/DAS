#!/usr/bin/env python

import multiprocessing
import time
import DAS.core.das_core

class DASAnalyticsBase(multiprocessing.Process):
    """
    Basic managed analytics class. This is a process that is run quasi-independent of the core
    daemon. The main loop automatically handles messages and timing, calling the analysis_run
    method at a given interval.
    """
    stats = {}
    defaults = {
                'interval':300,
                }
    
    @classmethod
    def merge_defaults(cls,config):
        """
        Merge the supplied config with the defaults in this class.
        """
        default_copy = copy.deepcopy(cls.defaults)
        default_copy.update(config)
        return default_copy
    
    @staticmethod
    def validate_config(config):
        """
        Stub to be overriden, if validation rules are required for the supplied config.
        Return false if something is irredeemably wrong.
        """
        return True
    
    def __init__(self,name,config,das_config,controller_pipe):
        """
        Create a new analytic class. Subclasses should generally not override the details
        of this class, but use the configure() method (called at __init__ time).
        """
        multiprocessing.Process.__init__(self)
        self.config = config
        self.das_config = das_config
        self.das_core = DAS.core.das_core.DASCore(das_config)
        self._pipe = controller_pipe
        self.name = name
        self.internal_interval = 1
        self.analysis_interval = config['interval']       
        self.log(0,"created")
        self._ticks = 0
        self._last_analysis_time = 0
        self._analysis_calls = 0
        self._min_analysis_time = 0
        self._max_analysis_time = 1e30
        self._total_analysis_time = 0
        self._time_started = 0
        self._stop = False
        self.configure(config)
    
    def configure(self,config):
        """
        Stub for subclasses to perform any init-time configuration, before the main loop is entered.
        """
        pass
        
    def analysis_start(self):
        """
        Stub for subclasses to perform any actions when the main loop is first started.
        """
        pass
    
    def analysis_run(self):
        """
        Stub for subclasses to perform the main analysis action. Should be called every 'interval' seconds.
        """
        pass
    
    def run(self):
        """
        Actual main loop. This should not be overridden (use analysis_run), as it handles receiving pipe messages,
        calling the analysis at correct intervals and exiting if necessary.
        """
        self.log(0,"started")
        self._time_started = time.time()
        self.analysis_start()
        while True:
            self._ticks += 1
            self._pipe_handler()
            
            if self._stop:
                return
            
            analysis_time = 0
            if time.time()-self._last_analysis_time > self.analysis_interval:
                self._last_analysis_time = time.time()
                analysis_time = time.time()
                self.analysis_run()
                analysis_time = time.time()-analysis_time
                self._total_analysis_time += analysis_time
                if analysis_time < self._min_analysis_time:
                    self._min_analysis_time = analysis_time
                if analysis_time > self._max_analysis_time:
                    self._max_analysis_time = analysis_time
            
            time.sleep(self.internal_interval)
            
    def log(self,importance,message):
        self._pipe.send({'type':'log','time':time.time(),'level':importance,'text':message,'src':self.name})
    
    def _pipe_handler(self):
        """
        Poll the incoming pipe for messages, then pass them to functions named pipe_$msg.type if available.
        Pass them to pipe_default otherwise. If those functions return a value, pass it back as a pipe message.
        """
        while self._pipe.poll():
            msg = self._pipe.recv()
            if msg and isinstance(msg,dict):
                msg['type'] = msg.get('type','default')
                funcname = 'pipe_%s'%msg['type']
                if hasattr(self,funcname):
                    if callable(getattr(self,funcname)):
                        retval = self.__dict__[funcname](msg)
                        if not retval==None:
                            if not isinstance(retval,dict):
                                retval = {'data':retval}
                            retval['src'] = self.name
                            retval['type'] = msg['type']
                            self._pipe.send(retval)
                    else:
                        self.pipe_default(msg)
                else:
                    self.pipe_default(msg)
            else:
                self.log(1,'Invalid pipe msg: %s'%msg)
    
    def pipe_default(self,msg):
        """
        Handle messages for which no specialist handler is available.
        """
        self.log(1,'Attempted to call invalid pipe function "%s"'%msg['type'])
                
    def pipe_ping(self,msg):
        """
        Respond to heartbeats by replying with the same message.
        """
        return msg
    
    def pipe_stats(self,msg):
        """
        Provide any stats this analyser may collect. These should correspond to entries in self.statdesc.
        """
        return {}
    
    def pipe_stop(self,msg):
        """
        Try and stop the analyser gracefully. Calls finalise and then stops the main loop.
        """
        self._stop = True
        self.finalise()
        
    def pipe_started(self,msg):
        """
        Report the time we started running at.
        """
        return self._time_started
    
    def pipe_status(self,msg):
        """
        Reply with a quick status message. This should be overridden to provide informative responses.
        """
        return "Still Alive"
    
    def finalise(self):
        """
        Stub for subclasses, to perform clean-up actions before shutdown.
        """
        pass
    