#!/usr/bin/env python

import multiprocessing
import time
import copy

class DASServiceBase(multiprocessing.Process):
    stats = {}
    defaults = {'interval':1}
    
    @classmethod
    def merge_defaults(cls,config):
        default_copy = copy.deepcopy(cls.defaults)
        default_copy.update(config)
        return default_copy
    
    @staticmethod
    def validate_config(config):
        return True
    
    def __init__(self,name,config,controller_pipe):
        multiprocessing.Process.__init__(self)
        self.config = config
        self._pipe = controller_pipe
        self.name = name
        self.log(0,"created")
        self._ticks = 0
        self._time_started = 0
        self._stop = False
        self.configure(config)
    
    def configure(self,config):
        pass
        
    def service_start(self):
        pass
    
    def service_run(self):
        pass
    
    def run(self):
        self.log(0,"started")
        self._time_started = time.time()
        self.service_start()
        while True:
            self._ticks += 1
            self._pipe_handler()
            if self._stop:
                return
            self.service_run()
            time.sleep(self.config['interval'])
            
    def log(self,importance,message):
        self._pipe.send({'type':'log','time':time.time(),'level':importance,'text':message, 'src':self.name})
    
    def _pipe_handler(self):
        while self._pipe.poll():
            msg = self._pipe.recv()
            if msg and isinstance(msg,dict):
                msg['type'] = msg.get('type','default')
                funcname = 'pipe_%s'%msg['type']
                if hasattr(self,funcname):
                    if callable(getattr(self,funcname)):
                        retval = getattr(self,funcname)(msg)
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
        self.log(1,'Attempted to call invalid pipe function "%s"'%msg['type'])
                
    def pipe_ping(self,msg):
        return msg
    
    def pipe_stats(self,msg):
        return {}
        
    def pipe_statdesc(self,msg):
        return {}
    
    def pipe_stop(self,msg):
        self._stop = True
        self.finalise()
        
    def pipe_started(self,msg):
        return self._time_started
    
    def pipe_status(self,msg):
        return "Still Alive"
    
    def finalise(self):
        pass
