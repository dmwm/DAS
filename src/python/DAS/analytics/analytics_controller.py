#!/usr/bin/env python

import DAS.core.das_robot
import DAS.core.das_core
import DAS.utils.das_config
import multiprocessing
import os
import DAS.core.das_core
import time
import sys
import analytics_utils
import analytics_base
import analytics_service
import fnmatch

class DASAnalyticsController(DAS.core.das_robot.Robot):
    """
    Analytics Controller
    
    This is the sole daemon process started for analytics, which spawns
    a series of owned processes (using multiprocessing.Process). These
    processes perform actual functions, and are divided into analysers
    (which connect to the analytics DB and perform periodic calculations)
    and services (which are assistance modules to the analytics daemon, such
    as logging and web access).   
    """
    def __init__(self, das_config, analytics_config, daemon=True):
        """
        Create a new DASAnalyticController. 
        das_config is a DAS.utils.das_config.das_config object.
        analytics_config is a path to a analytics config
        daemon determines whether to detach and fork.
        """
        self.das_config = das_config
        self.config = self.read_config(analytics_config)
        self.daemon = daemon
        self.pidfile = self.config.pidfile
        
        self.analysers = {}
        self.analyser_pipes = {}
        self.services = {}
        self.service_pipes = {}
        self.all_pipes = {}
        self.subscriptions = []
        
        self.current_heartbeat = 0
        self.last_heartbeat = {}
        
        self.master_pipe = analytics_utils.FakePipe()
        
        self.timers = []
        
        self._stop = False
    
    def read_config(self, conffile):
        """
        Read the analytics config by executing the supplied file with, using a DASAnalyticConfig object as the global namespace.
        """
        config_object = analytics_utils.DASAnalyticsConfig()
        exec_dict = {'analyser':config_object.analyser, 'service':config_object.service}
        execfile(conffile, exec_dict)
        config_object.__dict__.update(exec_dict)
        return config_object
        
    def load_modules(self, config):
        """
        Import all the modules defined in DASAnalyticsConfig.modules, 
        then load the necessary classes.
        Set up the pipes for communication, and add any message 
        subscriptions defined by services.
        Don't actually start them yet.
        """
        modules = []
        for module in config.modules:
            if '.' in module:
                modules += [__import__(module, fromlist=[module.split('.')[-1]])]
            else:
                modules += [__import__(module)]
        
        def get_class(name):
            for module in modules:
                if hasattr(module, name):
                    return getattr(module, name)
            return None
        
        for a_name, (a_classname, a_config) in config._analysers.items():
            a_class = get_class(a_classname)
            if a_class:
                if not analytics_base.DASAnalyticsBase in a_class.__bases__:
                    print "Warning: DASAnalyticsBase not in superclasses of analyser %s (class %s, bases %s)" % (a_name, a_classname, a_class.__bases__)
                a_config = a_class.merge_defaults(a_config)
                validation = a_class.validate_config(a_config)
                if validation == True:
                    master_pipe, slave_pipe = multiprocessing.Pipe()
                    self.analysers[a_name] = \
                        a_class(a_name, a_config, self.das_config, slave_pipe)
                    self.analysers[a_name].internal_interval = self.config.analyser_interval
                    self.analyser_pipes[a_name] = master_pipe
                    self.all_pipes[a_name] = master_pipe
                    self.last_heartbeat[a_name] = (0, 0)
                else:
                    print "Validation failed for analyser %s." % a_name
                    print str(validation)
            else:
                print "Class '%s' for analyser '%s' not found."\
                        % (a_classname, a_name)
                raise
            
        for s_name, (s_classname, s_config) in config._services.items():
            s_class = get_class(s_classname)
            if s_class:
                if not analytics_service.DASServiceBase in s_class.__bases__:
                    print "Warning: DASServiceBase not in superclasses of service %s (class %s, bases %s)" % (s_name, s_classname, s_class.__bases__)
                s_config = s_class.merge_defaults(s_config)
                validation = s_class.validate_config(s_config)
                if validation == True:
                    master_pipe, slave_pipe = multiprocessing.Pipe()
                    self.services[s_name] = s_class(s_name, s_config, slave_pipe)
                    self.service_pipes[s_name] = master_pipe
                    self.all_pipes[s_name] = master_pipe
                    
                    if 'subscriptions' in s_config:
                        for sc in s_config['subscriptions']:
                            if isinstance(sc, (tuple, list)) and len(sc)==3:
                                self.subscriptions += [(s_name, sc[0], sc[1], sc[2])]
                            else:
                                print 'Bad subscriptions %s: Should be (match_class,match_src,match_msgtype)' 
                    
                else:
                    print "Validation failed for service %s." % a_name
                    print str(validation)
            else:
                print "Class '%s' for service '%s' not found." \
                % (s_classname, s_name)
                raise
        
    def daemonize(self):
        """
        Daemonize using the method from das_robot 
        only if daemon=True at __init__.
        """
        if self.daemon:
            DAS.core.das_robot.Robot.daemonize(self)
    
    def start(self):
        self.load_modules(self.config)
        DAS.core.das_robot.Robot.start(self)
    
    def status(self):
        """
        Return status information about Robot instance.
        """
        # Get the pid from the pidfile
        try:
            pidf  = file(self.pidfile, 'r')
            pid = int(pidf.read().strip())
            pidf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        print "DAS Analytics Controller information"
        print "PID    :", pid
        print "pidfile:", self.pidfile
    
    def log(self, importance, message):
        """
        Log a message with given importance and text. 
        Will only work if something has subscribed to log-type messages.
        """
        self.master_pipe.send(\
        {'type':'log', 'time':time.time(), 'level':importance,
        'text':message, 'src':'core'})
    
    def run(self):
        """
        Actually run the analysis controller. 
        This starts all the analysers and services, then
        goes into a loop handling any messages sent and 
        checking that the analysers remain alive.
        """
        
        for name, service in self.services.items():
            service.start()
        for name, analyser in self.analysers.items():
            analyser.start()
            
        self.master_pipe, self.all_pipes['controller'] = multiprocessing.Pipe()
        
        self.timers += [{'interval':self.config.heartbeat_interval,
                         'func':self.check_heartbeat}]
        self.timers += [{'interval':10, 'func':self.log,
                         'args':(0, 'still alive')}]
        #general timer interface
        while True:
            t = time.time()
            for name,pipe in self.all_pipes.items():
                while pipe.poll():
                    self.handle_msg(pipe.recv())
            
            if self._stop:
                return
            
            self.check_timers(t)

        # import and instantiate requested modules
        # start modules
        # loop
            # poll for messages (what messages?)
            # check for signals
            # check for updated code
            # check for process exited
            
            time.sleep(self.config.core_interval)
    
    def check_timers(self, t):
        """
        Check any scheduled actions in the list self.timers.
        This a list of dictionaries like 
        {
         'func': function or bound method to call
         'args', 'kwargs': arguments for function
         'next': absolute time to call it (+- system interval)
         'interval': if set, repeat call at this interval. Otherwise only call once.
         }
        """
        for timer in self.timers[:]:
            if timer.get('next', 0) < t:
                timer['func'](*timer.get('args', ()), **timer.get('kwargs', {}))
                if timer.get('interval',0) > 0:
                    timer['next'] = t + timer['interval']
                else:
                    self.timers.remove(timer)
                
    def check_heartbeat(self):
        """
        Check that all our analysers and services are responding 
        to 'ping' signals. Each ping
        has a sequence number, and are generally emitted at 
        ~minute (configurable, 'heartbeat_interval')
        intervals. Emit a warning if the last received message 
        is more than 'heartbeat_warn' sequence number out of date.
        Restart the relevant module if more than 
        'heartbeat_restart' out of date.
        If a module is currently doing work, then this takes 
        control away from the main loop, which handles
        heartbeat, so some latitude needs to be given here 
        if the analysis is CPU intensive.
        """
        for name, (seq, when) in self.last_heartbeat.items():
            if seq < self.current_heartbeat - self.config.heartbeat_warn:
                self.log(1,\
                'expired heartbeat for %s (seq=%s, time=%s)'\
                        % (name, seq, when))
            if seq < self.current_heartbeat - self.config.heartbeat_restart:
                self.log(2, \
                'restarting %s due to expired heartbeat (seq=%s, time=%s)'\
                        % (name, seq, when))
                pass #dummy for the moment
                
        self.current_heartbeat += 1
        for name, pipe in self.analyser_pipes.items():
            pipe.send({'type':'ping', 'seq':self.current_heartbeat})
        
    @analytics_utils.traced        
    def handle_msg(self, msg):
        """
        Handle incoming messages. Make sure the format is good 
        (ie, a dictionary with a 'type' field).
        If so, check if any service has subscribed to this 
        message type, then see if there is a function
        that handles messages of this type. This function uses 
        a fixed list instead of introspection to avoid
        the possibility of badly-typed messages interfering with 
        core operation.
        """
        if isinstance(msg, dict):
            msgtype = msg.get('type', None)
            self.handle_subscriptions(msg)
            
            if msgtype != None:
                func = {
                        'log':lambda msg: None,
                        'ping':self.handle_msg_ping,
                        'broadcast':self.handle_msg_broadcast,
                        'stop':self.handle_msg_stop,
                        'kill':self.handle_msg_kill,
                        }.get(msgtype,None)
                if func == None:
                    func = self.handle_msg_default
                func(msg)
            else:
                self.log(1, 'Invalid message type: %s' % msg)
                
        else:
            self.log(1, 'Invalid pipe msg: %s' % msg)
            
    
    def stop_child(self, name):
        """
        Try and gently stop a child. This emits the 'stop' signal, 
        which should cause it to stop and
        finalise neatly if possible. A timer action is then 
        scheduled for 'stop_time' to kill the child
        if it has failed to stop by this time.
        """
        child = None
        pipe = None
        if name in self.analysers:
            child = self.analysers[name]
            pipe = self.analyser_pipes[name]
        elif name in self.services:
            child = self.services[name]
            pipe = self.service_pipes[name]
        else:
            self.log(1, 'unable to stop non-existent child %s' % name)
            return
        pipe.send({'type':'stop'})
        self.timers += [{'func':self.kill_child, 'args':(name,),
                         'next':time.time()+self.config.stop_time}]
        
        
    def kill_child(self, name):
        """
        Get rid of a child process, politeness be damned. 
        If the process object still exists and is alive,
        send SIGTERM. Then delete the process and pipe objects.
        """
        child = None
        if name in self.analysers:
            child = self.analysers[name]
        elif name in self.services:
            child = self.services[name]
        else:
            self.log(1,'unable to stop non-existent child %s' % name)
            return
        if child:
            if child.is_alive():
                child.terminate()
        if name in self.analysers:
            del self.analysers[name]
            del self.analyser_pipes[name]
            del self.all_pipes[name]
        elif name in self.services:
            del self.services[name]
            del self.service_pipes[name]
            del self.all_pipes[name]
        
    def handle_msg_default(self, msg):
        """
        Handle a message of a type not given in our list of specialist handlers.
        """
        self.log(1,'unhandled message: %s' % msg)
    
    def handle_msg_ping(self, msg):
        """
        Handle reply 'ping' messages, updating the sequence number in 
        the last_heartbeat.
        """
        self.last_heartbeat[msg.get('src', '')] = (msg.get('seq',0), time.time())
        
    def handle_msg_broadcast(self, msg):
        """
        Re-broadcast a message to a specified set of recipients.
        Contents {
                  type: broadcast
                  broadcast_class: all|analyser|service
                  broadcast_match: (wildcard against name)
                  payload: {actual message}
                  }
        """
        broadcast_class = msg.get('broadcast_class', 'all')
        broadcast_match = msg.get('broadcast_match', '*')
        payload = msg.get('payload', {})
        payload['broadcast'] = True
        pipe_dict = {}
        if broadcast_class == 'all':
            pipe_dict = self.all_pipes
        elif broadcast_class == 'analyser':
            pipe_dict = self.analyser_pipes
        elif broadcast_class == 'service':
            pipe_dict = self.service_pipes
        for name, pipe in pipe_dict.items():
            if  broadcast_class == '*' or broadcast_class == '' \
                or fnmatch.fnmatch(name, broadcast_match):
                pipe.send(payload)
    
    def handle_msg_stop(self, msg):
        """
        Try and shut down gracefully, if possible. 
        Emit the stop signal to each child,
        then join each process for up to 'stop_time'. 
        After this method returns, the main loop
        should terminate.
        """
        self._stop = True
        wait = msg.get('stop_time', self.config.stop_time)
        for name in self.analysers:
            self.stop_child(name)
        for name in self.services:
            self.stop_child(name)
        for name, process in self.analysers.items():
            process.join(wait)
        for name, process in self.services.items():
            process.join(wait)
            
    def handle_msg_kill(self, msg):
        """
        Shut down ungracefully. Send SIGTERM to all children, 
        then exit hard.
        """
        for name, process in self.analysers.items():
            process.terminate()
        for name, process in self.services.items():
            process.terminate()
        sys.exit(1)
           
    def handle_subscriptions(self, msg):
        """
        Match messages to subscription rules, 
        using fnmatch (shell-type wildcard matching).
        This might need re-ordering for efficiency.
        """
        src = msg.get('src','')
        msgtype = msg.get('type','')
        for (target, match_class, match_src, match_msg) in self.subscriptions:
            if  match_class == 'all' or \
                (match_class == 'analyser' and src in self.analysers) \
                or (match_class == 'service' and src in self.services):
                if  match_src == '' or match_src == '*' \
                    or fnmatch.fnmatch(src, match_src):
                    if  match_msg == '' or match_msg == '*' \
                        or fnmatch.fnmatch(msgtype, match_msg):
                        msg['subscribed'] = True
                        self.all_pipes[target].send(msg)
            
if __name__ == '__main__':
    import optparse
    parser = optparse.OptionParser()
    parser.add_option('--start', dest='start', action='store_true',
        help='Start Analytics Daemon', default=False)
    parser.add_option('--stop', dest='stop', action='store_true',
        help='Stop Analytics Daemon', default=False)
    parser.add_option('--restart', dest='restart', action='store_true',
        help='Restart Analytics Daemon', default=False)
    parser.add_option('--status', dest='status', action='store_true',
        help='Status of Analytics Daemon', default=False)
    parser.add_option('-f', '--foreground', dest='daemon', action='store_false',
        help='Ward against demonic intervention', default=True)
    parser.add_option('-d', '--das_config', dest='das_config',
        action='store', help='Location of DAS config file', default=None)
    opts, args = parser.parse_args()
    
    if len(args)==1:
        analytics_config = args[0]
    else:
        print "Positional arguments should be '/path/to/analytics_config'"
        raise
    
    if not os.path.exists(analytics_config):
        print "Analytics Config (%s) doesn't appear to exist." \
                % analytics_config
        raise
    
    if opts.das_config == None:
        das_config = DAS.utils.das_config.das_readconfig()
    else:
        if os.path.exists(opts.das_config):
            das_config = \
                DAS.utils.das_config.das_readconfig(opts.das_config)
        else:
            print "DAS Config (%s) doesn't appear to exist." \
                % opts.das_config
            raise
    
    
    DAC = DASAnalyticsController(das_config,
                                 analytics_config,
                                 opts.daemon)
    
    if opts.start:
        DAC.start()
    elif opts.restart:
        DAC.restart()
    elif opts.stop:
        DAC.stop()
    elif opts.status:
        DAC.status()
    else:
        print "Failed to specify an action. This would probably help"
        raise
    
    
    
