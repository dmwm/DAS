from analytics_config import DASAnalyticsConfig
from analytics_scheduler import TaskScheduler
from analytics_web import DASAnalyticsWeb
from analytics_utils import PipeHandler

import logging.handlers
import sys
import os
import random
import time
import multiprocessing

from DAS.core.das_robot import Robot

DASAnalyticsConfig.add_option('log_to_stdout',
                              group="Logging",
                              type=int,
                              default=0,
                              help="Logging level to print to stdout.")
DASAnalyticsConfig.add_option('log_to_stderr',
                              group="Logging",
                              type=int,
                              default=0,
                              help="Logging level to print to stderr.")
DASAnalyticsConfig.add_option('log_to_file',
                              group="Logging",
                              type=int,
                              default=0,
                              help="Logging level to print to logfile.")
DASAnalyticsConfig.add_option('logfile',
                              group="Logging",
                              type=basestring,
                              default='das_analytics.log',
                              help="Name for logfile.")
DASAnalyticsConfig.add_option('logfile_mode',
                              group="Logging",
                              type=basestring,
                              choices=['None','TimeRotate','SizeRotate'],
                              default='None',
                              help="Number of files for rotating log.")
DASAnalyticsConfig.add_option('logfile_rotating_count',
                              group="Logging",
                              type=int,
                              default=0,
                              help="Number of rotating logfiles.")
DASAnalyticsConfig.add_option('logfile_rotating_size',
                              group="Logging",
                              type=int,
                              default=100000,
                              help="Size of rotating logfiles.")
DASAnalyticsConfig.add_option('logfile_rotating_interval',
                              group="Logging",
                              type=int,
                              default=24,
                              help="Interval of rotating logfiles.")
DASAnalyticsConfig.add_option('log_format',
                              group="Logging",
                              type=basestring,
                              default="",
                              help="Logging format to use.")
class DASAnalyticsLogging(object):
    "Helper object that does the necessary logging config."
    def __init__(self, config):
        self.logger = logging.getLogger("DASAnalytics")
        self.logger.setLevel(logging.DEBUG)
        self.handlers = []
        formatter = logging.Formatter(config.log_format)
        if config.log_to_stdout > 0:
            stdout_h = logging.StreamHandler(sys.stdout)
            stdout_h.setLevel(config.log_to_stdout)
            stdout_h.setFormatter(formatter)
            self.handlers.append(stdout_h)
        if config.log_to_stderr > 0:
            stderr_h = logging.StreamHandler(sys.stderr)
            stderr_h.setLevel(config.log_to_stdout)
            stderr_h.setFormatter(formatter)
            self.handlers.append(stderr_h)
        if config.log_to_file > 0:
            if config.logfile_mode == 'TimeRotate':
                file_h = logging.handlers.TimedRotatingFileHandler(
                                    filename=config.logfile,
                                    when='H',
                                    interval=config.logfile_rotating_interval,
                                    backupCount=config.logfile_rotating_count)
            elif config.logfile_mode == 'SizeRotate':
                file_h = logging.handlers.RotatingFileHandler(
                                    filename=config.logfile,
                                    maxBytes=config.logfile_rotating_size,
                                    backupCount=config.logfile_rotating_count)
            else:
                file_h = logging.FileHandler(config.logfile)
            file_h.setLevel(config.log_to_file)
            file_h.setFormatter(formatter)
            self.handlers.append(file_h)
        for handler in self.handlers:
            self.logger.addHandler(handler)
    def get_logger(self):
        "Get the root logger"
        return self.logger


DASAnalyticsConfig.add_option("detach",
                              short="d",
                              type=bool,
                              group="Daemon",
                              default=False,
                              help="Daemonize the controller.")
DASAnalyticsConfig.add_option("detach_stdout",
                              type=bool,
                              group="Daemon",
                              default=True,
                              help="Detach stdout on daemonization.")
DASAnalyticsConfig.add_option("detach_stderr",
                              type=bool,
                              group="Daemon",
                              default=True,
                              help="Detach stderr on daemonization.")
DASAnalyticsConfig.add_option("pidfile",
                              type=basestring,
                              group="Daemon",
                              default="analytics.pid",
                              help="File to store the PID in.")
DASAnalyticsConfig.add_option("start",
                              type=bool,
                              group="Daemon",
                              default=False,
                              help="Wake up and smell the ashes.")
DASAnalyticsConfig.add_option("restart",
                              type=bool,
                              group="Daemon",
                              default=False,
                              help="Have you tried turning it off and on again?")
DASAnalyticsConfig.add_option("stop",
                              type=bool,
                              group="Daemon",
                              default=False,
                              help="SCRAM the pile.")
DASAnalyticsConfig.add_option("status",
                              type=bool,
                              group="Daemon",
                              default=False,
                              help="SNAFU.")
DASAnalyticsConfig.add_option("no_start_offset",
                              type=bool,
                              default=False,
                              help="Prevent jobs being randomly offset by up to their interval.")
DASAnalyticsConfig.add_option("web",
                              type=bool,
                              default=False,
                              help="Enable webserver.")
class DASAnalyticsController(Robot):
    def __init__(self):
        self.config = DASAnalyticsConfig()
        self.config.configure()
        
        self.pidfile = self.config.pidfile
        
        self.stdin = os.devnull
        self.stdout = os.devnull if self.config.detach_stdout else sys.stdout
        self.stderr = os.devnull if self.config.detach_stderr else sys.stderr
        
        self.web = self.config.web
        self.pipe = None
        
        operations = sum([self.config.start, 
                          self.config.restart, 
                          self.config.stop, 
                          self.config.status]) #bool is int, ish
        if operations == 1:
            if self.config.start:
                self.start()
            if self.config.restart:
                self.restart()
            if self.config.stop:
                self.stop()
            if self.config.status:
                self.status()
        
        else:
            print "Require exactly one of"
            print "--start --stop --restart --status"
    
    def daemonize(self):
        "Only detach if the daemonize option if set"
        if self.config.detach:
            Robot.daemonize(self)    
        
    def status(self):
        "Brief info (override from robot which does specific things here)"
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

        print "Analytics info: running"
        print "PID    :", pid
        print "pidfile:", self.pidfile
       
    def result_callback(self, result):
        "Callback to send job results to the webserver pipe"
        self.pipe.send(('result', result))
    
    def run(self):
        "Overriden robot method. Actually do something."
        
        start_time = time.time()
        
        logconf = DASAnalyticsLogging(self.config)
        logger = logconf.get_logger()
        
        scheduler = TaskScheduler(self.config)
        
        if self.web:
            self.pipe, remote = multiprocessing.Pipe(True)
            logger.addHandler(PipeHandler(self.pipe))
            scheduler.add_callback(self.result_callback)
            web = DASAnalyticsWeb(self.config, remote)
            web.start()
        
        tasks = self.config.get_tasks()
        logger.info("Scheduling %s tasks." % len(tasks))
        for task in self.config.get_tasks():
            if self.config.no_start_offset:
                scheduler.add_task(task, offset=0)
            else:
                scheduler.add_task(task, offset=random.random()*task.interval)
        
        counter = 0
        while True:
            
            # scheduler used to run in a separate thread
            # but since we need to have a loop here, why do so?
            scheduler.run()
            
            # this lot should probably go in a separate function
            # in which case objects like scheduler need to be moved to self
            if self.web:
                if counter % 5 == 0:
                    self.pipe.send(('tasks', scheduler.get_tasks()))
                    self.pipe.send(('info', {'uptime':time.time()-start_time}))
                while self.pipe.poll():
                    msg = self.pipe.recv()
                    if isinstance(msg, tuple) and len(msg) == 2:
                        msgtype, payload = msg
                        if msgtype == "shutdown":
                            return
                        if msgtype == "stop":
                            self.stop()
                        if msgtype == "restart":
                            self.restart()
                        
            counter += 1
            time.sleep(1)
    
if __name__ == '__main__':
    controller = DASAnalyticsController()