#!/usr/bin/env python

from DAS.analytics.config import DASAnalyticsConfig
from DAS.analytics.scheduler import TaskScheduler
from DAS.analytics.web import AnalyticsWeb
from DAS.analytics.results import ResultManager
from DAS.analytics.utils import multilogging
import cherrypy
import re

import logging.handlers
import sys
import random

RE_TIMEROTATE = re.compile(r"^(\d+(?:\.\d*))\s*([hd])")
RE_SIZEROTATE = re.compile(r"^(\d+(?:\.\d*))\s*([km])")

DASAnalyticsConfig.add_option('verbose', group='Logging', type=bool,
      default=False, help="Verbose logging", short='v')
DASAnalyticsConfig.add_option('log_to_stdout', group="Logging",
      type=bool, default=False, help="Log to STDOUT")
DASAnalyticsConfig.add_option('log_to_stderr', group="Logging", type=bool,
      default=False, help="Log to STDERR")
DASAnalyticsConfig.add_option('log_to_file', group="Logging", type=bool,
      default=False, help="Log to file.")
DASAnalyticsConfig.add_option('logfile', group="Logging", type=basestring,
      default='/tmp/das_analytics.log',
      help="Name for logfile.")
DASAnalyticsConfig.add_option('logfile_mode', group="Logging", type=basestring,
      default=None,
      help="Mode for rotating logs, if any. \
Numbers postfixed 'h' or 'd' indicate timed rotation, 'k' or 'm' size rotation.")
DASAnalyticsConfig.add_option('logfile_rotates', group="Logging", type=int,
      default=5, help="Number of rotating logfiles.")
DASAnalyticsConfig.add_option('log_format', group="Logging", type=basestring,
      default="%(asctime)s:%(name)s:%(levelname)s - %(message)s",
      help="Logging format to use.")
DASAnalyticsConfig.add_option("no_start_offset", type=bool, default=False,
      help="Prevent jobs being randomly offset by up to their interval.")
DASAnalyticsConfig.add_option('pid', type=str, default="/tmp/das_analytics.pid",
      help="Location and name of web server PID file, e.g. /tmp/pid.txt")

class DASAnalyticsLogging(object):
    "Helper object that does the necessary logging config."
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("DASAnalytics")
        self.formatter = logging.Formatter(self.config.log_format)
        if self.config.verbose:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
        
        if self.config.log_to_stderr:
            self.add_handler(logging.StreamHandler(sys.stderr))
        elif self.config.log_to_stdout:
            self.add_handler(logging.StreamHandler(sys.stdout))
        if self.config.log_to_file:
            self.configure_file()
        
    def configure_file(self):
        match_time = RE_TIMEROTATE.match(self.config.logfile_mode.lower())
        match_size = RE_SIZEROTATE.match(self.config.logfile_mode.lower())
        if match_time:
            handler = logging.handlers.TimedRotatingFileHandler(
                                        filename=self.config.logfile,
                                        when=match_time.group(2),
                                        interval=float(match_time.group(1)),
                                        backupCount=self.config.logfile_rotates)
        elif match_size:
            size = float(match_size.group(1))
            if match_size.group(2) == 'm':
                size *= 1024*1024
            elif match_size.group(2) == 'k':
                size *= 1024
            handler = logging.handlers.RotatingFileHandler(
                                        filename=self.config.logfile,
                                        maxBytes=size,
                                        backupCount=self.config.logfile_rotates)
        else:
            handler = logging.FileHandler(self.config.logfile)
        self.add_handler(handler)
        
    def add_handler(self, handler):
        if self.config.verbose:
            handler.setLevel(logging.DEBUG)
        else:
            handler.setLevel(logging.INFO)
        handler.setFormatter(self.formatter)
        self.logger.addHandler(handler)

def controller():
    """
    Connect all the components together and press the big red button.
    """
    multilogging()
    config = DASAnalyticsConfig()
    config.configure()
    logconf = DASAnalyticsLogging(config)

    scheduler = TaskScheduler(config, cherrypy.engine)
    scheduler.subscribe()
    results = ResultManager(config)
    web = AnalyticsWeb(config, scheduler, results)
    logconf.logger.info("Analytics starting")
    logconf.add_handler(results)
    scheduler.add_callback(results.receive_task_result)
    
    if config.get_tasks():
        logconf.logger.info("Adding %d tasks", len(config.get_tasks()))
        for task in config.get_tasks():
            if config.no_start_offset:
                scheduler.add_task(task, offset=0)
            else:
                scheduler.add_task(task, offset=random.random()*task.interval)
    
    cherrypy.config["engine.autoreload_on"] = False
    cherrypy.config["server.socket_port"] = config.web_port
        
    cherrypy.tree.mount(web, config.web_base)
        
    pid = cherrypy.process.plugins.PIDFile(cherrypy.engine, config.pid)
    pid.subscribe()
    
    logconf.logger.info("Starting cherrypy")
    cherrypy.engine.start()
    cherrypy.engine.block()
    logconf.logger.info("Stopping cherrypy")

if __name__ == '__main__':
    controller()
