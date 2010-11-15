#!/usr/bin/env python

from DAS.analytics.analytics_config import DASAnalyticsConfig
from DAS.analytics.analytics_scheduler import TaskScheduler
from DAS.analytics.analytics_web import DASAnalyticsWeb
from DAS.analytics.analytics_utils import multilogging, das_factory, set_global_das

import logging.handlers
import sys
import os
import random
import time
import threading

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
                              default='/tmp/das_analytics.log',
                              help="Name for logfile.")
DASAnalyticsConfig.add_option('logfile_mode',
                              group="Logging",
                              type=basestring,
                              choices=['None','TimeRotate','SizeRotate'],
                              default='TimeRotate',
                              help="Mode for rotating logs, if any.")
DASAnalyticsConfig.add_option('logfile_rotating_count',
                              group="Logging",
                              type=int,
                              default=5,
                              help="Number of rotating logfiles.")
DASAnalyticsConfig.add_option('logfile_rotating_size',
                              group="Logging",
                              type=int,
                              default=1000000,
                              help="Size of rotating logfiles.")
DASAnalyticsConfig.add_option('logfile_rotating_interval',
                              group="Logging",
                              type=int,
                              default=24,
                              help="Interval of rotating logfiles.")
DASAnalyticsConfig.add_option('log_format',
                              group="Logging",
                              type=basestring,
                              default="%(asctime)s:%(name)s:%(levelname)s - %(message)s",
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


DASAnalyticsConfig.add_option("no_start_offset",
                              type=bool,
                              default=False,
                              help="Prevent jobs being randomly offset by up to their interval.")
DASAnalyticsConfig.add_option("web",
                              type=bool,
                              default=False,
                              help="Enable webserver.")
DASAnalyticsConfig.add_option("global_das",
                              type=bool,
                              default=False,
                              help="Use a single DAS instance.")
class DASAnalyticsController:
    def __init__(self):
        self.config = None
        self.logger = None
        self.scheduler = None
        self.webserver = None
        
        self.finish = False
        
        self.run()
    
    def run(self):
        "Actually do something."
        
        multilogging()
        
        self.config = DASAnalyticsConfig()
        self.config.configure()
        
        logconf = DASAnalyticsLogging(self.config)
        self.logger = logconf.get_logger()
        self.logger.info("Analytics server starting.")
        self.logger.info("Configuration: %s", self.config._options)
        
        if self.config.global_das:
            set_global_das(True)
            das_factory("DASAnalytics.DAS")
        else:
            set_global_das(False)
        
        self.scheduler = TaskScheduler(self, self.config)
        
        if self.config.web:
            self.webserver = DASAnalyticsWeb(self.config, self)
            webthread = threading.Thread(target=self.webserver.start)
            webthread.daemon = True
            webthread.start()
        
        tasks = self.config.get_tasks()
        self.logger.info("Scheduling %s tasks." % len(tasks))
        for task in self.config.get_tasks():
            if self.config.no_start_offset:
                self.scheduler.add_task(task, offset=0)
            else:
                self.scheduler.add_task(task, offset=random.random()*task.interval)
        
        while True:
            
            if self.finish:
                return
            
            self.scheduler.run()
            
            time.sleep(1)
            
    def stop(self):
        """
        Stop the main loop, after which all the daemon threads should die.
        """
        self.finish = True
            
    
if __name__ == '__main__':
    controller = DASAnalyticsController()