#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Logger module
"""

__revision__ = "$Id: logger.py,v 1.2 2010/04/06 20:34:39 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Gordon Ball"

import DAS.analytics.analytics_service
import time

class Logger(DAS.analytics.analytics_service.DASServiceBase):
    """Logger class"""
    defaults = {
                'interval':1,
                'output':'/tmp/das_analytics_log',
                'subscriptions':[('all', '*', 'log')],
                'flush_frequency':5
                }
    levels = {
              0: 'INFO',
              1: 'ERROR',
              2: 'CRITICAL'
              }
    def configure(self, config):
        """Configure the logger"""
        self.output = open(config['output'], 'w')
        self.flush_frequency = config['flush_frequency']
        self.flush_count = 0
        
    def pipe_log(self, msg):
        """Pipe log"""
        print 'pipe_log'
        src = msg.get('src', '')
        level = msg.get('level', 0)
        tstamp = msg.get('time', 0)
        text = msg.get('text', '')
        self.output.write('[%s] %s: %s: %s\n' % \
                (time.strftime('%d-%m-%Y %H:%M:%S', time.gmtime(tstamp)),
                        src, self.levels.get(level, 'OTHER'), text))
        self.flush_count += 1
        if self.flush_count % self.flush_frequency:
            self.output.flush()
        
    def finalise(self):
        """Close the output"""
        self.output.close()
        
    
        
    
