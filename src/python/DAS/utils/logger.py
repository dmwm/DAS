#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0301,C0103

"""
General purpose DAS logger class
"""

__revision__ = "$Id: logger.py,v 1.3 2009/05/28 18:59:11 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

import os
import logging
import logging.handlers

class DASLogger:
    """
    DAS base logger class
    """
    def __init__(self, idir='/tmp', name="DAS", verbose=0, stdout=0):
        self.verbose = verbose
        self.name = name
        self.dir = idir
        self.stdout = stdout
        self.logger = logging.getLogger(self.name)
        self.loglevel = logging.INFO
        self.logname = os.path.join(self.dir, '%s.log' % name) 
        try:
            if  not os.path.isdir(self.dir):
                os.makedirs(self.dir)
            # check if we can create log file over there
            if  not os.path.isfile(self.logname):
                f = open(self.logname, 'a')
                f.close()
        except:
            msg = "Not enough permissions to create a DAS log file in '%s'"\
                   % self.dir
            raise msg
        hdlr = logging.handlers.TimedRotatingFileHandler( \
                  self.logname, 'midnight', 1, 7 )
        formatter = logging.Formatter( \
                  '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
        hdlr.setFormatter( formatter )
        self.logger.addHandler(hdlr)
        self.level(verbose)

    def level(self, level):
        """
        Set logging level
        """
        self.verbose = level
        if  level == 1:
            self.loglevel = logging.INFO
        elif level == 2:
            self.loglevel = logging.DEBUG
        else:
            self.loglevel = logging.NOTSET
        self.logger.setLevel(self.loglevel)

    def error(self, msg):
        """
        Write given message to the logger at error logging level
        """
        self.logger.error(msg)
        if  self.stdout:
            print '### ERROR ###', msg

    def info(self, msg):
        """
        Write given message to the logger at info logging level
        """
        self.logger.info(msg)
#        if  self.stdout:
#            print '### INFO ###', msg

    def debug(self, msg):
        """
        Write given message to the logger at debug logging level
        """
        self.logger.debug(msg)
#        if  self.stdout and self.verbose > 1:
#            print '### DEBUG ###', msg

    def warning(self, msg):
        """
        Write given message to the logger at warning logging level
        """
        self.logger.warn(msg)
#        if  self.stdout:
#            print '### WARNING ###', msg

    def exception(self, msg):
        """
        Write given message to the logger at exception logging level
        """
        self.logger.error(msg)
#        if  self.stdout:
#            print '### EXCEPTION ###', msg

    def critical(self, msg):
        """
        Write given message to the logger at critical logging level
        """
        self.logger.critical(msg)
#        if  self.stdout:
#            print '### CRITICAL ###', msg
