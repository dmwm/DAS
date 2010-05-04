#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0301,C0103

"""
General purpose DAS logger class
"""

__revision__ = "$Id: logger.py,v 1.7 2009/12/07 20:53:03 valya Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "Valentin Kuznetsov"

import os
import logging
import logging.handlers

class DummyLogger:
    """
    Base logger class
    """
    def error(self, msg):
        """
        logger error method
        """
        pass

    def info(self, msg):
        """
        logger info method
        """
        pass

    def debug(self, msg):
        """
        logger debug method
        """
        pass

    def warning(self, msg):
        """
        logger warning method
        """
        pass

    def exception(self, msg):
        """
        logger expception method
        """
        pass

    def critical(self, msg):
        """
        logger critical method
        """
        pass

class DASLogger:
    """
    DAS base logger class
    """
    def __init__(self, idir='/tmp', name="DAS", verbose=0, stdout=0):
        self.verbose  = verbose
        self.name     = name
        self.dir      = idir
        self.stdout   = stdout
        self.logger   = logging.getLogger(self.name)
        self.loglevel = logging.INFO
        self.logname  = os.path.join(self.dir, '%s.log' % name) 
        self.addr     = repr(self).split()[-1]
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
#        logging.getLogger('').addHandler(hdlr)

        self.logger.addHandler(hdlr)
        self.level(verbose)

        # redirect SQLAlchemy/CherryPy output to our logger
        set_sqlalchemy_logger(hdlr, self.verbose)
        set_cherrypy_logger(hdlr, self.verbose)

    def level(self, level):
        """
        Set logging level
        """
        self.verbose = level
        if  level == 1:
            self.loglevel = logging.INFO
        elif level >= 2:
            self.loglevel = logging.DEBUG
        else:
            self.loglevel = logging.NOTSET
        self.logger.setLevel(self.loglevel)

    def error(self, msg):
        """
        Write given message to the logger at error logging level
        """
        msg = self.addr + ' ' + msg
        self.logger.error(msg)
        if  self.stdout:
            print 'ERROR ', msg

    def info(self, msg):
        """
        Write given message to the logger at info logging level
        """
        msg = self.addr + ' ' + msg
        self.logger.info(msg)
        if  self.stdout:
            print 'INFO  ', msg

    def debug(self, msg):
        """
        Write given message to the logger at debug logging level
        """
        msg = self.addr + ' ' + msg
        self.logger.debug(msg)
        if  self.stdout and self.verbose > 1:
            print 'DEBUG ', msg

    def warning(self, msg):
        """
        Write given message to the logger at warning logging level
        """
        msg = self.addr + ' ' + msg
        self.logger.warn(msg)

    def exception(self, msg):
        """
        Write given message to the logger at exception logging level
        """
        msg = self.addr + ' ' + msg
        self.logger.error(msg)
        if  self.stdout:
            print 'EXCPT ', msg

    def critical(self, msg):
        """
        Write given message to the logger at critical logging level
        """
        msg = self.addr + ' ' + msg
        self.logger.critical(msg)

def set_sqlalchemy_logger(hdlr, level):
    """set up logging for SQLAlchemy"""
    # we will only keep engine output, which prints out queries
    # the orm are irrelevant, and pool requires additional timeout
    # to be closed
    logging.getLogger('sqlalchemy.engine').setLevel(level)
    logging.getLogger('sqlalchemy.orm').setLevel(logging.NOTSET)
    logging.getLogger('sqlalchemy.pool').setLevel(logging.NOTSET)

    logging.getLogger('sqlalchemy.engine').addHandler(hdlr)
    logging.getLogger('sqlalchemy.orm').addHandler(hdlr)
    logging.getLogger('sqlalchemy.pool').addHandler(hdlr)

def set_cherrypy_logger(hdlr, level):
    """set up logging for CherryPy"""
    logging.getLogger('cherrypy.error').setLevel(level)
    logging.getLogger('cherrypy.access').setLevel(level)

    logging.getLogger('cherrypy.error').addHandler(hdlr)
    logging.getLogger('cherrypy.access').addHandler(hdlr)

