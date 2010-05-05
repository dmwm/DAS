#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0301,C0103

"""
General purpose DAS logger class
"""

__revision__ = "$Id: logger.py,v 1.11 2010/04/14 17:49:34 valya Exp $"
__version__ = "$Revision: 1.11 $"
__author__ = "Valentin Kuznetsov"

import os
import logging
import logging.handlers

class DummyLogger(object):
    """
    Base logger class
    """
    def __init__(self):
        pass

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

class DASLogger(object):
    """
    DAS base logger class
    """
    def __init__(self, idir='/tmp', name="DAS", verbose=0):
        self.verbose  = verbose
        self.name     = name
        self.dir      = idir
        self.logger   = logging.getLogger(self.name)
        self.loglevel = logging.INFO
        self.addr     = repr(self).split()[-1]
        logging.basicConfig()
        self.level(verbose)

#        self.logname  = os.path.join(self.dir, '%s.log' % name) 
#        try:
#            if  not os.path.isdir(self.dir):
#                os.makedirs(self.dir)
#            if  not os.path.isfile(self.logname):
#                f = open(self.logname, 'a')
#                f.close()
#        except:
#            msg = "Not enough permissions to create a DAS log file in '%s'"\
#                   % self.dir
#            raise msg
#        hdlr = logging.handlers.TimedRotatingFileHandler( \
#                  self.logname, 'midnight', 1, 7 )
#        hdlr = logging.StreamHandler()
#        formatter = logging.Formatter( \
#                  '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
#        hdlr.setFormatter( formatter )
#        self.logger.addHandler(hdlr)
#        self.level(verbose)
#        set_cherrypy_logger(hdlr, self.verbose)

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
        if  not msg:
            msg = "No message"
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.error(msg)

    def info(self, msg):
        """
        Write given message to the logger at info logging level
        """
        if  not msg:
            msg = "No message"
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.info(msg)

    def debug(self, msg):
        """
        Write given message to the logger at debug logging level
        """
        if  not msg:
            msg = "No message"
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.debug(msg)

    def warning(self, msg):
        """
        Write given message to the logger at warning logging level
        """
        if  not msg:
            msg = "No message"
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.warn(msg)

    def exception(self, msg):
        """
        Write given message to the logger at exception logging level
        """
        if  not msg:
            msg = "No message"
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.error(msg)

    def critical(self, msg):
        """
        Write given message to the logger at critical logging level
        """
        if  not msg:
            msg = "No message"
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.critical(msg)

def set_cherrypy_logger(hdlr, level):
    """set up logging for CherryPy"""
    logging.getLogger('cherrypy.error').setLevel(level)
    logging.getLogger('cherrypy.access').setLevel(level)

    logging.getLogger('cherrypy.error').addHandler(hdlr)
    logging.getLogger('cherrypy.access').addHandler(hdlr)

