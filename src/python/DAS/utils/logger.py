#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0301,C0103

"""
General purpose DAS logger class
"""

__revision__ = "$Id: logger.py,v 1.12 2010/04/14 20:29:59 valya Exp $"
__version__ = "$Revision: 1.12 $"
__author__ = "Valentin Kuznetsov"

import os
import logging
import logging.handlers
import cherrypy

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
    def __init__(self, logfile=None, verbose=0, name='DAS',
                 format='%(name)s %(levelname)s %(message)s'):
        self.verbose  = int(verbose)
        self.logfile  = logfile
        self.name     = name
        self.logger   = logging.getLogger(self.name)
        self.loglevel = logging.INFO
        self.addr     = repr(self).split()[-1]
        self.logname  = name
        if  logfile:
            self.dir, _  = os.path.split(logfile)
            self.logname = logfile
            try:
                if  not os.path.isdir(self.dir):
                    os.makedirs(self.dir)
                if  not os.path.isfile(self.logname):
                    fds = open(self.logname, 'a')
                    fds.close()
            except:
                msg = "Not enough permissions to create %s"\
                       % self.logfile 
                raise Exception(msg)
#            logging.basicConfig(filename=logfile, level=self.loglevel,
#                        format=format)
#        else:
#            logging.basicConfig(level=self.loglevel, format=format)
#        self.level(verbose)

        if  self.logfile:
            hdlr = logging.handlers.TimedRotatingFileHandler( \
                      self.logname, 'midnight', 1, 7 )
        else:
            hdlr = logging.StreamHandler()
        formatter = logging.Formatter(format)
#        formatter = logging.Formatter( \
#                  '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
        hdlr.setFormatter( formatter )
        self.logger.addHandler(hdlr)
        self.level(verbose)
        self.handler = hdlr

    def level(self, level):
        """
        Set logging level
        """
        self.verbose = level
        if  level == 1:
            self.loglevel = logging.INFO
        elif level == 2:
            self.loglevel = logging.DEBUG
        elif level >= 2:
            self.loglevel = logging.NOTSET
        else:
            self.loglevel = logging.ERROR
        self.logger.setLevel(self.loglevel)

    def error(self, msg='N/A'):
        """
        Write given message to the logger at error logging level
        """
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.error(msg)

    def info(self, msg='N/A'):
        """
        Write given message to the logger at info logging level
        """
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.info(msg)

    def debug(self, msg='N/A'):
        """
        Write given message to the logger at debug logging level
        """
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.debug(msg)

    def warning(self, msg='N/A'):
        """
        Write given message to the logger at warning logging level
        """
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.warn(msg)

    def exception(self, msg='N/A'):
        """
        Write given message to the logger at exception logging level
        """
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.error(msg)

    def critical(self, msg='N/A'):
        """
        Write given message to the logger at critical logging level
        """
        msg = str(msg)
        msg = self.addr + ' ' + msg
        self.logger.critical(msg)

def set_cherrypy_logger(hdlr, level):
    """set up logging for CherryPy"""
    logging.getLogger('cherrypy.error').setLevel(level)
    logging.getLogger('cherrypy.access').setLevel(level)

    logging.getLogger('cherrypy.error').addHandler(hdlr)
    logging.getLogger('cherrypy.access').addHandler(hdlr)

#    log = cherrypy.log
#    log.error_log.addHandler(hdlr)
#    log.access_log.addHandler(hdlr)
#    hdlr.setLevel(level)
#    log.screen = False # disable printout of cherrypy msg's on a screen
    # change cherrypy logger time() method to return empty string
    # since my handler already have timestamp in it so instead of having
    # 2010-05-26 14:11:34,873 - cherrypy.error - INFO - [26/May/2010:14:11:34] ENGINE
    # we get
    # 2010-05-26 14:11:34,873 - cherrypy.error - INFO -  ENGINE
#    log.time = empty_str 
#    log.access_log_format = \
#        '%(h)s %(l)s %(u)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

def empty_str():
    """Return empty string"""
    return ('')

