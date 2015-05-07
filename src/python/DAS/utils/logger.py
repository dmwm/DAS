#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=C0301,C0103

"""
General purpose DAS logger class. 
PrintManager class is based on the following work
http://stackoverflow.com/questions/245304/how-do-i-get-the-name-of-a-function-or-method-from-within-a-python-function-or-m
http://stackoverflow.com/questions/251464/how-to-get-the-function-name-as-string-in-python
"""
from __future__ import print_function

__author__ = "Valentin Kuznetsov"

import logging
import logging.handlers
import inspect
import functools

def funcname():
    """Extract caller name from a stack"""
    return inspect.stack()[1][3]

def print_msg(msg, cls, prefix=''):
    """
    Print message in a form cls::caller msg, suitable for class usage
    """
    print("%s %s:%s %s" % (prefix, cls, inspect.stack()[2][3], msg))

class PrintManager(object):
    """PrintManager class"""
    def __init__(self, name='function', verbose=0):
        super(PrintManager, self).__init__()
        self.name      = name
        if  verbose == None or verbose == False:
            verbose = 0
        if  verbose == True:
            verbose = 1
        self.verbose   = verbose
        self.infolog   = \
        functools.partial(print_msg, cls=self.name, prefix='INFO')
        self.debuglog  = \
        functools.partial(print_msg, cls=self.name, prefix='DEBUG')
        self.warnlog   = \
        functools.partial(print_msg, cls=self.name, prefix='WARNING')
        self.errlog    = \
        functools.partial(print_msg, cls=self.name, prefix='ERROR')

    def info(self, msg):
        """print info messages"""
        msg = str(msg)
        if  self.verbose:
            self.infolog(msg)

    def debug(self, msg):
        """print debug messages"""
        msg = str(msg)
        if  self.verbose > 1:
            self.debuglog(msg)

    def warning(self, msg):
        """print warning messages"""
        msg = str(msg)
        self.warnlog(msg)

    def error(self, msg):
        """print warning messages"""
        msg = str(msg)
        self.errlog(msg)

class NullHandler(logging.Handler):
    """Do nothing logger"""
    def emit(self, record):
        "This method does nothing."
        pass
    def handle(self, record):
        "This method does nothing."
        pass

def set_cherrypy_logger(hdlr, level):
    """set up logging for CherryPy"""
    logging.getLogger('cherrypy.error').setLevel(level)
    logging.getLogger('cherrypy.access').setLevel(level)

    logging.getLogger('cherrypy.error').addHandler(hdlr)
    logging.getLogger('cherrypy.access').addHandler(hdlr)
