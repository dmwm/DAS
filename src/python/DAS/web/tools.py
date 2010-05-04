#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Web tools.
"""

__license__ = "GPL"
__revision__ = "$Id: tools.py,v 1.2 2010/02/18 15:08:14 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"
__email__ = "vkuznet@gmail.com"

# system modules
import os
import types
import logging
import plistlib

from datetime import datetime, timedelta
from time import mktime
from wsgiref.handlers import format_date_time

# cherrypy modules
import cherrypy
from cherrypy import log as cplog
from cherrypy import request

# cheetag modules
from Cheetah.Template import Template
from Cheetah import Version

try:
    from json import JSONEncoder
except:
    # Prior python 2.6 json comes from simplejson
    from simplejson import JSONEncoder

class Page(object):
    """
    __Page__

    Page is a base class that holds a configuration
    """
    def __init__(self):
        self.name = "Page"

    def warning(self, msg):
        """Define warning log"""
        if  msg:
            self.log(msg, logging.WARNING)

    def exception(self, msg):
        """Define exception log"""
        if  msg:
            self.log(msg, logging.ERROR)

    def debug(self, msg):
        """Define debug log"""
        if  msg:
            self.log(msg, logging.DEBUG)

    def info(self, msg):
        """Define info log"""
        if  msg:
            self.log(msg, logging.INFO)

    def log(self, msg, severity):
        """Define log level"""
        if type(msg) != str:
            msg = str(msg)
        if  msg:
            cplog(msg, context=self.name,
                    severity=severity, traceback=False)

class TemplatedPage(Page):
    """
    TemplatedPage is a class that provides simple Cheetah templating
    """
    def __init__(self, config):
        Page.__init__(self)
        templatedir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'templates')
        if  not os.path.isdir(templatedir):
            templatedir  = os.environ['DAS_ROOT'] + '/src/templates'
        self.templatedir = config.get('templatedir', templatedir)
        self.name = "TemplatedPage"
        self.debug("Templates are located in: %s" % self.templatedir)
        self.debug("Using Cheetah version: %s" % Version)

    def templatepage(self, file=None, *args, **kwargs):
        """
        Template page method.
        """
        searchList = []
        if len(args) > 0:
            searchList.append(args)
        if len(kwargs) > 0:
            searchList.append(kwargs)
        templatefile = "%s/%s.tmpl" % (self.templatedir, file)
        if os.path.exists(templatefile):
            template = Template(file=templatefile, searchList=searchList)
            return template.respond()
        else:
            self.warning("%s not found at %s" % (file, self.templatedir))
            return "Template %s not known" % file

def exposexml (func):
    """CherryPy expose XML decorator"""
    def wrapper (self, *args, **kwds):
        data = func (self, *args, **kwds)
        if  type(data) is types.ListType:
            results = data
        else:
            results = [data]
        cherrypy.response.headers['Content-Type'] = "application/xml"
        return self.templatepage('das_xml', resultlist = results)
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposeplist (func):
    """
    Return data in XML plist format, 
    see http://docs.python.org/library/plistlib.html#module-plistlib
    """
    def wrapper (self, *args, **kwds):
        data_struct = func(self, *args, **kwds)
        plist_str = plistlib.writePlistToString(data_struct)
        cherrypy.response.headers['Content-Type'] = "application/xml+plist"
        return plist_str
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposetext (func):
    """CherryPy expose Text decorator"""
    def wrapper (self, *args, **kwds):
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/plain"
        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposejson (func):
    """CherryPy expose JSON decorator"""
    def wrapper (self, *args, **kwds):
        encoder = JSONEncoder()
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "application/json"
        try:
            jsondata = encoder.encode(data)
            return jsondata
        except:
            Exception("Fail to jsontify obj '%s' type '%s'" % (data, type(data)))
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposejs (func):
    """CherryPy expose JavaScript decorator"""
    def wrapper (self, *args, **kwds):
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "application/javascript"
        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposecss (func):
    """CherryPy expose CSS decorator"""
    def wrapper (self, *args, **kwds):
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/css"
        return data
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def make_timestamp(seconds=0):
    """Create timestamp"""
    then = datetime.now() + timedelta(seconds=seconds)
    return mktime(then.timetuple())

def make_rfc_timestamp(seconds=0):
    """Create RFC timestamp"""
    return format_date_time(make_timestamp(seconds))

def exposedasjson (func):
    """
    This will prepend the DAS header to the data and calculate the checksum of
    the data to set the etag correctly

    TODO: pass in the call_time value, can we get this in a smart/neat way?
    TODO: include the request_version in the data hash - a new version should
    result in an update in a cache
    TODO: "inherit" from the exposejson
    """
    def wrapper (self, *args, **kwds):
        encoder = JSONEncoder()
#        data = runDas(self, func, *args, **kwds)
        data = func(self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "application/json"
        try:
            jsondata = encoder.encode(data)
            return jsondata
        except:
            Exception("Failed to json-ify obj '%s' type '%s'" % (data, type(data)))

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

def exposedasplist (func):
    """
    Return data in XML plist format, 
    see http://docs.python.org/library/plistlib.html#module-plistlib
    """
    def wrapper (self, *args, **kwds):
#        data_struct = runDas(self, func, *args, **kwds)
        data_struct = func(self, *args, **kwds)
        plist_str = plistlib.writePlistToString(data_struct)
        cherrypy.response.headers['ETag'] = data_struct['results'].__str__().__hash__()
        cherrypy.response.headers['Content-Type'] = "application/xml"
        return plist_str
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    wrapper.exposed = True
    return wrapper

