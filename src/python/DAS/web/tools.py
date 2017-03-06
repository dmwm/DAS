#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Web tools.
"""

__license__ = "GPL"
__revision__ = "$Id: tools.py,v 1.5 2010/04/07 18:19:31 valya Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "Valentin Kuznetsov"
__email__ = "vkuznet@gmail.com"

# system modules
import os
import sys
import types
import logging

from datetime import datetime, timedelta
from time import mktime
from wsgiref.handlers import format_date_time

# cherrypy modules
import cherrypy
from cherrypy import log as cplog
from cherrypy import expose

try:
    from Cheetah.Template import Template
except:
    pass
try:
    import jinja2
except:
    pass

import DAS.utils.jsonwrapper as json
from DAS.web.utils import quote
from DAS.utils.url_utils import url_extend_params, url_extend_params_as_dict
from json import JSONEncoder

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
        templatedir  = os.environ.get('DAS_TMPLPATH', '')
        if  not templatedir or not os.path.isdir(templatedir):
            templatedir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'jinja_templates')
        self.templatedir = config.get('templatedir', templatedir)
        self.name = "TemplatedPage"
        verbose = config.get('verbose', 0)
        self.jinja = 'jinja2' in sys.modules
        if  self.jinja:
            templates = 'JINJA'
        else:
            templates = 'Cheetah'
        self.log("### DAS uses %s templates" % templates, logging.INFO)
        self.log("Templates are located in: %s" % self.templatedir, logging.INFO)

    def templatepage(self, ifile=None, *args, **kwargs):
        """Choose template page handler based on templates engine"""
        if  self.jinja and self.templatedir.find("jinja") != -1:
            return self.templatepage_jinja(ifile, *args, **kwargs)
        return self.templatepage_cheetah(ifile, *args, **kwargs)

    def templatepage_cheetah(self, ifile=None, *args, **kwargs):
        """
        Template page method.
        """
        search_list = []
        if len(args) > 0:
            search_list.append(args)
        if len(kwargs) > 0:
            search_list.append(kwargs)
        templatefile = os.path.join(self.templatedir, ifile + '.tmpl')
        if os.path.exists(templatefile):
            # little workaround to fix '#include'
            search_list.append({'templatedir': self.templatedir})
            template = Template(file=templatefile, searchList=search_list)
            return template.respond()

        else:
            self.warning("%s not found at %s" % (ifile, self.templatedir))
            return "Template %s not known" % ifile

    def templatepage_jinja(self, ifile=None, *args, **kwargs):
        """
        Template page method.
        """
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(self.templatedir))
        for arg in args:
            kwargs.update(**arg)
        # Add DAS function to jinja template environment that they'll be
        # visible and usable in jinja tempaltes
        das_funcs = {"quote":quote, 'url_extend_params':url_extend_params,
                "url_extend_params_as_dict":url_extend_params_as_dict}
        kwargs.update(**das_funcs)
        tmpl = os.path.join(self.templatedir, ifile + '.tmpl')
        if  os.path.exists(tmpl):
            template = env.get_template(ifile + '.tmpl')
            return template.render(kwargs)
        else:
            self.warning("%s not found at %s" % (ifile, self.templatedir))
            return "Template %s not known" % ifile

def exposetext (func):
    """CherryPy expose Text decorator"""
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/plain"
        return data
    return wrapper

def jsonstreamer(func):
    """JSON streamer decorator"""
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        cherrypy.response.headers['Content-Type'] = "application/json"
        func._cp_config = {'response.stream': True}
        head, data = func (self, *args, **kwds)
        yield json.dumps(head)[:-1] # do not yield }
        yield ', "data": ['
        if  isinstance(data, dict):
            for chunk in JSONEncoder().iterencode(data):
                yield chunk
        elif  isinstance(data, list) or isinstance(data, types.GeneratorType):
            sep = ''
            for rec in data:
                if  sep:
                    yield sep
                for chunk in JSONEncoder().iterencode(rec):
                    yield chunk
                if  not sep:
                    sep = ', '
        else:
            msg = 'jsonstreamer, improper data type %s' % type(data)
            raise Exception(msg)
        yield ']}'
    return wrapper

def exposejson (func):
    """CherryPy expose JSON decorator"""
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        encoder = JSONEncoder()
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/json"
        try:
            jsondata = encoder.encode(data)
            return jsondata
        except:
            Exception("Fail to JSONtify obj '%s' type '%s'" \
                % (data, type(data)))
    return wrapper

def exposejs (func):
    """CherryPy expose JavaScript decorator"""
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/javascript"
        return data
    return wrapper

def exposecss (func):
    """CherryPy expose CSS decorator"""
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/css"
        return data
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
    """
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        encoder = JSONEncoder()
        data = func(self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/json"
        try:
            jsondata = encoder.encode(data)
            return jsondata
        except:
            Exception("Failed to JSONtify obj '%s' type '%s'" \
                % (data, type(data)))
    return wrapper

def enable_cross_origin(func):
    """
    Enables Cross Origin Requests (from a predefined list of DAS origins)
    to be run on each given back-end server (keyword search, autocompletion)
    """
    from DAS.utils.das_config import das_readconfig
    dasconfig = das_readconfig()

    # load list of hosts from where keyword search could be initialized
    valid_origins = dasconfig['load_balance'].get('valid_origins', [])

    def enable_cross_orign_requests():
        """
        on each request, add additional headers that will allow browser
        to use the KWS  result (loaded from other origin/domain)
        """

        # output the requests origin if it's allowed
        origin = cherrypy.request.headers.get('Origin', '')
        if origin in valid_origins:
            cherrypy.response.headers['Access-Control-Allow-Origin'] = origin

        cherrypy.response.headers['Access-Control-Allow-Headers'] = 'X-JSON'
        cherrypy.response.headers['Access-Control-Expose-Headers'] = 'X-JSON'

    def wrapper(self, *args, **kwds):
        data = func(self, *args, **kwds)
        enable_cross_orign_requests()
        return data

    return wrapper
