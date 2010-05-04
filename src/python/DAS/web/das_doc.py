#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS doc server class.
"""

__revision__ = "$Id: das_doc.py,v 1.1 2010/02/15 18:30:47 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os
import sys
import types

from cherrypy import expose
from cherrypy.lib.static import serve_file, staticdir

class DocServer(object):
    """
    Documentation Server class. It uses static content of
    provided directory to serve sphinx documentation.
    All methods are defined with respect to default
    sphinx notations.
    """
    def __init__(self, dir):
        self.dir = dir

    @expose
    def error(self, msg):
        """Return error message"""
        return msg

    @expose
    def index(self, *args, **kwargs):
        """Serve default index.html web page"""
        return serve_file(os.path.join(self.dir, 'index.html'))

    def filename(self, args):
        """Construct input filename out of provided list"""
        input = ""
        for item in args:
            input += item
            if  item != args[-1]:
                input += '/'
        return input

    @expose
    def default(self, *args, **kwargs):
        """Default method to redirect input requests."""
        input = self.filename(args)
        filename = os.path.join(self.dir, input)
        if  not os.path.isfile(filename):
            return self.error('No such file %s' % input) 
        return serve_file(filename)

    @expose
    def _static(self, *args, **kwargs):
        """Serve _static/<filename> content"""
        input = self.filename(args)
        filename = os.path.join(self.dir, '_static/%s' % input)
        if  not os.path.isfile(filename):
            return self.error('No such file %s' % input) 
        return serve_file(filename)

    @expose
    def _images(self, *args, **kwargs):
        """Serve _images/<filename> content"""
        input = self.filename(args)
        filename = os.path.join(self.dir, '_images/%s' % input)
        if  not os.path.isfile(filename):
            return self.error('No such file %s' % input) 
        return serve_file(filename)
