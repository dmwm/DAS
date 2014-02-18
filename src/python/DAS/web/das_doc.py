#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0613,W0622

"""
DAS doc service class.
"""

__revision__ = "$Id: das_doc.py,v 1.3 2010/03/18 17:52:02 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os

from cherrypy import expose
from cherrypy.lib.static import serve_file

@expose
def error(msg):
    """Return error message"""
    return msg

def construct_filename(args):
    """Construct input filename out of provided list"""
    input = ""
    for item in args:
        input += item
        if  item != args[-1]:
            input += '/'
    return input

class DASDocService(object):
    """
    DAS documentation service class. It uses static content of
    provided directory to serve sphinx documentation.
    All methods are defined with respect to default
    sphinx notations.
    """
    def __init__(self, idir):
        self.dir = idir

    @expose
    def index(self, *args, **kwargs):
        """Serve default index.html web page"""
        return serve_file(os.path.join(self.dir, 'index.html'))

    @expose
    def default(self, *args, **kwargs):
        """Default method to redirect input requests."""
        input = construct_filename(args)
        filename = os.path.join(self.dir, input)
        if  not os.path.isfile(filename):
            return error('No such file %s' % input)
        return serve_file(filename)

    @expose
    def _static(self, *args, **kwargs):
        """Serve _static/<filename> content"""
        input = construct_filename(args)
        filename = os.path.join(self.dir, '_static/%s' % input)
        if  not os.path.isfile(filename):
            return error('No such file %s' % input)
        return serve_file(filename)

    @expose
    def _images(self, *args, **kwargs):
        """Serve _images/<filename> content"""
        input = construct_filename(args)
        filename = os.path.join(self.dir, '_images/%s' % input)
        if  not os.path.isfile(filename):
            return error('No such file %s' % input)
        return serve_file(filename)
