#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS doc server.
"""

__revision__ = "$Id: das_doc_server.py,v 1.1 2010/01/26 21:05:40 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os
import sys
import types
import cherrypy

from cherrypy import expose, tools
from cherrypy.lib.static import serve_file, staticdir
from optparse import OptionParser

class DocOptionParser: 
    """
    Doc server option parser
    """
    def __init__(self):
        dir = os.environ['DAS_ROOT'] + '/doc/build/html'
        self.parser = OptionParser()
        self.parser.add_option("-p", "--port", action="store", type="string", 
                                          default=8213, dest="port",
             help="specify port number.")
        self.parser.add_option("-d", "--dir", action="store", type="string", 
                                          default=dir, dest="dir",
             help="specify port number.")

    def getOpt(self):
        """
        Returns list of options
        """
        return self.parser.parse_args()

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
        
if __name__ == '__main__':
    optManager  = DocOptionParser()
    (opts, args) = optManager.getOpt()

    cherrypy.config.update({'environment':'production', 
        'log.screen':True, 
        'server.socket_host':'0.0.0.0',
        'server.socket_port': opts.port})

    dir = opts.dir
    mimes = ['text/css', 'application/javascript', 'text/javascript']
    static_dict = { 'tools.gzip.on':True,
                    'tools.gzip.mime_types':mimes,
                    'tools.staticdir.on':True,
                    'tools.staticdir.dir':dir,
    }
    conf = {'/': {'tools.staticdir.root':dir},
            '_static' : static_dict,
    }
    cherrypy.quickstart(DocServer(dir), '/', config=conf)

