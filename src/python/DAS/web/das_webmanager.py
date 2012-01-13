#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS web server.
"""

__license__ = "GPL"
__revision__ = "$Id: das_webmanager.py,v 1.3 2010/03/15 02:44:09 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"
__email__ = "vkuznet@gmail.com"

# system modules
import os
import sys

# cherrypy modules
import cherrypy
from cherrypy import expose, response, tools
from cherrypy.lib.static import serve_file
from cherrypy import config as cherryconf

#try:
#    from WMCore.WebTools.Page import exposecss, exposejs, TemplatedPage
#except:
#    from DAS.web.tools import exposecss, exposejs, TemplatedPage
import DAS
from DAS.web.tools import exposecss, exposejs, TemplatedPage

def set_headers(itype, size=0):
    """
    Set response header Content-type (itype) and Content-Length (size).
    """
    if  size > 0:
        response.headers['Content-Length'] = size
    response.headers['Content-Type'] = itype
    response.headers['Expires'] = 'Sat, 14 Oct 2017 00:59:30 GMT'
    
def minify(content):
    """
    Remove whitespace in provided content.
    """
    content = content.replace('\n', ' ')
    content = content.replace('\t', ' ')
    content = content.replace('   ', ' ')
    content = content.replace('  ', ' ')
    return content

def update_map(emap, mapdir, entry):
    """Update entry map for given entry and mapdir"""
    if  not emap.has_key():
        emap[entry] = mapdir + entry

def parse_args(params):
    """
    Return list of arguments for provided set, where elements in a set
    can be in a form of file1_and_file2_and_file3
    """
    return [arg for s in params for arg in s.split('_and_')]

class DASWebManager(TemplatedPage):
    """
    DAS web manager.
    """
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        self.base   = '' # defines base path for HREF in templates
        self.imgdir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'images')
        if  not os.path.isdir(self.imgdir):
            self.imgdir = os.environ['DAS_IMAGESPATH']
        self.cssdir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'css')
        if  not os.path.isdir(self.cssdir):
            self.cssdir = os.environ['DAS_CSSPATH']
        self.jsdir  = '%s/%s' % (__file__.rsplit('/', 1)[0], 'js')
        if  not os.path.isdir(self.jsdir):
            self.jsdir = os.environ['DAS_JSPATH']
        if  not os.environ.has_key('YUI_ROOT'):
            msg = 'YUI_ROOT is not set in environment'
            raise Exception(msg)
        self.yuidir = os.environ['YUI_ROOT']

        # To be filled at run time
        self.cssmap = {}
        self.jsmap  = {}
        self.imgmap = {}
        self.yuimap = {}

        # Update CherryPy configuration
        mime_types  = ['text/css']
        mime_types += ['application/javascript', 'text/javascript',
                       'application/x-javascript', 'text/x-javascript']
        cherryconf.update({'tools.encode.on': True, 
                           'tools.gzip.on': True,
                           'tools.gzip.mime_types': mime_types,
                          })
        self._cache    = {}

    @expose
    def index(self, *args, **kwargs):
        """Main page"""
        page = "DAS Web Server"
        return self.page(page)

    def top(self):
        """
        Provide masthead for all web pages
        """
        return self.templatepage('das_top', base=self.base)

    def bottom(self, div=""):
        """
        Provide footer for all web pages
        """
        ctime = 0
        return self.templatepage('das_bottom', div=div, 
                ctime=ctime, version=DAS.version)

    def page(self, content, _ctime=None, _response=False):
        """
        Provide page wrapped with top/bottom templates.
        """
        page  = self.top()
        page += content
        page += self.bottom()
        return page

    @expose
    def images(self, *args, **kwargs):
        """
        Serve static images.
        """
        args = list(args)
        self.check_scripts(args, self.imgmap, self.imgdir)
        mime_types = ['*/*', 'image/gif', 'image/png', 
                      'image/jpg', 'image/jpeg']
        accepts = cherrypy.request.headers.elements('Accept')
        for accept in accepts:
            if  accept.value in mime_types and len(args) == 1 \
                and self.imgmap.has_key(args[0]):
                image = self.imgmap[args[0]]
                # use image extension to pass correct content type
                ctype = 'image/%s' % image.split('.')[-1]
                cherrypy.response.headers['Content-type'] = ctype
                return serve_file(image, content_type=ctype)

    @exposecss
    @exposejs
    @tools.gzip()
    def yui(self, *args, **kwargs):
        """
        Serve YUI library. YUI files has disperse directory structure, so
        input args can be in a form of (build, container, container.js)
        which corresponds to a single YUI JS file
        build/container/container.js
        """
        args = ['/'.join(args)] # preserve YUI dir structure
        scripts = self.check_scripts(args, self.yuimap, self.yuidir)
        return self.serve_files(args, scripts, self.yuimap)
        
    @exposecss
    @tools.gzip()
    def css(self, *args, **kwargs):
        """
        Cat together the specified css files and return a single css include.
        Multiple files can be supplied in a form of file1&file2&file3
        """
        args = parse_args(args)
        scripts = self.check_scripts(args, self.cssmap, self.cssdir)
        return self.serve_files(args, scripts, self.cssmap, 'css', True)
        
    @exposejs
    @tools.gzip()
    def js(self, *args, **kwargs):
        """
        Cat together the specified js files and return a single js include.
        Multiple files can be supplied in a form of file1&file2&file3
        """
        args = parse_args(args)
        scripts = self.check_scripts(args, self.jsmap, self.jsdir)
        return self.serve_files(args, scripts, self.jsmap)

    def serve_files(self, args, scripts, resource, datatype='', minimize=False):
        """
        Return asked set of files for JS, YUI, CSS.
        """
        idx = "-".join(scripts)
        if  idx not in self._cache.keys():
            data = ''
            if  datatype == 'css':
                data = '@CHARSET "UTF-8";'
            for script in args:
                path = os.path.join(sys.path[0], resource[script])
                path = os.path.normpath(path)
                ifile = open(path)
                data = "\n".join ([data, ifile.read().\
                    replace('@CHARSET "UTF-8";', '')])
                ifile.close()
            if  datatype == 'css':
                set_headers("text/css")
            if  minimize:
                self._cache[idx] = minify(data)
            else:
                self._cache[idx] = data
        return self._cache[idx] 
    
    def check_scripts(self, scripts, resource, path):
        """
        Check a script is known to the resource map 
        and that the script actually exists   
        """           
        for script in scripts:
            if  script not in resource.keys():
                spath = os.path.normpath(os.path.join(path, script))
                if  os.path.isfile(spath):
                    resource.update({script: spath})
        return scripts
