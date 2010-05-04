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
import time

# cherrypy modules
import cherrypy
from cherrypy import expose, response, tools
from cherrypy.lib.static import serve_file
from cherrypy import config as cherryconf

#try:
#    from WMCore.WebTools.Page import exposecss, exposejs, TemplatedPage
#except:
#    from DAS.web.tools import exposecss, exposejs, TemplatedPage
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

class DASWebManager(TemplatedPage):
    """
    DAS web manager.
    """
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        self.base = '' # defines base path for HREF in templates
        imgdir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'images')
        if  not os.path.isdir(imgdir):
            imgdir = os.environ['DAS_ROOT'] + '/src/images'
        cssdir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'css')
        if  not os.path.isdir(cssdir):
            cssdir = os.environ['DAS_ROOT'] + '/src/css'
        jsdir  = '%s/%s' % (__file__.rsplit('/', 1)[0], 'js')
        if  not os.path.isdir(jsdir):
            jsdir = os.environ['DAS_ROOT'] + '/src/js'
        self.cssmap   = {
            'das.css': cssdir + '/das.css',
            'prettify.css': cssdir + '/prettify.css',
        }
        self.jsmap    = {
            'utils.js': jsdir + '/utils.js',
            'ajax_utils.js': jsdir + '/ajax_utils.js',
            'prototype.js': jsdir + '/prototype.js',
            'prettify.js': jsdir + '/prettify.js',
            'rico.js': jsdir + '/rico.js',
        }
        self.imagemap = {
            'loading.gif': imgdir + '/loading.gif',
            'cms_logo.jpg': imgdir + '/cms_logo.jpg',
            'cms_logo.png': imgdir + '/cms_logo.png',
            'mongodb_logo.png': imgdir + '/mongodb_logo.png',
        }
        self.cache    = {}

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

    def bottom(self):
        """
        Provide footer for all web pages
        """
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        ctime = 0
        return self.templatepage('das_bottom', div="", services="",
                timestamp=timestamp, ctime=ctime)

    def page(self, content):
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
        mime_types = ['*/*', 'image/gif', 'image/png', 
                      'image/jpg', 'image/jpeg']
        accepts = cherrypy.request.headers.elements('Accept')
        for accept in accepts:
            if  accept.value in mime_types and len(args) == 1 \
                and self.imagemap.has_key(args[0]):
                image = self.imagemap[args[0]]
                # use image extension to pass correct content type
                ctype = 'image/%s' % image.split('.')[-1]
                cherrypy.response.headers['Content-type'] = ctype
                return serve_file(image, content_type=ctype)

    @exposecss
    @tools.gzip()
    def css(self, *args, **kwargs):
        """
        Cat together the specified css files and return a single css include.
        Get css by calling: /controllers/css/file1/file2/file3
        """
        mime_types = ['text/css']
        cherryconf.update({'tools.encode.on': True, 
                           'tools.gzip.on': True,
                           'tools.gzip.mime_types': mime_types,
                          })
        
        args = list(args)
        scripts = self.check_scripts(args, self.cssmap)
        idx = "-".join(scripts)
        
        if  idx not in self.cache.keys():
            data = '@CHARSET "UTF-8";'
            for script in args:
                if  self.cssmap.has_key(script):
                    path = os.path.join(sys.path[0], self.cssmap[script])
                    path = os.path.normpath(path)
                    ifile = open(path)
                    data = "\n".join ([data, ifile.read().\
                        replace('@CHARSET "UTF-8";', '')])
                    ifile.close()
            set_headers ("text/css")
            self.cache[idx] = minify(data)
        return self.cache[idx] 
        
    @exposejs
    @tools.gzip()
    def js(self, *args, **kwargs):
        """
        Cat together the specified js files and return a single js include.
        Get js by calling: /controllers/js/file1/file2/file3
        """
        mime_types = ['application/javascript', 'text/javascript',
                      'application/x-javascript', 'text/x-javascript']
        cherryconf.update({'tools.gzip.on': True,
                           'tools.gzip.mime_types': mime_types,
                           'tools.encode.on': True,
                          })
        
        args = list(args)
        scripts = self.check_scripts(args, self.jsmap)
        idx = "-".join(scripts)
        
        if  idx not in self.cache.keys():
            data = ''
            for script in args:
                path = os.path.join(sys.path[0], self.jsmap[script])
                path = os.path.normpath(path)
                ifile = open(path)
                data = "\n".join ([data, ifile.read()])
                ifile.close()
            self.cache[idx] = data
        return self.cache[idx] 
    
#    @exposejs
#    def yui(self, *args, **kwargs):
#        """
#        cat together the specified YUI files. args[0] should be the YUI version,
#        and the scripts should be specified in the kwargs s=scriptname
#        """
#        cherryconf.update ({'tools.encode.on': True, 'tools.gzip.on': True})
#        version = args[0]
#        scripts = self.makelist(kwargs['s'])
        
        
    def check_scripts(self, scripts, map):
        """
        Check a script is known to the map and that the script actually exists   
        """           
        for script in scripts:
            if script not in map.keys():
                self.warning("%s not known" % script)
                scripts.remove(script)
            else:
                path = os.path.join(sys.path[0], map[script])
                path = os.path.normpath(path)
                if not os.path.exists(path):
                    self.warning("%s not found at %s" % (script, path))
                    scripts.remove(script)
        return scripts
    
