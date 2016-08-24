#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS web server.
"""

__author__ = "Valentin Kuznetsov"

import sys
# python 3
if  sys.version.startswith('3.'):
    basestring = str

# system modules
import os
import time
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
from DAS.web.utils import checkargs

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
    if  entry not in emap:
        emap[entry] = mapdir + entry

def das_web_files():
    "List files used by DAS web server"
    files = ['cms_logo.png', 'das.css', 'opentip.css', 'kwsearch.css',
            'prettify.css',
            'prototype.js', 'utils.js', 'ajax_utils.js',
            'fonts-min.css', 'container.css', 'autocomplete.css', 'paginator.css',
            'datatable.css', 'yahoo-dom-event.js', 'container-min.js',
            'datasource-min.js', 'connection-min.js', 'yahoo-min.js',
            'cookie-min.js', 'json-min.js', 'autocomplete-min.js',
            'element-min.js', 'paginator-min.js', 'datatable-min.js',
            'opentip-prototype-excanvas.min.js', 'kwdsearch.js']
    return files

def check_values(pfiles):
    "Check passed files against das web files"
    dasfiles = das_web_files()
    if  isinstance(pfiles, list):
        for name in pfiles:
            fname = name.split('/')[-1]
            if fname not in dasfiles:
                raise Exception("Passed wrong file", name)
    elif isinstance(pfiles, basestring):
        if pfiles not in dasfiles:
                raise Exception("Passed wrong file", pfiles)
    else:
        raise Exception("Wrong type", pfiles, type(pfiles))

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
        if  'YUI_ROOT' not in os.environ:
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
        return self.templatepage('das_bottom', div=div, version=DAS.version, time=time)

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
                and args[0] in self.imgmap:
                image = self.imgmap[args[0]]
                # use image extension to pass correct content type
                ctype = 'image/%s' % image.split('.')[-1]
                cherrypy.response.headers['Content-type'] = ctype
                return serve_file(image, content_type=ctype)

    def serve(self, kwds, imap, idir, datatype='', minimize=False):
        "Serve files for high level APIs (yui/css/js)"
        args = []
        for key, val in kwds.items():
            if  key == 'f': # we only look-up files from given kwds dict
                if  isinstance(val, list):
                    for name in val:
                        if name.startswith('/') or name.find('..')!=-1:
                            continue
                        fname = os.path.join(idir, name)
                        if os.path.exists(fname) and os.path.isfile(fname):
                            args.append(name)
                else:
                    if  not val.startswith('/') or name.find('..')!=-1:
                        fname = os.path.join(idir, val)
                        if os.path.exists(fname) and os.path.isfile(fname):
                            args.append(val)
        scripts = self.check_scripts(args, imap, idir)
        return self.serve_files(args, scripts, imap, datatype, minimize)

    @exposecss
    @tools.gzip()
    @checkargs(['f', 'resource'])
    def css(self, **kwargs):
        """
        Serve provided CSS files. They can be passed as
        f=file1.css&f=file2.css
        """
        check_values(kwargs.get('f', []))
        resource = kwargs.get('resource', 'css')
        if  resource == 'css':
            return self.serve(kwargs, self.cssmap, self.cssdir, 'css', True)
        elif resource == 'yui':
            return self.serve(kwargs, self.yuimap, self.yuidir)

    @exposejs
    @tools.gzip()
    @checkargs(['f', 'resource'])
    def js(self, **kwargs):
        """
        Serve provided JS scripts. They can be passed as
        f=file1.js&f=file2.js with optional resource parameter
        to speficy type of JS files, e.g. resource=yui.
        """
        check_values(kwargs.get('f', []))
        resource = kwargs.get('resource', 'js')
        if  resource == 'js':
            return self.serve(kwargs, self.jsmap, self.jsdir)
        elif resource == 'yui':
            return self.serve(kwargs, self.yuimap, self.yuidir)

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
