#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS web interface to test various CMS data services
"""
__author__ = "Valentin Kuznetsov"

# system modules
import logging
import cherrypy
from cherrypy import expose, tree, engine, HTTPError
from inspect import getmembers

# DAS modules
from DAS.utils.logger import DASLogger, set_cherrypy_logger, NullHandler
from DAS.web.das_webmanager import DASWebManager
from DAS.web.das_codes import web_code
from DAS.web.utils import wrap2dasjson

class Root(object):
    """
    DASTestDataService web server class.
    """
    def __init__(self, config=None):
        self.engine = engine
        if  not config:
            self.config = {}
        else:
            self.config = config

    def start(self, blocking=False):
        """Start the server."""
        obj = DASTestDataService(self.config) # mount test server
        tree.mount(obj, '/')
        self.engine.start()
        if  blocking:
            self.engine.block()

    def stop(self):
        """Stop the server"""
        self.engine.exit()

class DASTestDataService(DASWebManager):
    """
    DAS web service interface for various CMS data services.
    We expose methods whose name reflect CMS data service name, e.g.
    for DBS system we provide dbs method, for phedex the phedex one, etc.
    All methods provide data in a format of reflected data-service,
    DBS provides XML, while phedex JSON.
    """
    def __init__(self, config):
        DASWebManager.__init__(self, config)
        logfile      = config.get('logfile', None)
        loglevel     = config.get('loglevel', 0)
        self.logger  = DASLogger(logfile=logfile, verbose=loglevel)
        set_cherrypy_logger(self.logger.handler, loglevel)
        # force to load the page all the time
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        cherrypy.response.headers['Pragma'] = 'no-cache'
        if  not loglevel: # be really quiet
            hdlr = NullHandler()
            logger = logging.getLogger('foo')
            logger.addHandler(hdlr)
            logger.setLevel(logging.NOTSET)
            cherrypy.log.logger_root = logger
        # get list of registered in this class systems
        # we inpsect the class and get all names except in elist
        elist = ['index', 'default'] # exclude from search below
        self.systems = [m[0] for m in getmembers(self) \
                if m[0][:2] != '__' and m[0] not in elist]

    @expose
    def index(self, **_kwargs):
        """
        represents DAS web interface. 
        It uses das_searchform template for
        input form and yui_table for output Table widget.
        """
        page = "<h1>Welcome to DAS CMS Test server</h1>"
        page = "<p>Available systems:</p><ul>"
        for system in self.systems:
            aref  = "<a href=\"/%s\">%s</a>" % (system, system) 
            page += "<li>%s</li>" % aref
        page += "</ul>"
        return page

    @expose
    def dbs(self, **kwargs):
        """
        Test DBS data-service
        """
        try:
            cherrypy.response.headers['Content-Type'] = "application/xml"
            data = """<?xml version='1.0' standalone='yes'?><dbs>
                    <primary_dataset id='0' primary_name='abc'/><SUCCESS/></dbs>"""
        except:
            data = ""
        return data

    @expose
    def phedex(self, **kwargs):
        """
        Test PhEDEx data-service
        """
        try:
            node = kwargs.get('node')
            data = """<?xml version='1.0' encoding='ISO-8859-1'?>
                    <phedex timestamp='1'><node name='%s'/></phedex>""" % node
        except:
            data = ""
        return data

    @expose
    def sitedb(self, **kwargs):
        """
        Test SiteDB data-service
        """
        try:
            if  kwargs.has_key('name'):
                site = kwargs.get('name')
            elif kwargs.has_key('site'):
                site = kwargs.get('site')
            else:
                code = web_code('Unsupported key')
                HTTPError(500, 'DAS error, code=%s' % code)
            data = {"0": {"name":site}}
        except:
            data = {}
        return wrap2dasjson(data)

    @expose
    def ip(self, **kwargs):
        """
        Test IP data-service
        """
        try:
            addr = kwargs.get('ip')
            data = {'ip':{'address': addr}}
        except:
            data = {}
        return wrap2dasjson(data)

    @expose
    def zip(self, **kwargs):
        """
        Test Google maps geo zip data-service
        """
        try:
            code = int(kwargs.get('q'))
            data = {'zip': {'code': code, 
                            'place' :[{'city': 'test1'}, {'city':'test2'}]}}
        except:
            data = {}
        return wrap2dasjson(data)
#
# main
#
if  __name__ == '__main__':
    # start TestDataService
    CONFIG = dict(logfile='', loglevel=1)
    SERVER = Root(CONFIG)
    SERVER.start(blocking=True)
