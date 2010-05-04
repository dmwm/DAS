#!/usr/bin/env python
"""
DAS server based on CherryPy web framework. We define Root class and
pass it into CherryPy web server.
"""

__revision__ = "$Id: das_server.py,v 1.1 2010/02/15 18:30:47 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os
import sys
import yaml
import types
import logging 
from optparse import OptionParser

# CherryPy modules
import cherrypy
from cherrypy import quickstart, expose, server, log
from cherrypy import tree, engine, dispatch, tools
from cherrypy import config as cpconfig

# local modules
from DAS.web.das_webmanager import DASWebManager
from DAS.web.DASSearch import DASSearch
from DAS.web.DASCacheModel import DASCacheModel
from DAS.web.das_doc import DocServer

class Root(object):
    def __init__(self, model, config):
        self.model  = model
        self.config = config
        self.app    = "Root"
        
    def configure(self):
        """Configure server, CherryPy and the rest."""
        try:
            cpconfig.update ({"server.environment": self.config['environment']})
        except:
            cpconfig.update ({"server.environment": 'production'})
        try:
            cpconfig.update ({"server.socket_port": int(self.config['port'])})
        except:
            cpconfig.update ({"server.socket_port": 8080})
        try:
            cpconfig.update ({"server.socket_host": self.config['host']})
        except:
            cpconfig.update ({"server.socket_host": '0.0.0.0'})
        try:
            cpconfig.update ({'tools.expires.secs': int(self.config['expires'])})
        except:
            cpconfig.update ({'tools.expires.secs': 300})
        try:
            cpconfig.update ({'log.screen': bool(self.config['log_screen'])})
        except:
            cpconfig.update ({'log.screen': True})
        try:
            cpconfig.update ({'log.access_file': self.config['access_log_file']})
        except:
            cpconfig.update ({'log.access_file': None})
        try:
            cpconfig.update ({'log.error_file': self.config['error_log_file']})
        except:
            cpconfig.update ({'log.error_file': None})
        try:
            log.error_log.setLevel(self.config['error_log_level'])
        except:     
            log.error_log.setLevel(logging.DEBUG)
        try:
            log.access_log.setLevel(self.config['access_log_level'])
        except:
            log.access_log.setLevel(logging.DEBUG)
        cpconfig.update ({
                          'tools.expires.on': True,
                          'tools.response_headers.on':True,
                          'tools.etags.on':True,
                          'tools.etags.autotags':True,
                          'tools.encode.on': True,
                          'tools.gzip.on': True
                          })
        #cpconfig.update ({'request.show_tracebacks': False})
        #cpconfig.update ({'request.error_response': self.handle_error})
        #cpconfig.update ({'tools.proxy.on': True})
        #cpconfig.update ({'proxy.tool.base': '%s:%s' % (socket.gethostname(), opts.port)})

        log("loading config: %s" % cpconfig, 
                                   context=self.app, 
                                   severity=logging.DEBUG, 
                                   traceback=False)

    def start(self, blocking=True):
        """Configure and start the server."""
        self.configure()
        config = {} # can be something to consider
        if  self.model == 'cache_server':
            obj = DASCacheModel(config)
            tree.mount(obj, '/')
        elif self.model == 'web_server':
            obj = DASSearch(config)
            tree.mount(obj, '/das')
        elif self.model == 'doc_server':
            dir = os.environ['DAS_ROOT'] + '/doc/build/html'
            mimes = ['text/css', 'application/javascript', 'text/javascript']
            static_dict = { 'tools.gzip.on':True,
                            'tools.gzip.mime_types':mimes,
                            'tools.staticdir.on':True,
                            'tools.staticdir.dir':dir,
            }
            conf = {'/': {'tools.staticdir.root':dir},
                    '_static' : static_dict,
            }
            cpconfig.update(conf)
            obj = DocServer(dir)
            tree.mount(obj, '/das/doc')
        else:
            obj = DASWebManager(config)
            tree.mount(obj, '/')
        engine.start()
        if  blocking:
            engine.block()
            
    def stop(self):
        """Stop the server."""
        engine.exit()
        engine.stop()
        
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="config", default=False,
        help="provide cherrypy configuration file")
    parser.add_option("--cache-server", action="store_true", dest="cache_server",
        help="start DAS cache server, default port 8211")
    parser.add_option("--web-server", action="store_true", dest="web_server",
        help="start DAS web server, default port 8212")
    parser.add_option("--doc-server", action="store_true", dest="doc_server",
        help="start DAS doc server, default port 8210")
    parser.add_option("-p", "--port", dest="port", default=False,
        help="specify port number")
    opts, args = parser.parse_args()

    # Read server configuration
    conf_file = opts.config
    config = {}
    if  conf_file:
        fdesc  = open(config_file, 'r')
        config = yaml.loads(fdesc.read())
        fdesc.close()

    # Choose which DAS server to start
    if  opts.cache_server:
        model = "cache_server"
        config['port'] = 8211
    elif opts.web_server:
        model = "web_server"
        config['port'] = 8212
    elif opts.doc_server:
        model = "doc_server"
        config['port'] = 8210
    else:
        print "Please specify which DAS server you want to start, see --help"
        sys.exit(1)

    # Redefine DAS port if it is required
    if  opts.port:
        config['port'] = int(opts.port)

    # Start DAS server
    root = Root(model, config)
    root.start()
